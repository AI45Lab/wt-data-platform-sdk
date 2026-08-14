"""Count production serving rows for each dataset found in ``job_id``.

The script intentionally delegates reads to ``scripts/inspect/query_data.py``
instead of duplicating its table-routing and query behavior. It first discovers
distinct job IDs, extracts the first ``#``-delimited field, and then runs one
literal contains-LIKE count per dataset with bounded concurrency.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

DEFAULT_CONCURRENCY = 5
DEFAULT_TABLE = "wind_tunnel_serving"
DEFAULT_VERBOSE = False
MAX_INVALID_EXAMPLES = 5
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
QUERY_SCRIPT = REPOSITORY_ROOT / "scripts" / "inspect" / "query_data.py"


class QueryCommandError(RuntimeError):
    """Report a failed or malformed ``query_data.py`` invocation."""


def positive_int(raw_value: str) -> int:
    """Parse an argparse value that must be a positive integer."""
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Count rows in a table for each dataset extracted from distinct "
            "# delimited job_id values."
        )
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Table containing job_id values (default: {DEFAULT_TABLE}).",
    )
    parser.add_argument(
        "--concurrency",
        type=positive_int,
        default=DEFAULT_CONCURRENCY,
        help=f"Maximum concurrent count queries (default: {DEFAULT_CONCURRENCY}).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=DEFAULT_VERBOSE,
        help="Print every query_data.py command to stderr before executing it.",
    )
    return parser


def _process_failure_detail(completed: subprocess.CompletedProcess[str]) -> str:
    """Return a bounded, useful diagnostic from a failed child process."""
    detail = (completed.stderr or "").strip() or (completed.stdout or "").strip()
    if not detail:
        return "no diagnostic output"
    if len(detail) > 2000:
        return "..." + detail[-2000:]
    return detail


def _run_query_data(
    arguments: Sequence[str],
    output_path: Path,
    *,
    table: str = DEFAULT_TABLE,
    verbose: bool = DEFAULT_VERBOSE,
) -> dict[str, Any]:
    """Run query_data.py and return its JSON output payload."""
    command = [
        sys.executable,
        str(QUERY_SCRIPT),
        "--table",
        table,
        *arguments,
        "--output",
        str(output_path),
    ]
    if verbose:
        print(f"Executing query: {shlex.join(command)}", file=sys.stderr)
    try:
        completed = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise QueryCommandError(f"failed to start query_data.py: {exc}") from exc

    if completed.returncode != 0:
        detail = _process_failure_detail(completed)
        raise QueryCommandError(f"query_data.py exited with code {completed.returncode}: {detail}")

    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise QueryCommandError("query_data.py did not create its JSON output") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise QueryCommandError(f"cannot read query_data.py JSON output: {exc}") from exc

    if not isinstance(payload, dict):
        raise QueryCommandError("query_data.py JSON output must be an object")
    return payload


def _discover_job_ids(
    output_path: Path,
    *,
    table: str = DEFAULT_TABLE,
    verbose: bool = DEFAULT_VERBOSE,
) -> list[Any]:
    """Return all distinct job_id values reported by query_data.py."""
    payload = _run_query_data(
        ["--distinct", "job_id"],
        output_path,
        table=table,
        verbose=verbose,
    )
    values = payload.get("values")
    if not isinstance(values, list):
        raise QueryCommandError("distinct query JSON output is missing the values list")

    job_ids: list[Any] = []
    for index, row in enumerate(values):
        if not isinstance(row, dict) or "job_id" not in row:
            raise QueryCommandError(f"distinct query returned an invalid value at index {index}")
        job_ids.append(row["job_id"])
    return job_ids


def extract_datasets(job_ids: Sequence[Any]) -> tuple[list[str], list[Any]]:
    """Extract sorted unique dataset names and return malformed job IDs."""
    datasets: set[str] = set()
    invalid_job_ids: list[Any] = []
    for job_id in job_ids:
        if not isinstance(job_id, str) or "#" not in job_id:
            invalid_job_ids.append(job_id)
            continue
        dataset = job_id.split("#", 1)[0].strip()
        if not dataset or "\x00" in dataset:
            invalid_job_ids.append(job_id)
            continue
        datasets.add(dataset)
    return sorted(datasets), invalid_job_ids


def escape_like_literal(value: str) -> str:
    """Escape a literal for Lance/DataFusion LIKE with backslash as ESCAPE."""
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped.replace("'", "''")


def build_dataset_filter(dataset: str) -> str:
    """Build the requested literal contains-LIKE predicate."""
    escaped = escape_like_literal(dataset)
    return f"(job_id LIKE '%{escaped}%' ESCAPE '\\')"


def _query_dataset_count(
    dataset: str,
    output_path: Path,
    *,
    table: str = DEFAULT_TABLE,
    verbose: bool = DEFAULT_VERBOSE,
) -> int:
    payload = _run_query_data(
        ["--query", build_dataset_filter(dataset), "--count"],
        output_path,
        table=table,
        verbose=verbose,
    )
    row_count = payload.get("filtered_rows")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise QueryCommandError(
            f"count query for dataset {dataset!r} returned an invalid filtered_rows value"
        )
    return row_count


def count_datasets(
    datasets: Sequence[str],
    output_dir: Path,
    concurrency: int,
    *,
    table: str = DEFAULT_TABLE,
    verbose: bool = DEFAULT_VERBOSE,
) -> tuple[dict[str, int], dict[str, str]]:
    """Count datasets concurrently while preserving all per-dataset failures."""
    if not datasets:
        return {}, {}

    counts: dict[str, int] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_dataset = {
            executor.submit(
                _query_dataset_count,
                dataset,
                output_dir / f"count-{index:05d}.json",
                table=table,
                verbose=verbose,
            ): dataset
            for index, dataset in enumerate(datasets)
        }
        for future in as_completed(future_to_dataset):
            dataset = future_to_dataset[future]
            try:
                counts[dataset] = future.result()
            except Exception as exc:  # noqa: BLE001 - isolate one dataset query failure
                errors[dataset] = str(exc)
    return counts, errors


def format_result_table(
    datasets: Sequence[str],
    counts: dict[str, int],
    errors: dict[str, str],
) -> str:
    """Format deterministic, dependency-free console output."""
    rows = [
        (dataset, "ERROR" if dataset in errors else str(counts[dataset]))
        for dataset in sorted(datasets)
    ]
    dataset_width = max([len("dataset"), *(len(dataset) for dataset, _ in rows)])
    count_width = max([len("row_count"), *(len(count) for _, count in rows)])
    lines = [
        f"{'dataset':<{dataset_width}} | {'row_count':>{count_width}}",
        f"{'-' * dataset_width}-+-{'-' * count_width}",
    ]
    lines.extend(f"{dataset:<{dataset_width}} | {count:>{count_width}}" for dataset, count in rows)
    return "\n".join(lines)


def _warning_value(value: Any) -> str:
    rendered = repr(value)
    return rendered if len(rendered) <= 120 else rendered[:117] + "..."


def _print_invalid_job_id_warning(invalid_job_ids: Sequence[Any]) -> None:
    if not invalid_job_ids:
        return
    examples = ", ".join(_warning_value(value) for value in invalid_job_ids[:MAX_INVALID_EXAMPLES])
    suffix = "" if len(invalid_job_ids) <= MAX_INVALID_EXAMPLES else ", ..."
    print(
        f"Warning: skipped {len(invalid_job_ids)} malformed job_id value(s); "
        f"examples: {examples}{suffix}",
        file=sys.stderr,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    with TemporaryDirectory(prefix="wt-serving-dataset-counts-") as temp_dir:
        output_dir = Path(temp_dir)
        try:
            job_ids = _discover_job_ids(
                output_dir / "distinct-job-ids.json",
                table=args.table,
                verbose=args.verbose,
            )
        except QueryCommandError as exc:
            print(f"Error: cannot discover job_id values: {exc}", file=sys.stderr)
            return 1

        datasets, invalid_job_ids = extract_datasets(job_ids)
        _print_invalid_job_id_warning(invalid_job_ids)
        print(
            "Warning: counts use contains-LIKE matching and may overlap; "
            "concurrent queries are not a single transactional snapshot.",
            file=sys.stderr,
        )

        counts, errors = count_datasets(
            datasets,
            output_dir,
            args.concurrency,
            table=args.table,
            verbose=args.verbose,
        )
        for dataset in sorted(errors):
            print(f"Error: dataset {dataset!r}: {errors[dataset]}", file=sys.stderr)

        print(format_result_table(datasets, counts, errors))
        return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
