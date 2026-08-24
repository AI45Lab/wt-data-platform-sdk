"""Count serving rows by the dataset name encoded in ``job_id``.

The dataset name is the trimmed first ``#``-delimited component of ``job_id``.
``job_id`` is authoritative here because it is required and immutable, while
``tags`` is an optional ETL-derived presentation field. The script captures one
fixed ID manifest through ``WTGatewayClient.export_data_batches()`` and then
aggregates ``job_id`` values locally in bounded batches.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
import sys
from typing import Any

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    GatewayConfig,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    TableConfig,
)


DEFAULT_BATCH_SIZE = 1000
DEFAULT_FILTER = "id IS NOT NULL"
MAX_INVALID_EXAMPLES = 5
ALLOWED_SERVING_TABLES = (DEFAULT_SERVING_TABLE, TEST_SERVING_TABLE)


@dataclass(frozen=True)
class DatasetCountResult:
    """One completed fixed-manifest dataset count."""

    counts: dict[str, int]
    total_rows: int
    invalid_rows: int
    invalid_examples: tuple[Any, ...]

    @property
    def counted_rows(self) -> int:
        return sum(self.counts.values())


def build_config(serving_table: str = DEFAULT_SERVING_TABLE) -> GatewayConfig:
    """Build a config pinned to one supported serving table."""
    if serving_table not in ALLOWED_SERVING_TABLES:
        raise ValueError(
            "delivery count table must be one of: "
            f"{', '.join(ALLOWED_SERVING_TABLES)}"
        )
    is_test = serving_table == TEST_SERVING_TABLE
    return GatewayConfig(
        tables=TableConfig(
            profile="test" if is_test else "production",
            landing_table=TEST_LANDING_TABLE if is_test else DEFAULT_LANDING_TABLE,
            serving_table=serving_table,
        )
    )


def dataset_from_job_id(job_id: Any) -> str | None:
    """Return the dataset prefix, or ``None`` for a malformed ``job_id``."""
    if not isinstance(job_id, str) or "#" not in job_id:
        return None
    dataset = job_id.split("#", 1)[0].strip()
    if not dataset or any(character in dataset for character in ("\x00", "\n", "\r")):
        return None
    return dataset


def count_datasets(
    client: WTGatewayClient,
    *,
    filter_query: str = DEFAULT_FILTER,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> DatasetCountResult:
    """Count one fixed set of serving rows without per-dataset rescans."""
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    normalized_filter = filter_query.strip()
    if not normalized_filter:
        raise ValueError("filter_query must be non-empty")

    table_name = client.config.tables.serving_table
    if table_name not in ALLOWED_SERVING_TABLES:
        raise ValueError(
            "delivery count table must be one of "
            f"{ALLOWED_SERVING_TABLES}, got '{table_name}'"
        )

    counts: Counter[str] = Counter()
    total_rows = 0
    invalid_rows = 0
    invalid_examples: list[Any] = []

    batches = client.export_data_batches(
        filter_query=normalized_filter,
        batch_size=batch_size,
        columns=["job_id"],
        deserialize_json=False,
    )
    for batch in batches:
        if "job_id" not in batch.columns:
            raise RuntimeError("dataset count batch did not return the job_id column")
        for job_id in batch["job_id"].tolist():
            total_rows += 1
            dataset = dataset_from_job_id(job_id)
            if dataset is None:
                invalid_rows += 1
                if len(invalid_examples) < MAX_INVALID_EXAMPLES:
                    invalid_examples.append(job_id)
                continue
            counts[dataset] += 1

    return DatasetCountResult(
        counts=dict(sorted(counts.items())),
        total_rows=total_rows,
        invalid_rows=invalid_rows,
        invalid_examples=tuple(invalid_examples),
    )


def format_result(result: DatasetCountResult) -> str:
    """Format deterministic, dependency-free console output."""
    rows = [(dataset, str(count)) for dataset, count in result.counts.items()]
    dataset_width = max([len("dataset"), *(len(dataset) for dataset, _ in rows)])
    count_width = max([len("row_count"), *(len(count) for _, count in rows)])
    lines = [
        f"{'dataset':<{dataset_width}} | {'row_count':>{count_width}}",
        f"{'-' * dataset_width}-+-{'-' * count_width}",
    ]
    lines.extend(
        f"{dataset:<{dataset_width}} | {count:>{count_width}}"
        for dataset, count in rows
    )
    lines.extend(
        [
            "",
            f"Rows in fixed manifest: {result.total_rows}",
            f"Rows counted: {result.counted_rows}",
            f"Rows skipped: {result.invalid_rows}",
        ]
    )
    return "\n".join(lines)


def _warning_value(value: Any) -> str:
    rendered = repr(value)
    return rendered if len(rendered) <= 120 else rendered[:117] + "..."


def print_invalid_warning(result: DatasetCountResult) -> None:
    """Report malformed job IDs without printing an unbounded value list."""
    if not result.invalid_rows:
        return
    examples = ", ".join(_warning_value(value) for value in result.invalid_examples)
    suffix = "" if result.invalid_rows <= len(result.invalid_examples) else ", ..."
    print(
        f"Warning: skipped {result.invalid_rows} malformed job_id value(s); "
        f"examples: {examples}{suffix}",
        file=sys.stderr,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Count serving rows by the first # delimited component of job_id. "
            "The command captures a fixed ID manifest and reads job_id in batches."
        )
    )
    parser.add_argument(
        "--table",
        choices=ALLOWED_SERVING_TABLES,
        default=DEFAULT_SERVING_TABLE,
        help=(
            "Serving table to count. Defaults to wind_tunnel_serving; "
            "use serving_test for integration validation."
        ),
    )
    parser.add_argument(
        "--filter",
        dest="filter_query",
        default=DEFAULT_FILTER,
        help=f"DLDB WHERE predicate without WHERE (default: {DEFAULT_FILTER!r}).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = build_config(args.table)
        with WTGatewayClient(config=config) as client:
            result = count_datasets(
                client,
                filter_query=args.filter_query,
            )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print_invalid_warning(result)
    print(format_result(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
