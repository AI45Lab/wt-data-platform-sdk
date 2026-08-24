#!/usr/bin/env python3
"""Count success and delivery rows by the first four ``job_id`` components.

The reporting key is:

    dataset#harness#model#task

For each key, this script reports:

1. ``landing_success_rows``: rows in the landing table whose ``job_id`` starts
   with that key and whose ``reward != 0``.
2. ``serving_delivered_rows``: rows in the serving table whose ``job_id`` starts
   with that key.

The implementation is intentionally read-only and narrow-column: it scans each
existing HASH partition at most once per table and only fetches ``job_id``.
That is usually much cheaper than running one ``LIKE`` query per reporting row,
and it avoids reading the wide JSON trajectory columns.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
import sys
from typing import Any

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
    TableConfig,
)


TASK_LABELS_ZH = {
    "exploit": "利用",
    "find": "挖掘",
    "mining-patch": "修复",
}


PrefixKey = tuple[str, str, str, str]


def _escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _escape_sql_like_pattern(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
        .replace("'", "''")
    )


def parse_prefix(value: str) -> PrefixKey:
    """Parse one ``dataset#harness#model#task`` prefix."""
    parts = [part.strip() for part in value.split("#")]
    if len(parts) != 4 or any(not part for part in parts):
        raise ValueError(
            "prefix must have exactly four non-empty components: "
            "dataset#harness#model#task"
        )
    return (parts[0], parts[1], parts[2], parts[3])


def prefix_to_string(prefix: PrefixKey) -> str:
    return "#".join(prefix)


def key_from_job_id(job_id: Any) -> PrefixKey | None:
    """Return the first four job_id components, or None for malformed values."""
    if not isinstance(job_id, str):
        return None
    parts = [part.strip() for part in job_id.split("#", 4)[:4]]
    if len(parts) != 4 or any(not part for part in parts):
        return None
    return (parts[0], parts[1], parts[2], parts[3])


def load_prefix_file(path: str | None) -> list[PrefixKey]:
    if not path:
        return []
    prefixes: list[PrefixKey] = []
    for line_number, raw_line in enumerate(Path(path).read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            prefixes.append(parse_prefix(line))
        except ValueError as exc:
            raise ValueError(f"{path}:{line_number}: {exc}") from exc
    return prefixes


def build_prefix_filter(prefixes: Sequence[PrefixKey]) -> str | None:
    """Build an optional SQL filter that narrows rows returned by dldb.

    HASH partition pruning cannot use a job_id prefix, so the script still scans
    all existing buckets. The SQL LIKE clauses reduce network transfer and local
    aggregation work when the caller provides a small fixed report list.
    """
    if not prefixes:
        return None
    clauses: list[str] = []
    for prefix in prefixes:
        value = prefix_to_string(prefix)
        exact = _escape_sql_string(value)
        like = _escape_sql_like_pattern(value + "#")
        clauses.append(f"(job_id = '{exact}' OR job_id LIKE '{like}%' ESCAPE '\\')")
    return "(" + " OR ".join(clauses) + ")"


def build_config(
    *,
    profile: str,
    landing_table: str | None,
    serving_table: str | None,
) -> GatewayConfig:
    normalized_profile = "production" if profile in {"prod", "production"} else "test"
    default_landing = (
        DEFAULT_LANDING_TABLE if normalized_profile == "production" else TEST_LANDING_TABLE
    )
    default_serving = (
        DEFAULT_SERVING_TABLE if normalized_profile == "production" else TEST_SERVING_TABLE
    )
    return GatewayConfig(
        tables=TableConfig(
            profile=normalized_profile,
            landing_table=landing_table or default_landing,
            serving_table=serving_table or default_serving,
        )
    )


def scan_job_id_counts(
    client: WTGatewayClient,
    *,
    table_name: str,
    filter_query: str,
    prefixes: set[PrefixKey] | None,
) -> tuple[Counter[PrefixKey], int, int]:
    """Scan one logical table and return counts, scanned rows, invalid rows."""
    counts: Counter[PrefixKey] = Counter()
    rows_scanned = 0
    invalid_rows = 0

    partitions = client.list_table_partitions(table_name)
    if not partitions:
        return counts, rows_scanned, invalid_rows

    for partition in partitions:
        frame = client._filter_table(  # noqa: SLF001 - inspect script, SDK-only path.
            table_name,
            query=filter_query,
            limit=None,
            columns=["job_id"],
            partitions=[partition],
            checkout_latest=True,
            extra={
                "api": "count_job_prefix_delivery",
                "partition": partition,
            },
        )
        if "job_id" not in frame.columns:
            raise RuntimeError(f"{table_name} query did not return job_id")

        rows_scanned += len(frame)
        for job_id in frame["job_id"].tolist():
            key = key_from_job_id(job_id)
            if key is None:
                invalid_rows += 1
                continue
            if prefixes is not None and key not in prefixes:
                continue
            counts[key] += 1

    return counts, rows_scanned, invalid_rows


def build_rows(
    landing_counts: Counter[PrefixKey],
    serving_counts: Counter[PrefixKey],
    requested_prefixes: Sequence[PrefixKey],
    *,
    task_label: str,
) -> list[dict[str, Any]]:
    if requested_prefixes:
        keys = list(dict.fromkeys(requested_prefixes))
    else:
        keys = sorted(set(landing_counts) | set(serving_counts))

    rows: list[dict[str, Any]] = []
    for dataset, harness, model, task in keys:
        display_task = TASK_LABELS_ZH.get(task, task) if task_label == "zh" else task
        rows.append(
            {
                "dataset": dataset,
                "harness": harness,
                "model": model,
                "task": display_task,
                "job_prefix": "#".join((dataset, harness, model, task)),
                "landing_success_rows": int(landing_counts[(dataset, harness, model, task)]),
                "serving_delivered_rows": int(serving_counts[(dataset, harness, model, task)]),
            }
        )
    return rows


def format_markdown(rows: Sequence[dict[str, Any]]) -> str:
    headers = [
        ("dataset", "数据集"),
        ("harness", "harness"),
        ("model", "model"),
        ("task", "任务"),
        ("landing_success_rows", "成功条数"),
        ("serving_delivered_rows", "总条数"),
    ]
    lines = [
        "| " + " | ".join(title for _, title in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(row[key]) for key, _ in headers) + " |")
    return "\n".join(lines)


def _print_scan_summary(
    *,
    landing_table: str,
    serving_table: str,
    landing_scanned: int,
    serving_scanned: int,
    landing_invalid: int,
    serving_invalid: int,
) -> None:
    print(
        "Scan summary: "
        f"{landing_table} matched_rows={landing_scanned}, invalid_job_id={landing_invalid}; "
        f"{serving_table} matched_rows={serving_scanned}, invalid_job_id={serving_invalid}",
        file=sys.stderr,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Count landing success rows and serving delivered rows by "
            "dataset#harness#model#task job_id prefix."
        )
    )
    parser.add_argument(
        "--profile",
        choices=("test", "production", "prod"),
        default="production",
        help="Table profile to use. Defaults to production.",
    )
    parser.add_argument("--landing-table", default=None)
    parser.add_argument("--serving-table", default=None)
    parser.add_argument(
        "--prefix",
        action="append",
        default=[],
        help=(
            "One dataset#harness#model#task prefix to report. "
            "Can be repeated. If omitted, all prefixes found in either table are reported."
        ),
    )
    parser.add_argument(
        "--prefix-file",
        default=None,
        help="Optional text file containing one dataset#harness#model#task prefix per line.",
    )
    parser.add_argument(
        "--task-label",
        choices=("raw", "zh"),
        default="raw",
        help="Render task as raw job_id value or a small built-in Chinese label map.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print the scan summary to stderr.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        requested_prefixes = [parse_prefix(value) for value in args.prefix]
        requested_prefixes.extend(load_prefix_file(args.prefix_file))
        prefix_set = set(requested_prefixes) if requested_prefixes else None
        prefix_sql = build_prefix_filter(requested_prefixes)

        config = build_config(
            profile=args.profile,
            landing_table=args.landing_table,
            serving_table=args.serving_table,
        )

        landing_filter_parts = ["job_id IS NOT NULL", "reward != 0"]
        serving_filter_parts = ["job_id IS NOT NULL"]
        if prefix_sql:
            landing_filter_parts.append(prefix_sql)
            serving_filter_parts.append(prefix_sql)

        with WTGatewayClient(config=config) as client:
            landing_counts, landing_scanned, landing_invalid = scan_job_id_counts(
                client,
                table_name=config.tables.landing_table,
                filter_query=" AND ".join(f"({part})" for part in landing_filter_parts),
                prefixes=prefix_set,
            )
            serving_counts, serving_scanned, serving_invalid = scan_job_id_counts(
                client,
                table_name=config.tables.serving_table,
                filter_query=" AND ".join(f"({part})" for part in serving_filter_parts),
                prefixes=prefix_set,
            )

        rows = build_rows(
            landing_counts,
            serving_counts,
            requested_prefixes,
            task_label=args.task_label,
        )
        print(format_markdown(rows))

        if not args.quiet:
            _print_scan_summary(
                landing_table=config.tables.landing_table,
                serving_table=config.tables.serving_table,
                landing_scanned=landing_scanned,
                serving_scanned=serving_scanned,
                landing_invalid=landing_invalid,
                serving_invalid=serving_invalid,
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
