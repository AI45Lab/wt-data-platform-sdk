#!/usr/bin/env python3
"""Count delivered serving rows by ``job_id`` reporting prefix.

The default reporting key is:

    dataset#harness#model#task

For ``cybergym`` only, the reporting key additionally includes the ``level*``
component found after the first four fields:

    cybergym#harness#model#task#level

For each key, this script reports serving-table counts only:

1. ``success_rows``: serving rows whose ``reward > 0``.
2. ``delivered_rows``: all serving rows in that reporting group.

The implementation is intentionally read-only and narrow-column: it scans each
matching serving row once and only fetches ``job_id`` and ``reward``. When
exact ``--job-id`` values are supplied, the SDK can prune HASH buckets and avoid
the prefix-wide table scan. The script avoids reading the wide JSON trajectory
columns.
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
    DEFAULT_SERVING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
    TableConfig,
)


TASK_LABELS_ZH = {
    "exploit": "利用",
    "find": "挖掘",
    "mining-patch": "修复",
}


PrefixKey = tuple[str, str, str, str, str | None]


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
    """Parse one report prefix.

    Non-cybergym prefixes normally contain four components. Cybergym prefixes
    may contain a fifth ``level*`` component such as
    ``cybergym#opencode#kimi-k3#find#level1``.
    """
    parts = [part.strip() for part in value.split("#")]
    if len(parts) not in {4, 5} or any(not part for part in parts):
        raise ValueError(
            "prefix must have four components dataset#harness#model#task, "
            "or five components for cybergym: dataset#harness#model#task#level"
        )
    if len(parts) == 5 and parts[0] != "cybergym":
        raise ValueError("five-component prefixes are only supported for cybergym")
    return (parts[0], parts[1], parts[2], parts[3], parts[4] if len(parts) == 5 else None)


def prefix_to_string(prefix: PrefixKey) -> str:
    dataset, harness, model, task, level = prefix
    parts = [dataset, harness, model, task]
    if level:
        parts.append(level)
    return "#".join(parts)


def prefix_to_sql_base(prefix: PrefixKey) -> str:
    """Return the first-four-component prefix used for SQL narrowing."""
    return "#".join(prefix[:4])


def key_from_job_id(job_id: Any) -> PrefixKey | None:
    """Return the reporting key for one job_id, or None for malformed values."""
    if not isinstance(job_id, str):
        return None
    parts = [part.strip() for part in job_id.split("#")]
    if len(parts) < 4 or any(not part for part in parts[:4]):
        return None
    dataset, harness, model, task = parts[:4]
    level = None
    if dataset == "cybergym":
        level = next(
            (
                part
                for part in parts[4:]
                if part.lower().startswith("level") and len(part) > len("level")
            ),
            None,
        )
    return (dataset, harness, model, task, level)


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


def load_text_values_file(path: str | None, *, label: str) -> list[str]:
    if not path:
        return []
    values: list[str] = []
    for line_number, raw_line in enumerate(Path(path).read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "\x00" in line:
            raise ValueError(f"{path}:{line_number}: {label} contains a NUL byte")
        values.append(line)
    return values


def build_prefix_filter(prefixes: Sequence[PrefixKey]) -> str | None:
    """Build an optional SQL filter that narrows rows returned by dldb.

    HASH partition pruning cannot use a job_id prefix, so the script still scans
    all existing buckets. The SQL LIKE clauses reduce network transfer and local
    aggregation work when the caller provides a small fixed report list.
    """
    if not prefixes:
        return None
    clauses: list[str] = []
    for value in sorted({prefix_to_sql_base(prefix) for prefix in prefixes}):
        exact = _escape_sql_string(value)
        like = _escape_sql_like_pattern(value + "#")
        clauses.append(f"(job_id = '{exact}' OR job_id LIKE '{like}%' ESCAPE '\\')")
    return "(" + " OR ".join(clauses) + ")"


def build_job_id_filter(job_ids: Sequence[str]) -> str:
    if not job_ids:
        raise ValueError("job_ids must be non-empty")
    quoted = ", ".join(f"'{_escape_sql_string(job_id)}'" for job_id in job_ids)
    return f"job_id IN ({quoted})"


def build_config(
    *,
    profile: str,
    serving_table: str | None,
) -> GatewayConfig:
    normalized_profile = "production" if profile in {"prod", "production"} else "test"
    default_serving = (
        DEFAULT_SERVING_TABLE if normalized_profile == "production" else TEST_SERVING_TABLE
    )
    return GatewayConfig(
        tables=TableConfig(
            profile=normalized_profile,
            serving_table=serving_table or default_serving,
        )
    )


def key_matches_requested(key: PrefixKey, requested: PrefixKey) -> bool:
    """Return whether an observed key belongs under a requested prefix."""
    if key[:4] != requested[:4]:
        return False
    requested_level = requested[4]
    return requested_level is None or key[4] == requested_level


def should_include_key(key: PrefixKey, prefixes: Sequence[PrefixKey] | None) -> bool:
    if not prefixes:
        return True
    return any(key_matches_requested(key, requested) for requested in prefixes)


def _is_positive_reward(value: Any) -> bool:
    if value is None:
        return False
    try:
        return float(value) > 0.0
    except (TypeError, ValueError):
        return False


def _accumulate_rows(
    rows: Sequence[dict[str, Any]],
    *,
    prefixes: Sequence[PrefixKey] | None,
    success_counts: Counter[PrefixKey],
    delivered_counts: Counter[PrefixKey],
) -> tuple[int, int]:
    """Accumulate serving rows and return scanned/invalid row counts."""
    rows_scanned = 0
    invalid_rows = 0
    for row in rows:
        rows_scanned += 1
        key = key_from_job_id(row.get("job_id"))
        if key is None:
            invalid_rows += 1
            continue
        if not should_include_key(key, prefixes):
            continue
        delivered_counts[key] += 1
        if _is_positive_reward(row.get("reward")):
            success_counts[key] += 1
    return rows_scanned, invalid_rows


def scan_serving_counts_by_filter(
    client: WTGatewayClient,
    *,
    table_name: str,
    filter_query: str,
    prefixes: Sequence[PrefixKey] | None,
) -> tuple[Counter[PrefixKey], Counter[PrefixKey], int, int]:
    """Scan matching serving rows with one narrow logical-table query."""
    success_counts: Counter[PrefixKey] = Counter()
    delivered_counts: Counter[PrefixKey] = Counter()
    rows = client.query_data(
        table=table_name,
        filter_query=filter_query,
        columns=["job_id", "reward"],
        limit=None,
        checkout_latest=True,
        exclude_none=False,
        deserialize_json=False,
    )
    rows_scanned, invalid_rows = _accumulate_rows(
        rows,
        prefixes=prefixes,
        success_counts=success_counts,
        delivered_counts=delivered_counts,
    )
    return success_counts, delivered_counts, rows_scanned, invalid_rows


def scan_serving_counts_by_job_ids(
    client: WTGatewayClient,
    *,
    table_name: str,
    job_ids: Sequence[str],
    prefixes: Sequence[PrefixKey] | None,
    batch_size: int = 50,
) -> tuple[Counter[PrefixKey], Counter[PrefixKey], int, int]:
    """Scan exact job IDs in HASH-pruned batches."""
    success_counts: Counter[PrefixKey] = Counter()
    delivered_counts: Counter[PrefixKey] = Counter()
    rows_scanned = 0
    invalid_rows = 0
    for offset in range(0, len(job_ids), batch_size):
        batch = job_ids[offset : offset + batch_size]
        rows = client.query_data(
            table=table_name,
            filter_query=build_job_id_filter(batch),
            columns=["job_id", "reward"],
            limit=None,
            checkout_latest=True,
            exclude_none=False,
            deserialize_json=False,
        )
        scanned, invalid = _accumulate_rows(
            rows,
            prefixes=prefixes,
            success_counts=success_counts,
            delivered_counts=delivered_counts,
        )
        rows_scanned += scanned
        invalid_rows += invalid
    return success_counts, delivered_counts, rows_scanned, invalid_rows


def build_rows(
    success_counts: Counter[PrefixKey],
    delivered_counts: Counter[PrefixKey],
    requested_prefixes: Sequence[PrefixKey],
    *,
    task_label: str,
) -> list[dict[str, Any]]:
    if requested_prefixes:
        observed_keys = sorted(set(success_counts) | set(delivered_counts))
        keys: list[PrefixKey] = []
        for requested in requested_prefixes:
            matches = [key for key in observed_keys if key_matches_requested(key, requested)]
            keys.extend(matches or [requested])
        keys = list(dict.fromkeys(keys))
    else:
        keys = sorted(set(success_counts) | set(delivered_counts))

    rows: list[dict[str, Any]] = []
    for dataset, harness, model, task, level in keys:
        display_task = TASK_LABELS_ZH.get(task, task) if task_label == "zh" else task
        rows.append(
            {
                "dataset": dataset,
                "harness": harness,
                "model": model,
                "task": display_task,
                "level": level or "",
                "job_prefix": prefix_to_string((dataset, harness, model, task, level)),
                "success_rows": int(success_counts[(dataset, harness, model, task, level)]),
                "delivered_rows": int(delivered_counts[(dataset, harness, model, task, level)]),
            }
        )
    return rows


def format_markdown(rows: Sequence[dict[str, Any]]) -> str:
    headers = [
        ("dataset", "数据集"),
        ("harness", "harness"),
        ("model", "model"),
        ("task", "任务"),
        ("level", "level"),
        ("success_rows", "成功条数"),
        ("delivered_rows", "总条数"),
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
    serving_table: str,
    serving_scanned: int,
    serving_invalid: int,
) -> None:
    print(
        "Scan summary: "
        f"{serving_table} matched_rows={serving_scanned}, invalid_job_id={serving_invalid}",
        file=sys.stderr,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Count serving success rows and delivered rows by "
            "dataset#harness#model#task job_id prefix. Cybergym is split by level."
        )
    )
    parser.add_argument(
        "--profile",
        choices=("test", "production", "prod"),
        default="production",
        help="Table profile to use. Defaults to production.",
    )
    parser.add_argument("--serving-table", default=None)
    parser.add_argument(
        "--prefix",
        action="append",
        default=[],
        help=(
            "One dataset#harness#model#task prefix to report. "
            "For cybergym, optionally pass dataset#harness#model#task#level. "
            "Can be repeated. If omitted, all prefixes found in serving are reported."
        ),
    )
    parser.add_argument(
        "--prefix-file",
        default=None,
        help=(
            "Optional text file containing one prefix per line. "
            "Cybergym may use dataset#harness#model#task#level."
        ),
    )
    parser.add_argument(
        "--job-id",
        action="append",
        default=[],
        help=(
            "Exact job_id to include. Can be repeated. When supplied, the script "
            "uses HASH-pruned exact job_id queries instead of a prefix-wide scan."
        ),
    )
    parser.add_argument(
        "--job-id-file",
        default=None,
        help="Optional text file containing one exact job_id per line.",
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
        requested_job_ids = list(dict.fromkeys([
            *args.job_id,
            *load_text_values_file(args.job_id_file, label="job_id"),
        ]))
        prefix_sql = build_prefix_filter(requested_prefixes)

        config = build_config(
            profile=args.profile,
            serving_table=args.serving_table,
        )

        serving_filter_parts = ["job_id IS NOT NULL"]
        if prefix_sql:
            serving_filter_parts.append(prefix_sql)

        with WTGatewayClient(config=config) as client:
            if requested_job_ids:
                success_counts, delivered_counts, serving_scanned, serving_invalid = (
                    scan_serving_counts_by_job_ids(
                        client,
                        table_name=config.tables.serving_table,
                        job_ids=requested_job_ids,
                        prefixes=requested_prefixes or None,
                    )
                )
            else:
                success_counts, delivered_counts, serving_scanned, serving_invalid = (
                    scan_serving_counts_by_filter(
                        client,
                        table_name=config.tables.serving_table,
                        filter_query=" AND ".join(f"({part})" for part in serving_filter_parts),
                        prefixes=requested_prefixes or None,
                    )
                )

        rows = build_rows(
            success_counts,
            delivered_counts,
            requested_prefixes,
            task_label=args.task_label,
        )
        print(format_markdown(rows))

        if not args.quiet:
            _print_scan_summary(
                serving_table=config.tables.serving_table,
                serving_scanned=serving_scanned,
                serving_invalid=serving_invalid,
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
