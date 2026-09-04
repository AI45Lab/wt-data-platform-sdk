#!/usr/bin/env python3
"""Show read-only fragment and scalar-index health for an env-config table.

Environment-config tables are unpartitioned dldb SimpleTables, so the HASH-only
``show_partition_status.py`` command cannot inspect them. This command reports
their Lance fragment statistics plus dldb index definitions and index coverage.

Examples:
  python scripts/inspect/show_env_config_status.py
  python scripts/inspect/show_env_config_status.py --profile production
  python scripts/inspect/show_env_config_status.py --profile production --json
"""

import argparse
import json
import sys
from typing import Any, Mapping

import dldb

from wt_sdk.config import (
    S3Config,
    TEST_ENV_CONFIG_TABLE,
    resolve_env_config_db_uri,
    resolve_env_config_table_name,
)
from wt_sdk.core.evaluation_env_schema import SCALAR_INDEX_COLUMNS


def _read_attr(value: Any, name: str, default=None):
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _normalize_index(index: Any) -> dict[str, Any]:
    return {
        "name": _read_attr(index, "name"),
        "type": _read_attr(index, "index_type", _read_attr(index, "type")),
        "columns": list(_read_attr(index, "columns", []) or []),
        "num_indexed_rows": _read_attr(index, "num_indexed_rows"),
        "num_unindexed_rows": _read_attr(index, "num_unindexed_rows"),
        "size_bytes": _read_attr(index, "size_bytes"),
        "num_segments": _read_attr(index, "num_segments"),
        "index_version": _read_attr(index, "index_version"),
    }


def _normalize_coverage(item: Any) -> dict[str, Any]:
    return {
        "index_name": _read_attr(item, "index_name"),
        "num_indexed_rows": _read_attr(item, "num_indexed_rows"),
        "num_unindexed_rows": _read_attr(item, "num_unindexed_rows"),
        "fully_indexed": bool(_read_attr(item, "fully_indexed", False)),
    }


def inspect_env_config_status(
    session,
    *,
    table_name: str = TEST_ENV_CONFIG_TABLE,
) -> dict[str, Any]:
    """Return a read-only health snapshot for one env-config SimpleTable."""
    record = session.schema_table.get(table_name)
    if record is None:
        raise ValueError(f"Table {table_name!r} does not exist in dldb information_schema")
    if str(getattr(record, "partition_column", "") or "").strip():
        raise ValueError(
            f"Table {table_name!r} is partitioned; use show_partition_status.py instead"
        )

    indexes = sorted(
        (_normalize_index(item) for item in session.list_indices(table_name)),
        key=lambda item: str(item["name"]),
    )
    coverage = sorted(
        (_normalize_coverage(item) for item in session.list_index_coverage(table_name)),
        key=lambda item: str(item["index_name"]),
    )

    # dldb 1.1.2 exposes partition_status only for HASH tables. Resolve the
    # exact SimpleTable through dldb and read its non-mutating Lance stats.
    table = session._get_table(table_name)
    lance_table = getattr(table, "table", None)
    stats_reader = getattr(lance_table, "stats", None)
    if not callable(stats_reader):
        raise RuntimeError(
            "Installed dldb/Lance table does not expose read-only stats()"
        )
    stats = stats_reader()
    if not isinstance(stats, Mapping):
        raise RuntimeError(f"Unexpected table stats type: {type(stats).__name__}")
    stats = dict(stats)

    expected_indexes = sorted(f"{column}_idx" for column in SCALAR_INDEX_COLUMNS)
    existing_indexes = sorted(
        str(item["name"]) for item in indexes if item.get("name")
    )
    missing_indexes = sorted(set(expected_indexes) - set(existing_indexes))
    unexpected_indexes = sorted(set(existing_indexes) - set(expected_indexes))
    index_tails = [
        item
        for item in coverage
        if not item["fully_indexed"] or (item["num_unindexed_rows"] or 0) > 0
    ]

    fragment_stats = stats.get("fragment_stats") or {}
    fragments = int(fragment_stats.get("num_fragments") or 0)
    small_fragments = int(fragment_stats.get("num_small_fragments") or 0)
    issues = []
    if fragments > 1 and small_fragments > 0:
        issues.append("fragmented")
    if missing_indexes:
        issues.append("missing_indexes")
    if index_tails:
        issues.append("unindexed_tail")

    return {
        "table_name": table_name,
        "state": ",".join(issues) if issues else "ok",
        "read_only": True,
        "stats": stats,
        "expected_indexes": expected_indexes,
        "existing_indexes": existing_indexes,
        "missing_indexes": missing_indexes,
        "unexpected_indexes": unexpected_indexes,
        "indexes": indexes,
        "index_coverage": coverage,
        "index_tails": index_tails,
    }


def _format_bytes(value: Any) -> str:
    if value is None:
        return "-"
    size = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if abs(size) < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024
    return str(value)


def _format_number(value: Any) -> str:
    return "-" if value is None else f"{int(value):,}"


def _print_report(db_uri: str, report: dict[str, Any]) -> None:
    stats = report["stats"]
    fragment_stats = stats.get("fragment_stats") or {}
    lengths = fragment_stats.get("lengths") or {}

    print(f"Environment-config database: {db_uri}")
    print(f"Table: {report['table_name']}")
    print(f"State: {report['state']}")
    print("Read only: true")
    print("=" * 80)
    print(f"Rows: {_format_number(stats.get('num_rows'))}")
    print(f"Total bytes: {_format_bytes(stats.get('total_bytes'))}")
    print(f"Fragments: {_format_number(fragment_stats.get('num_fragments'))}")
    print(
        "Small fragments: "
        f"{_format_number(fragment_stats.get('num_small_fragments'))}"
    )
    print(
        "Fragment rows (min/p50/p99/max): "
        f"{_format_number(lengths.get('min'))}/"
        f"{_format_number(lengths.get('p50'))}/"
        f"{_format_number(lengths.get('p99'))}/"
        f"{_format_number(lengths.get('max'))}"
    )
    print(f"Expected indexes: {', '.join(report['expected_indexes']) or '(none)'}")
    print(f"Existing indexes: {', '.join(report['existing_indexes']) or '(none)'}")
    print(f"Missing indexes: {', '.join(report['missing_indexes']) or '(none)'}")
    print(
        "Unexpected indexes: "
        f"{', '.join(report['unexpected_indexes']) or '(none)'}"
    )

    print("\nIndex coverage:")
    if not report["index_coverage"]:
        print("  (none)")
        return
    print(f"  {'index':<24} {'indexed':>12} {'unindexed':>12} {'complete':>10}")
    for item in report["index_coverage"]:
        indexed = item["num_indexed_rows"]
        unindexed = item["num_unindexed_rows"]
        print(
            f"  {str(item['index_name']):<24} "
            f"{str(indexed if indexed is not None else '-'):>12} "
            f"{str(unindexed if unindexed is not None else '-'):>12} "
            f"{str(item['fully_indexed']).lower():>10}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Show read-only fragment and scalar-index health for a "
            "profile-selected env-config table"
        )
    )
    parser.add_argument(
        "--profile",
        choices=("test", "prod", "production"),
        default="test",
        help=(
            "Select env_config_test for test or evaluation_env_config for "
            "production. Defaults to test."
        ),
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help=(
            "Database URI (default: WT_SDK_ENV_CONFIG_DB_URI, then "
            "s3://wind-tunnel-env-config)."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the complete machine-readable report as JSON.",
    )
    args = parser.parse_args()

    db_uri = resolve_env_config_db_uri(args.db_uri)
    table_name = resolve_env_config_table_name(profile=args.profile)
    session = dldb.connect(
        db_uri,
        storage_options=S3Config().to_storage_options(),
    )
    try:
        report = inspect_env_config_status(session, table_name=table_name)
        if args.json:
            print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        else:
            _print_report(db_uri, report)
        return 0
    except Exception as exc:
        print(f"Error inspecting env-config table: {exc}", file=sys.stderr)
        return 1
    finally:
        session.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
