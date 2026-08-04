#!/usr/bin/env python3
"""Create missing indexes and optimize HASH buckets for landing or serving.

Examples:
  python scripts/ops/maintain_table_indexes.py \
    --table wind_tunnel_landing --all-partitions

  python scripts/ops/maintain_table_indexes.py \
    --table wind_tunnel_serving --all-partitions

  python scripts/ops/maintain_table_indexes.py \
    --table serving_test --partition job-123

Only the two production tables and two test tables are supported. Their table
names determine whether landing or serving index definitions are used.
"""
import argparse
import json
import sys

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
    TableConfig,
    default_config,
)


TABLE_ROLES = {
    DEFAULT_LANDING_TABLE: "landing",
    TEST_LANDING_TABLE: "landing",
    DEFAULT_SERVING_TABLE: "serving",
    TEST_SERVING_TABLE: "serving",
}


def resolve_table_role(table_name: str) -> str:
    """Resolve the fixed role of one supported production/test table."""
    if table_name not in TABLE_ROLES:
        raise ValueError(
            f"Unsupported table {table_name!r}; expected one of: "
            f"{', '.join(sorted(TABLE_ROLES))}"
        )
    return TABLE_ROLES[table_name]


def _parse_partition(value: str):
    try:
        return int(value)
    except ValueError:
        return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create missing role-specific indexes and optimize selected "
            "HASH(job_id) buckets"
        )
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=sorted(TABLE_ROLES),
        help="Supported production or test table to maintain.",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: configured WT_SDK_DB_URI).",
    )
    parser.add_argument(
        "--partition",
        action="append",
        default=None,
        help="Raw job_id or HASH bucket integer. Can be repeated.",
    )
    parser.add_argument(
        "--all-partitions",
        action="store_true",
        help="Maintain every existing physical HASH bucket for the table.",
    )
    parser.add_argument(
        "--columns",
        default=None,
        help="Comma-separated subset of configured scalar-index columns.",
    )
    parser.add_argument(
        "--no-create-missing",
        action="store_true",
        help="Only optimize; do not create missing indexes.",
    )
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Only create missing indexes; do not optimize buckets.",
    )

    args = parser.parse_args()
    if args.all_partitions and args.partition:
        parser.error("use either --all-partitions or --partition, not both")
    if not args.all_partitions and not args.partition:
        parser.error("provide --all-partitions or at least one --partition")

    resolve_table_role(args.table)

    tables = TableConfig(
        db_uri=args.db_uri or default_config.tables.db_uri,
    )
    config = GatewayConfig(
        s3=default_config.s3,
        tables=tables,
        dldb_model=default_config.dldb_model,
        enable_dldb_timing_logs=default_config.enable_dldb_timing_logs,
        log_dldb_metrics_summary_on_close=default_config.log_dldb_metrics_summary_on_close,
        dldb_metrics_log_path=default_config.dldb_metrics_log_path,
    )

    columns = None
    if args.columns:
        columns = [column.strip() for column in args.columns.split(",") if column.strip()]
    partitions = (
        [_parse_partition(value) for value in args.partition]
        if args.partition
        else None
    )

    client = WTGatewayClient(config)
    try:
        summary = client.maintain_table_indexes(
            args.table,
            partitions=partitions,
            all_partitions=args.all_partitions,
            columns=columns,
            create_missing=not args.no_create_missing,
            optimize=not args.no_optimize,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        return 1 if summary.get("errors") else 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
