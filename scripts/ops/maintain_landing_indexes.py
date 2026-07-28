#!/usr/bin/env python3
"""
Maintain landing scalar indexes for HASH(job_id) tables.

This script creates missing scalar indexes on selected HASH buckets and then
runs dldb optimize so newly appended data is incorporated into existing indexes.

Examples:
  # Maintain all existing buckets in landing_test
  python scripts/ops/maintain_landing_indexes.py --table landing_test --all-partitions

  # Maintain buckets for specific raw job_ids or bucket numbers
  python scripts/ops/maintain_landing_indexes.py --table landing_test --partition job-123 --partition 42

  # Create missing indexes only, without optimize
  python scripts/ops/maintain_landing_indexes.py --table landing_test --all-partitions --no-optimize
"""
import argparse
import json
import sys

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import GatewayConfig, TableConfig, default_config


def _parse_partition(value: str):
    try:
        return int(value)
    except ValueError:
        return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Maintain scalar indexes for a HASH(job_id) landing table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--table",
        default="landing_test",
        help="Landing logical table to maintain (default: landing_test)",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: s3://wind-tunnel-dldb)",
    )
    parser.add_argument(
        "--partition",
        action="append",
        default=None,
        help="Raw job_id or HASH bucket int to maintain. Can be specified multiple times.",
    )
    parser.add_argument(
        "--all-partitions",
        action="store_true",
        help="Maintain all existing physical HASH buckets for the landing table.",
    )
    parser.add_argument(
        "--columns",
        default=None,
        help="Comma-separated scalar index columns to maintain. Default uses LANDING_SCALAR_INDEXES.",
    )
    parser.add_argument(
        "--no-create-missing",
        action="store_true",
        help="Do not create missing indexes before optimize.",
    )
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Do not run optimize after creating missing indexes.",
    )

    args = parser.parse_args()

    if not args.all_partitions and not args.partition:
        print("Error: provide --all-partitions or at least one --partition")
        return 1

    config = GatewayConfig(
        s3=default_config.s3,
        tables=TableConfig(
            db_uri=args.db_uri or default_config.tables.db_uri,
            landing_table=args.table,
            serving_table=default_config.tables.serving_table,
        ),
        dldb_model=default_config.dldb_model,
        enable_dldb_timing_logs=default_config.enable_dldb_timing_logs,
        log_dldb_metrics_summary_on_close=default_config.log_dldb_metrics_summary_on_close,
        dldb_metrics_log_path=default_config.dldb_metrics_log_path,
    )

    columns = None
    if args.columns:
        columns = [column.strip() for column in args.columns.split(",") if column.strip()]

    partitions = None
    if args.partition:
        partitions = [_parse_partition(value) for value in args.partition]

    client = WTGatewayClient(config)
    try:
        summary = client.maintain_landing_indexes(
            partitions=partitions,
            all_partitions=args.all_partitions,
            columns=columns,
            create_missing=not args.no_create_missing,
            optimize=not args.no_optimize,
            clear_tracked=False,
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str))
        return 1 if summary.get("errors") else 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
