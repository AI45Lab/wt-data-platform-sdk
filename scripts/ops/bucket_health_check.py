#!/usr/bin/env python3
"""Read-only health check for HASH buckets of landing/serving tables.

Reports per-bucket rows, fragments, rows/fragment, version, and worst index
unindexed ratio, then flags buckets that need maintenance:

  - fragments > --max-frags (default 200), or
  - rows/fragment < --min-rows-per-frag (default 50), or (both only when
    fragments >= --min-frags, so tiny cold buckets are skipped)
  - version > --max-versions (default 3000), or
  - worst index unindexed ratio > --max-unindexed-ratio (default 0.2)

Examples:
  python scripts/ops/bucket_health_check.py --table wind_tunnel_landing

  # Print only flagged bucket ids (for the scheduled maintain wrapper):
  python scripts/ops/bucket_health_check.py --table wind_tunnel_landing --print-partitions
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

# dldb reconfigures the global loguru sink to stdout at import time, which would
# pollute --print-partitions machine output. Force logs back to stderr.
from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO")


TABLE_ROLES = {
    DEFAULT_LANDING_TABLE: "landing",
    TEST_LANDING_TABLE: "landing",
    DEFAULT_SERVING_TABLE: "serving",
    TEST_SERVING_TABLE: "serving",
}


def _worst_unindexed_ratio(coverage) -> float:
    worst = 0.0
    for row in coverage or []:
        indexed = getattr(row, "num_indexed_rows", None) or 0
        unindexed = getattr(row, "num_unindexed_rows", None) or 0
        denom = indexed + unindexed
        if denom > 0:
            worst = max(worst, unindexed / denom)
    return worst


def _bucket_report(client, table_name, partition):
    status = client.session.partition_status(table_name, partition=partition)
    stats = getattr(status, "stats", None)
    fragment_stats = getattr(stats, "fragment_stats", None) if stats else None
    rows = getattr(stats, "num_rows", None) if stats else None
    frags = getattr(fragment_stats, "num_fragments", None) if fragment_stats else None
    unindexed_ratio = _worst_unindexed_ratio(getattr(status, "coverage", None))
    return {
        "partition": partition,
        "materialized": bool(getattr(status, "materialized", False)),
        "rows": rows,
        "fragments": frags,
        "rows_per_fragment": (
            round(rows / frags, 1) if rows is not None and frags else None
        ),
        "version": getattr(status, "version", None),
        "worst_unindexed_ratio": round(unindexed_ratio, 4),
    }


def _flags_for(report, args):
    flags = []
    frags = report["fragments"] or 0
    rows = report["rows"] or 0
    if frags >= args.min_frags:
        if frags > args.max_frags:
            flags.append(f"frags>{args.max_frags}")
        if rows is not None and rows / frags < args.min_rows_per_frag:
            flags.append(f"rows/frag<{args.min_rows_per_frag}")
    if (report["version"] or 0) > args.max_versions:
        flags.append(f"version>{args.max_versions}")
    if (report["worst_unindexed_ratio"] or 0.0) > args.max_unindexed_ratio:
        flags.append(f"unindexed>{args.max_unindexed_ratio:.0%}")
    return flags


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only HASH bucket health check (rows/fragments/version/index coverage)"
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=sorted(TABLE_ROLES),
        help="Supported production or test table to check.",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: configured WT_SDK_DB_URI).",
    )
    parser.add_argument("--max-frags", type=int, default=200)
    parser.add_argument(
        "--min-frags",
        type=int,
        default=10,
        help="Fragment-based rules only apply to buckets with at least this many fragments.",
    )
    parser.add_argument("--min-rows-per-frag", type=int, default=50)
    parser.add_argument("--max-versions", type=int, default=3000)
    parser.add_argument("--max-unindexed-ratio", type=float, default=0.2)
    parser.add_argument(
        "--print-partitions",
        action="store_true",
        help="Print only the flagged bucket ids, space-separated (for automation).",
    )
    args = parser.parse_args()

    tables = TableConfig(db_uri=args.db_uri or default_config.tables.db_uri)
    config = GatewayConfig(s3=default_config.s3, tables=tables)

    client = WTGatewayClient(config)
    try:
        partitions = client._list_existing_partitions_for_table(args.table)
        reports, errors = [], []
        for position, partition in enumerate(partitions, start=1):
            try:
                report = _bucket_report(client, args.table, int(partition))
            except Exception as exc:
                errors.append({"partition": partition, "error": str(exc)})
                continue
            report["flags"] = _flags_for(report, args)
            reports.append(report)
            print(
                f"[{position}/{len(partitions)}] partition={report['partition']} "
                f"rows={report['rows']} frags={report['fragments']} "
                f"rows/frag={report['rows_per_fragment']} version={report['version']} "
                f"unindexed={report['worst_unindexed_ratio']:.1%} "
                f"flags={','.join(report['flags']) or '-'}",
                file=sys.stderr,
            )

        flagged = [report["partition"] for report in reports if report["flags"]]
        summary = {
            "table_name": args.table,
            "buckets_total": len(partitions),
            "buckets_flagged": len(flagged),
            "flagged_partitions": flagged,
            "thresholds": {
                "max_frags": args.max_frags,
                "min_rows_per_frag": args.min_rows_per_frag,
                "max_versions": args.max_versions,
                "max_unindexed_ratio": args.max_unindexed_ratio,
            },
            "errors": errors,
        }
        if args.print_partitions:
            print(" ".join(str(p) for p in flagged))
        else:
            print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        return 1 if errors else 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
