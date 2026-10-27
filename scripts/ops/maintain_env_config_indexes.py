#!/usr/bin/env python3
"""Create missing indexes and fully optimize a profile-selected env-config table.

The target is ``env_config_test`` for the test profile and
``evaluation_env_config`` for production, in the database selected by
``WT_SDK_ENV_CONFIG_DB_URI``. Since the 2026.10 job-splitting change these
tables are HASH(job_id)-partitioned, so indexes are created and optimized
per materialized bucket; legacy unpartitioned tables keep the single-table
code path.

Examples:
  python scripts/ops/maintain_env_config_indexes.py --dry-run
  python scripts/ops/maintain_env_config_indexes.py
  python scripts/ops/maintain_env_config_indexes.py --profile production --dry-run
  python scripts/ops/maintain_env_config_indexes.py --no-optimize
"""

import argparse
import json
import sys
from datetime import timedelta
from typing import Any, List, Optional, Tuple

import dldb

from wt_sdk.config import (
    S3Config,
    TEST_ENV_CONFIG_TABLE,
    resolve_env_config_db_uri,
    resolve_env_config_table_name,
)
from wt_sdk.core.evaluation_env_schema import (
    EVALUATION_ENV_SCHEMA,
    SCALAR_INDEX_COLUMNS,
)


INDEX_TYPE = "BTREE"


def _index_name(index: Any) -> str:
    if isinstance(index, dict):
        return str(index["name"])
    return str(index.name)


def _coverage_row(item: Any) -> dict[str, Any]:
    def read(name: str):
        if isinstance(item, dict):
            return item.get(name)
        return getattr(item, name, None)

    return {
        "index_name": read("index_name"),
        "num_indexed_rows": read("num_indexed_rows"),
        "num_unindexed_rows": read("num_unindexed_rows"),
        "fully_indexed": read("fully_indexed"),
    }


def _maintenance_targets(session, table_name: str) -> Tuple[Optional[str], List[Optional[int]]]:
    """Return (partition_type, targets) for index maintenance.

    targets is [None] for unpartitioned tables (whole-table operations) or
    the list of materialized HASH bucket ids for partitioned tables.
    """
    try:
        record = session.schema_table.get(table_name)
    except Exception:
        record = None
    partition_type = str(getattr(record, "partition_type", None) or "").upper()
    if record is None or partition_type != "HASH":
        return (partition_type or None), [None]
    table = session._get_table(table_name, None)
    if not hasattr(table, "list_partitions"):
        return partition_type, [None]
    return partition_type, sorted(table.list_partitions())


def maintain_env_config_indexes(
    session,
    *,
    table_name: str = TEST_ENV_CONFIG_TABLE,
    create_missing: bool = True,
    optimize: bool = True,
    dry_run: bool = False,
    cleanup_older_than: timedelta | None = None,
) -> dict[str, Any]:
    """Create missing indexes, compact fragments, and refresh index tails."""
    actual_schema = session.get_schema(table_name)
    expected_fields = set(EVALUATION_ENV_SCHEMA.names)
    actual_fields = set(actual_schema.names)
    missing_schema_fields = sorted(expected_fields - actual_fields)
    if missing_schema_fields:
        raise ValueError(
            f"Table {table_name!r} is missing schema fields required by the SDK: "
            + ", ".join(missing_schema_fields)
        )

    partition_type, targets = _maintenance_targets(session, table_name)
    configured = [(column, INDEX_TYPE) for column in SCALAR_INDEX_COLUMNS]

    per_target_existing = {}
    for target in targets:
        per_target_existing[target] = {
            _index_name(index)
            for index in session.list_indices(table_name, partition=target)
        }
    existing_before = sorted(
        {name for existing in per_target_existing.values() for name in existing}
    )
    per_target_missing = {
        target: [
            (column, index_type)
            for column, index_type in configured
            if f"{column}_idx" not in existing
        ]
        for target, existing in per_target_existing.items()
    }

    summary: dict[str, Any] = {
        "table_name": table_name,
        "partition_type": partition_type,
        "targets": [str(target) for target in targets],
        "expected_indexes": [f"{column}_idx" for column, _ in configured],
        "existing_indexes_before": existing_before,
        "missing_indexes_before": {
            str(target): [f"{column}_idx" for column, _ in missing]
            for target, missing in per_target_missing.items()
        },
        "indexes_created": [],
        "optimize_requested": optimize,
        "optimized": [],
        "coverage": [],
        "dry_run": dry_run,
        "errors": [],
    }

    if dry_run:
        return summary

    for target in targets:
        label = "whole table" if target is None else f"bucket {target}"
        missing = per_target_missing[target]

        if create_missing and missing:
            print(
                f"Creating {len(missing)} missing indexes on {label}: "
                f"{[f'{c}_idx' for c, _ in missing]}",
                flush=True,
            )
            for column, index_type in missing:
                try:
                    session.create_scalar_index(
                        table_name,
                        column,
                        partition=target,
                        index_type=index_type,
                    )
                    summary["indexes_created"].append(
                        {
                            "partition": target,
                            "column": column,
                            "index_name": f"{column}_idx",
                            "index_type": index_type,
                        }
                    )
                except Exception as exc:
                    summary["errors"].append(
                        {
                            "action": "create_scalar_index",
                            "partition": target,
                            "column": column,
                            "error": str(exc),
                        }
                    )

        if optimize:
            try:
                print(
                    f"Optimizing {label} (compact_files + optimize_indices + cleanup_old_versions)...",
                    flush=True,
                )
                if not callable(getattr(session, "optimize", None)):
                    raise RuntimeError(
                        "Installed dldb does not expose session.optimize(...)"
                    )
                session.optimize(
                    table_name,
                    partition=target,
                    cleanup_older_than=cleanup_older_than,
                )
                summary["optimized"].append(target)
            except Exception as exc:
                summary["errors"].append(
                    {
                        "action": "optimize",
                        "partition": target,
                        "error": str(exc),
                    }
                )

        try:
            summary["coverage"].extend(
                {
                    "partition": target,
                    **_coverage_row(item),
                }
                for item in session.list_index_coverage(
                    table_name, partition=target
                )
            )
        except Exception as exc:
            summary["errors"].append(
                {
                    "action": "list_index_coverage",
                    "partition": target,
                    "error": str(exc),
                }
            )

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create missing profile-selected env-config indexes, compact "
            "fragments, refresh indexes, and clean old versions"
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
        "--dry-run",
        action="store_true",
        help="Inspect existing/missing indexes without changing the table.",
    )
    parser.add_argument(
        "--no-create-missing",
        action="store_true",
        help="Optimize the table without creating missing indexes.",
    )
    parser.add_argument(
        "--no-optimize",
        action="store_true",
        help="Create missing indexes without full table optimize.",
    )
    parser.add_argument(
        "--cleanup-older-than-hours",
        type=float,
        default=24.0,
        metavar="HOURS",
        help=(
            "Lance old-version retention window passed to optimize() "
            "cleanup_old_versions. 0 deletes every superseded version "
            "immediately. dldb's own 168h/7d fallback is never used "
            "through this CLI."
        ),
    )
    parser.add_argument(
        "--dldb-model",
        choices=["none", "metrics", "debug"],
        default="debug",
        help=(
            "dldb instrumentation model for this run. 'debug' emits dldb_debug "
            "started/heartbeat/completed progress logs for create_scalar_index / "
            "optimize (requires dldb>=1.1.4). Defaults to 'debug' so manual runs "
            "always show per-step progress; use 'none' to silence."
        ),
    )
    parser.add_argument(
        "--dldb-heartbeat-interval-s",
        type=float,
        default=None,
        metavar="SECONDS",
        help=(
            "dldb debug heartbeat interval in seconds. 0 disables heartbeats "
            "(started/completed logs are still emitted). Only used with "
            "--dldb-model debug; default is dldb's 15s."
        ),
    )
    args = parser.parse_args()

    if args.no_create_missing and args.no_optimize:
        parser.error("both maintenance actions cannot be disabled")

    db_uri = resolve_env_config_db_uri(args.db_uri)
    table_name = resolve_env_config_table_name(profile=args.profile)
    print(f"Environment-config database: {db_uri}")
    print(f"Table: {table_name}")
    print(f"Table URI: {db_uri}/{table_name}.lance")

    connect_kwargs: dict[str, Any] = {
        "storage_options": S3Config().to_storage_options(),
    }
    if args.dldb_model != "none":
        connect_kwargs["model"] = args.dldb_model
        if args.dldb_heartbeat_interval_s is not None:
            connect_kwargs["heartbeat_interval_s"] = args.dldb_heartbeat_interval_s
    session = dldb.connect(db_uri, **connect_kwargs)
    try:
        summary = maintain_env_config_indexes(
            session,
            table_name=table_name,
            create_missing=not args.no_create_missing,
            optimize=not args.no_optimize,
            dry_run=args.dry_run,
            cleanup_older_than=timedelta(hours=args.cleanup_older_than_hours),
        )
        print(
            json.dumps(
                summary,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
        return 1 if summary["errors"] else 0
    finally:
        session.shutdown()


if __name__ == "__main__":
    sys.exit(main())
