#!/usr/bin/env python3
"""Create missing indexes, fully optimize, or only clean an env-config table.

The target is selected by ``WT_SDK_PROFILE`` (default: test), unless an explicit
``--table`` or ``--profile`` is supplied. ``env_test`` is the test table and
``wt_env_config`` is the production table, in the database selected by
``WT_SDK_ENV_CONFIG_DB_URI``. Since the 2026.10 job-splitting change these
tables are HASH(job_id)-partitioned, so maintenance runs per materialized
bucket; legacy unpartitioned tables keep the single-table path.

Examples:
  python scripts/ops/maintain_env_config_indexes.py --dry-run
  python scripts/ops/maintain_env_config_indexes.py
  python scripts/ops/maintain_env_config_indexes.py --table wt_env_config --dry-run
  python scripts/ops/maintain_env_config_indexes.py --no-optimize
  python scripts/ops/maintain_env_config_indexes.py --cleanup-only
"""

import argparse
import json
import sys
from datetime import timedelta
from typing import Any, List, Optional, Tuple

import dldb

from wt_sdk.config import (
    DEFAULT_ENV_CONFIG_TABLE,
    TEST_ENV_CONFIG_TABLE,
    resolve_env_config_db_uri,
    resolve_env_config_table_name,
    storage_options_for,
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
    cleanup_only: bool = False,
    dry_run: bool = False,
    cleanup_older_than: timedelta | None = None,
) -> dict[str, Any]:
    """Create indexes and optimize, or only remove superseded table versions."""
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

    if cleanup_only:
        summary: dict[str, Any] = {
            "table_name": table_name,
            "partition_type": partition_type,
            "targets": [str(target) for target in targets],
            "cleanup_only": True,
            "cleaned_up": [],
            "optimize_requested": False,
            "optimized": [],
            "coverage": [],
            "dry_run": dry_run,
            "errors": [],
        }
        if dry_run:
            return summary

        for target in targets:
            label = "whole table" if target is None else f"bucket {target}"
            try:
                print(f"Cleaning old versions on {label}...", flush=True)
                if not callable(getattr(session, "cleanup_old_versions", None)):
                    raise RuntimeError(
                        "Installed dldb does not expose session.cleanup_old_versions(...)"
                    )
                session.cleanup_old_versions(
                    table_name,
                    partition=target,
                    cleanup_older_than=cleanup_older_than,
                    delete_unverified=False,
                )
                summary["cleaned_up"].append(target)
            except Exception as exc:
                summary["errors"].append(
                    {
                        "action": "cleanup_old_versions",
                        "partition": target,
                        "error": str(exc),
                    }
                )
        return summary

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
        "cleanup_only": False,
        "cleaned_up": [],
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
            "fragments, refresh indexes, and clean old versions, or only "
            "clean old versions"
        )
    )
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--table",
        choices=(TEST_ENV_CONFIG_TABLE, DEFAULT_ENV_CONFIG_TABLE),
        default=None,
        help="Explicit table name; overrides WT_SDK_PROFILE.",
    )
    target.add_argument(
        "--profile",
        choices=("test", "prod", "production"),
        default=None,
        help=(
            "Select env_test for test or wt_env_config for production. "
            "Defaults to WT_SDK_PROFILE, then test."
        ),
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: WT_SDK_ENV_CONFIG_DB_URI, then the GPFS env-config path).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Preview indexes, or only report cleanup targets with "
            "--cleanup-only, without changing the table."
        ),
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
        "--cleanup-only",
        action="store_true",
        help=(
            "Only delete old versions in materialized buckets (or the legacy "
            "whole table); skip index inspection/creation and full optimize."
        ),
    )
    parser.add_argument(
        "--cleanup-older-than-hours",
        type=float,
        default=24.0,
        metavar="HOURS",
        help=(
            "Lance old-version retention window passed to cleanup_old_versions "
            "directly with --cleanup-only, or through optimize() otherwise. "
            "0 deletes every superseded version immediately. dldb's own "
            "168h/7d fallback is never used through this CLI."
        ),
    )
    parser.add_argument(
        "--dldb-model",
        choices=["none", "metrics", "debug"],
        default="debug",
        help=(
            "dldb instrumentation model for this run. 'debug' emits dldb_debug "
            "started/heartbeat/completed progress logs for create_scalar_index / "
            "optimize / cleanup_old_versions (requires dldb>=1.1.4; "
            "cleanup-only requires dldb>=1.1.11). Defaults to 'debug' so manual "
            "runs always show per-step progress; use 'none' to silence."
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

    if args.cleanup_only:
        conflicts = [
            flag
            for flag, enabled in (
                ("--no-create-missing", args.no_create_missing),
                ("--no-optimize", args.no_optimize),
            )
            if enabled
        ]
        if conflicts:
            parser.error(
                "--cleanup-only cannot be combined with " + ", ".join(conflicts)
            )
    if args.no_create_missing and args.no_optimize:
        parser.error("both maintenance actions cannot be disabled")

    db_uri = resolve_env_config_db_uri(args.db_uri)
    table_name = resolve_env_config_table_name(args.table, profile=args.profile)
    print(f"Environment-config database: {db_uri}")
    print(f"Table: {table_name}")
    print(f"Table URI: {db_uri}/{table_name}.lance")

    connect_kwargs: dict[str, Any] = {}
    storage_options = storage_options_for(db_uri)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
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
            cleanup_only=args.cleanup_only,
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
