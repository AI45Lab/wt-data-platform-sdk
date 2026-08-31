#!/usr/bin/env python3
"""Create missing indexes and fully optimize a profile-selected env-config table.

The target is ``env_config_test`` for the test profile and
``evaluation_env_config`` for production. Both are unpartitioned tables in the
database selected by ``WT_SDK_ENV_CONFIG_DB_URI``.

Examples:
  python scripts/ops/maintain_env_config_indexes.py --dry-run
  python scripts/ops/maintain_env_config_indexes.py
  python scripts/ops/maintain_env_config_indexes.py --profile production --dry-run
  python scripts/ops/maintain_env_config_indexes.py --no-optimize
"""

import argparse
import json
import sys
from typing import Any

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


def maintain_env_config_indexes(
    session,
    *,
    table_name: str = TEST_ENV_CONFIG_TABLE,
    create_missing: bool = True,
    optimize: bool = True,
    dry_run: bool = False,
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

    configured = [(column, INDEX_TYPE) for column in SCALAR_INDEX_COLUMNS]
    existing_before = sorted(
        {_index_name(index) for index in session.list_indices(table_name)}
    )
    missing = [
        (column, index_type)
        for column, index_type in configured
        if f"{column}_idx" not in existing_before
    ]

    summary: dict[str, Any] = {
        "table_name": table_name,
        "expected_indexes": [f"{column}_idx" for column, _ in configured],
        "existing_indexes_before": existing_before,
        "missing_indexes_before": [f"{column}_idx" for column, _ in missing],
        "indexes_created": [],
        "optimize_requested": optimize,
        "optimized": False,
        "coverage": [],
        "dry_run": dry_run,
        "errors": [],
    }

    if dry_run:
        return summary

    if create_missing:
        for column, index_type in missing:
            try:
                session.create_scalar_index(
                    table_name,
                    column,
                    index_type=index_type,
                )
                summary["indexes_created"].append(
                    {
                        "column": column,
                        "index_name": f"{column}_idx",
                        "index_type": index_type,
                    }
                )
            except Exception as exc:
                summary["errors"].append(
                    {
                        "action": "create_scalar_index",
                        "column": column,
                        "error": str(exc),
                    }
                )

    if optimize:
        try:
            if not callable(getattr(session, "optimize", None)):
                raise RuntimeError(
                    "Installed dldb does not expose session.optimize(...)"
                )
            session.optimize(table_name)
            summary["optimized"] = True
        except Exception as exc:
            summary["errors"].append(
                {
                    "action": "optimize",
                    "error": str(exc),
                }
            )

    try:
        summary["coverage"] = [
            _coverage_row(item)
            for item in session.list_index_coverage(table_name)
        ]
    except Exception as exc:
        summary["errors"].append(
            {
                "action": "list_index_coverage",
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
    args = parser.parse_args()

    if args.no_create_missing and args.no_optimize:
        parser.error("both maintenance actions cannot be disabled")

    db_uri = resolve_env_config_db_uri(args.db_uri)
    table_name = resolve_env_config_table_name(profile=args.profile)
    print(f"Environment-config database: {db_uri}")
    print(f"Table: {table_name}")
    print(f"Table URI: {db_uri}/{table_name}.lance")

    session = dldb.connect(
        db_uri,
        storage_options=S3Config().to_storage_options(),
    )
    try:
        summary = maintain_env_config_indexes(
            session,
            table_name=table_name,
            create_missing=not args.no_create_missing,
            optimize=not args.no_optimize,
            dry_run=args.dry_run,
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
