#!/usr/bin/env python3
"""Create or verify the two production trajectory-statistics tables.

This command never drops tables, rewrites rows, or computes statistics.
"""

import argparse
from typing import Optional

import dldb

from wt_sdk.config import (
    DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE,
    DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE,
    TableConfig,
    storage_options_for,
)
from wt_sdk.trajectory_stats.schemas import (
    CATEGORY_TRAJECTORY_STATS_SCHEMA,
    SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN,
    SESSION_TRAJECTORY_METRICS_PARTITION_TYPE,
    SESSION_TRAJECTORY_METRICS_PARTITIONS,
    SESSION_TRAJECTORY_METRICS_SCHEMA,
)


def _verify_table(
    session,
    table_name: str,
    expected_schema,
    *,
    partition_column: Optional[str] = None,
    partition_type: Optional[str] = None,
    partitions: Optional[int] = None,
) -> None:
    actual_schema = session.get_schema(table_name)
    if actual_schema != expected_schema:
        raise ValueError(f"Table {table_name!r} schema does not match the SDK schema")

    record = session.schema_table.get(table_name)
    if record is None:
        raise ValueError(f"Table {table_name!r} has no dldb schema metadata")

    actual_column = getattr(record, "partition_column", None) or None
    actual_type = str(getattr(record, "partition_type", None) or "").upper() or None
    expected_type = partition_type.upper() if partition_type else None
    if actual_column != partition_column or actual_type != expected_type:
        raise ValueError(
            f"Table {table_name!r} partitioning does not match: "
            f"expected {expected_type}({partition_column}), "
            f"found {actual_type}({actual_column})"
        )
    if partitions is not None and getattr(record, "partitions", None) != partitions:
        raise ValueError(
            f"Table {table_name!r} has {getattr(record, 'partitions', None)} buckets; "
            f"expected {partitions}"
        )


def _initialize_table(
    session,
    table_name: str,
    schema,
    **partition_kwargs,
) -> bool:
    """Create one missing table and verify it; return whether it was created."""
    created = False
    if not session.table_exists(table_name):
        session.create_table(table_name, schema, **partition_kwargs)
        created = True
    _verify_table(session, table_name, schema, **partition_kwargs)
    return created


def init_trajectory_stats_tables(
    *,
    confirm_create: bool = False,
    confirm_production: bool = False,
    dry_run: bool = False,
) -> int:
    """Create or verify both production tables without modifying table rows."""
    tables = TableConfig(profile="production")
    db_uri = tables.serving_db_uri

    print(f"Production serving database: {db_uri}")
    print(f"Session metrics table: {DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE}")
    print(f"Category stats table: {DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE}")
    print(
        "Session partitioning: "
        f"{SESSION_TRAJECTORY_METRICS_PARTITION_TYPE}"
        f"({SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN}) "
        f"x {SESSION_TRAJECTORY_METRICS_PARTITIONS}"
    )
    print("Category partitioning: none")

    if dry_run:
        print("Dry run: no connection, table, or data was changed.")
        return 0
    if not confirm_create or not confirm_production:
        print(
            "Refusing to create production tables without both "
            "--confirm-create and --confirm-production. Use --dry-run to inspect first."
        )
        return 2

    connect_kwargs = {}
    storage_options = storage_options_for(db_uri)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
    session = dldb.connect(db_uri, **connect_kwargs)

    created = _initialize_table(
        session,
        DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE,
        SESSION_TRAJECTORY_METRICS_SCHEMA,
        partition_column=SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN,
        partition_type=SESSION_TRAJECTORY_METRICS_PARTITION_TYPE,
        partitions=SESSION_TRAJECTORY_METRICS_PARTITIONS,
    )
    print(
        f"{'Created' if created else 'Verified existing'} table: "
        f"{DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE}"
    )

    created = _initialize_table(
        session,
        DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE,
        CATEGORY_TRAJECTORY_STATS_SCHEMA,
    )
    print(
        f"{'Created' if created else 'Verified existing'} table: "
        f"{DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE}"
    )
    print("Initialization complete; no statistics were computed or written.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create or verify the production trajectory-statistics tables."
    )
    parser.add_argument(
        "--confirm-create",
        action="store_true",
        help="Allow creation of missing tables; existing tables are never replaced.",
    )
    parser.add_argument(
        "--confirm-production",
        action="store_true",
        help="Confirm that the target is the production serving database.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the target and partitioning without connecting.",
    )
    args = parser.parse_args()
    return init_trajectory_stats_tables(
        confirm_create=args.confirm_create,
        confirm_production=args.confirm_production,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
