#!/usr/bin/env python3
"""Drop and recreate the active production landing table after archive copy.

This script implements the destructive half of the periodic landing archive
workflow. It refuses to drop ``wind_tunnel_landing`` unless the dated archive
table exists and matches the current source table by schema, HASH partition
metadata, and total row count.

Stop writers and ETL before running this script.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from typing import Any, Dict

import dldb

from wt_sdk.config import DEFAULT_LANDING_TABLE, default_config
from wt_sdk.core.schemas import (
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_PARTITIONS,
    LANDING_SCHEMA,
)


def _default_archive_table(today: str | None = None) -> str:
    date_str = today or datetime.now().strftime("%Y%m%d")
    return f"archived_{date_str}_{DEFAULT_LANDING_TABLE}"


def _metadata(record) -> Dict[str, Any]:
    return {
        "partition_column": record.partition_column,
        "partition_type": record.partition_type,
        "partitions": record.partitions,
    }


def _expected_landing_metadata() -> Dict[str, Any]:
    return {
        "partition_column": LANDING_PARTITION_COLUMN,
        "partition_type": LANDING_PARTITION_TYPE,
        "partitions": LANDING_PARTITIONS,
    }


def _verify_archive_matches_source(
    session,
    *,
    source_table: str,
    archive_table: str,
) -> int:
    for table_name in (source_table, archive_table):
        if not session.table_exists(table_name):
            raise ValueError(f"Required table '{table_name}' does not exist")

    source_record = session.schema_table.get(source_table)
    archive_record = session.schema_table.get(archive_table)
    source_schema = session.get_schema(source_table)
    archive_schema = session.get_schema(archive_table)
    expected_metadata = _expected_landing_metadata()

    if _metadata(source_record) != expected_metadata:
        raise ValueError(
            f"Source table metadata is not the expected active landing layout: "
            f"expected={expected_metadata}, actual={_metadata(source_record)}"
        )
    if _metadata(archive_record) != expected_metadata:
        raise ValueError(
            f"Archive table metadata is not the expected active landing layout: "
            f"expected={expected_metadata}, actual={_metadata(archive_record)}"
        )
    if source_schema != LANDING_SCHEMA:
        raise ValueError("Source table schema differs from current LANDING_SCHEMA")
    if archive_schema != LANDING_SCHEMA:
        raise ValueError("Archive table schema differs from current LANDING_SCHEMA")

    source_count = session.count_rows(source_table)
    archive_count = session.count_rows(archive_table)
    if source_count != archive_count:
        raise ValueError(
            "Archive row count does not match source: "
            f"source={source_count}, archive={archive_count}"
        )
    return source_count


def drop_and_recreate_landing(
    *,
    source_table: str,
    archive_table: str,
    dry_run: bool,
    confirm_recreate: bool,
) -> None:
    if source_table != DEFAULT_LANDING_TABLE:
        raise ValueError(
            f"This production rebuild script only supports {DEFAULT_LANDING_TABLE!r}; "
            f"got {source_table!r}"
        )
    if source_table == archive_table:
        raise ValueError("Source table and archive table must be different")
    if archive_table.startswith(source_table):
        raise ValueError(
            "Archive table name must not start with the source table name; "
            "use archived_YYYYMMDD_wind_tunnel_landing to avoid dldb prefix confusion."
        )

    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )
    try:
        archived_rows = _verify_archive_matches_source(
            session,
            source_table=source_table,
            archive_table=archive_table,
        )

        print("=" * 80)
        print("Production landing drop/recreate preflight passed")
        print("=" * 80)
        print(f"Database:           {default_config.tables.db_uri}")
        print(f"Archive table:      {archive_table} ({archived_rows} rows)")
        print(f"Active table:       {source_table} ({archived_rows} rows, will be replaced)")
        print(f"New partitioning:   {LANDING_PARTITION_TYPE}({LANDING_PARTITION_COLUMN})")
        print(f"Hash buckets:       {LANDING_PARTITIONS}")
        print(f"New schema fields:  {len(LANDING_SCHEMA)}")
        print("Serving table:      not touched")
        print("=" * 80)

        if dry_run:
            print("[DRY RUN] No table was dropped or created.")
            return
        if not confirm_recreate:
            raise ValueError("Refusing destructive rebuild without --confirm-recreate")

        archived_rows_before_drop = _verify_archive_matches_source(
            session,
            source_table=source_table,
            archive_table=archive_table,
        )
        if archived_rows_before_drop != archived_rows:
            raise RuntimeError(
                "Source/archive counts changed after preflight; refusing to drop the source table"
            )

        print(f"Dropping active production table '{source_table}'...")
        session.drop_table(source_table)

        print(
            f"Creating empty '{source_table}' as "
            f"{LANDING_PARTITION_TYPE}({LANDING_PARTITION_COLUMN})..."
        )
        session.create_table(
            source_table,
            LANDING_SCHEMA,
            partition_column=LANDING_PARTITION_COLUMN,
            partition_type=LANDING_PARTITION_TYPE,
            partitions=LANDING_PARTITIONS,
        )
        new_record = session.schema_table.get(source_table)
        if _metadata(new_record) != _expected_landing_metadata():
            raise RuntimeError(
                f"New table metadata verification failed: got {_metadata(new_record)}"
            )
        if session.get_schema(source_table) != LANDING_SCHEMA:
            raise RuntimeError("New table schema verification failed")
        new_count = session.count_rows(source_table)
        if new_count != 0:
            raise RuntimeError(f"New table should be empty, found {new_count} rows")

        archive_count_after = session.count_rows(archive_table)
        if archive_count_after != archived_rows:
            raise RuntimeError(
                f"Archive row count changed unexpectedly: before={archived_rows}, after={archive_count_after}"
            )

        print("=" * 80)
        print("Production landing drop/recreate complete")
        print(f"{source_table}: empty {LANDING_PARTITION_TYPE}({LANDING_PARTITION_COLUMN}) table")
        print(f"{archive_table}: retained {archive_count_after} archived rows")
        print("No scalar indexes exist until data creates HASH buckets; run index maintenance after writes.")
        print("=" * 80)
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Drop and recreate wind_tunnel_landing after verifying its archive table.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--source-table", default=DEFAULT_LANDING_TABLE)
    parser.add_argument("--archive-table", default=_default_archive_table())
    parser.add_argument("--dry-run", action="store_true", help="Validate archive and print the plan only.")
    parser.add_argument(
        "--confirm-recreate",
        action="store_true",
        help="Required to drop the current production landing table and create the new empty table.",
    )
    args = parser.parse_args()

    if args.dry_run and args.confirm_recreate:
        print("--dry-run ignores --confirm-recreate", file=sys.stderr)
    try:
        drop_and_recreate_landing(
            source_table=args.source_table,
            archive_table=args.archive_table,
            dry_run=args.dry_run,
            confirm_recreate=args.confirm_recreate,
        )
    except Exception as exc:
        print(f"Drop/recreate failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
