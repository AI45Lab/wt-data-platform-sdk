#!/usr/bin/env python3
"""Rebuild only the production landing table as HASH(job_id).

This is intentionally a production-only, destructive operation.  Before it
drops ``wind_tunnel_landing``, it verifies that the old VALUE(dt) table has a
complete, schema-identical archive in ``wind_tunnel_landing_legacy``.  The
historical data remains in that archive and is not copied into the new table.

The script never touches ``wind_tunnel_serving``.

Usage:
    # Read-only preflight. Required before the destructive command.
    AWS_EC2_METADATA_DISABLED=true python scripts/migrations/production_landing_2026_07/rebuild_wind_tunnel_landing.py --dry-run

    # Drop the legacy production landing table and create an empty HASH(job_id)
    # table. Ensure all writers are stopped first.
    AWS_EC2_METADATA_DISABLED=true python scripts/migrations/production_landing_2026_07/rebuild_wind_tunnel_landing.py --confirm-rebuild
"""

import argparse
import sys
from typing import Any, Dict

import dldb

from wt_sdk.config import default_config
from wt_sdk.core.schemas import (
    LANDING_PARTITIONS,
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_SCHEMA,
    LANDING_SCALAR_INDEXES,
)


SOURCE_TABLE = "wind_tunnel_landing"
ARCHIVE_TABLE = "wind_tunnel_landing_legacy"
LEGACY_PARTITION_COLUMN = "dt"
LEGACY_PARTITION_TYPE = "VALUE"


def _pin_exact_dldb_table(session, table_name: str) -> None:
    """Avoid dldb's broad prefix lookup for source/archive table names."""
    from dldb.table import open_table_by_partition_type

    record = session.schema_table.get(table_name)
    if record is None:
        raise ValueError(f"Table '{table_name}' does not exist in dldb information_schema")
    session.tables[table_name] = open_table_by_partition_type(
        session.db_conn,
        session.schema_table,
        table_name,
        record.partition_type,
    )


def _metadata(record) -> Dict[str, Any]:
    return {
        "partition_column": record.partition_column,
        "partition_type": record.partition_type,
        "partitions": record.partitions,
    }


def _verify_archive(session) -> int:
    """Validate that the complete old production table is safely archived."""
    for table_name in (SOURCE_TABLE, ARCHIVE_TABLE):
        if not session.table_exists(table_name):
            raise ValueError(f"Required table '{table_name}' does not exist")
        _pin_exact_dldb_table(session, table_name)

    source_record = session.schema_table.get(SOURCE_TABLE)
    archive_record = session.schema_table.get(ARCHIVE_TABLE)
    source_schema = session.get_schema(SOURCE_TABLE)
    archive_schema = session.get_schema(ARCHIVE_TABLE)

    expected_legacy_metadata = {
        "partition_column": LEGACY_PARTITION_COLUMN,
        "partition_type": LEGACY_PARTITION_TYPE,
        "partitions": -1,
    }
    if _metadata(source_record) != expected_legacy_metadata:
        raise ValueError(
            "Source table is not the expected archived legacy layout: "
            f"got {_metadata(source_record)}"
        )
    if _metadata(archive_record) != expected_legacy_metadata:
        raise ValueError(
            "Archive table is not the expected legacy layout: "
            f"got {_metadata(archive_record)}"
        )
    if source_schema != archive_schema:
        raise ValueError("Archive schema differs from the source schema")

    source_count = session.count_rows(SOURCE_TABLE)
    archive_count = session.count_rows(ARCHIVE_TABLE)
    if source_count <= 0:
        raise ValueError("Refusing to rebuild: source table is empty")
    if source_count != archive_count:
        raise ValueError(
            "Archive row count does not match source: "
            f"source={source_count}, archive={archive_count}"
        )
    return source_count


def rebuild_wind_tunnel_landing(*, dry_run: bool, confirm_rebuild: bool) -> None:
    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )
    try:
        archived_rows = _verify_archive(session)
        print("=" * 72)
        print("Production landing rebuild preflight passed")
        print("=" * 72)
        print(f"Legacy archive:     {ARCHIVE_TABLE} ({archived_rows} rows)")
        print(f"Source to replace:  {SOURCE_TABLE}")
        print(f"New partitioning:   {LANDING_PARTITION_TYPE}({LANDING_PARTITION_COLUMN})")
        print(f"Hash buckets:       {LANDING_PARTITIONS}")
        print(f"New schema fields:  {len(LANDING_SCHEMA)}")
        print("Historical rows:    remain only in wind_tunnel_landing_legacy")
        print("Serving table:      not touched")
        print("=" * 72)

        if dry_run:
            print("[DRY RUN] No table was dropped or created.")
            return
        if not confirm_rebuild:
            raise ValueError("Refusing destructive rebuild without --confirm-rebuild")

        # Recheck immediately before dropping in case another process changed the source.
        archived_rows_before_drop = _verify_archive(session)
        if archived_rows_before_drop != archived_rows:
            raise RuntimeError(
                "Source/archive counts changed after preflight; refusing to drop the source table"
            )

        print(f"Dropping legacy production table '{SOURCE_TABLE}'...")
        _pin_exact_dldb_table(session, SOURCE_TABLE)
        session.drop_table(SOURCE_TABLE)

        print(
            f"Creating empty '{SOURCE_TABLE}' as "
            f"{LANDING_PARTITION_TYPE}({LANDING_PARTITION_COLUMN})..."
        )
        session.create_table(
            SOURCE_TABLE,
            LANDING_SCHEMA,
            partition_column=LANDING_PARTITION_COLUMN,
            partition_type=LANDING_PARTITION_TYPE,
            partitions=LANDING_PARTITIONS,
        )
        _pin_exact_dldb_table(session, SOURCE_TABLE)
        new_record = session.schema_table.get(SOURCE_TABLE)
        expected_new_metadata = {
            "partition_column": LANDING_PARTITION_COLUMN,
            "partition_type": LANDING_PARTITION_TYPE,
            "partitions": LANDING_PARTITIONS,
        }
        if _metadata(new_record) != expected_new_metadata:
            raise RuntimeError(
                f"New table metadata verification failed: got {_metadata(new_record)}"
            )
        if session.get_schema(SOURCE_TABLE) != LANDING_SCHEMA:
            raise RuntimeError("New table schema verification failed")
        new_count = session.count_rows(SOURCE_TABLE)
        if new_count != 0:
            raise RuntimeError(f"New table should be empty, found {new_count} rows")

        print("=" * 72)
        print("Production landing rebuild complete")
        print(f"{SOURCE_TABLE}: empty {LANDING_PARTITION_TYPE}({LANDING_PARTITION_COLUMN}) table")
        print(f"{ARCHIVE_TABLE}: retained {archived_rows} historical rows")
        print("No scalar indexes exist until data creates HASH buckets; run index maintenance after writes.")
        print("=" * 72)
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild only wind_tunnel_landing after verifying its legacy archive.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate archive and print the plan only.")
    parser.add_argument(
        "--confirm-rebuild",
        action="store_true",
        help="Required to drop the old production landing table and create the new empty table.",
    )
    args = parser.parse_args()

    if args.dry_run and args.confirm_rebuild:
        print("--dry-run ignores --confirm-rebuild", file=sys.stderr)
    try:
        rebuild_wind_tunnel_landing(
            dry_run=args.dry_run,
            confirm_rebuild=args.confirm_rebuild,
        )
    except Exception as exc:
        print(f"Rebuild failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
