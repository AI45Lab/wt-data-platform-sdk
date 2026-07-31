#!/usr/bin/env python3
"""
Add missing scalar indexes to all partitions of a partitioned table.

This script:
1. Lists all partitions for the logical table
2. Checks which indexes exist on each partition
3. Creates only the missing indexes

Safe to run periodically - it won't recreate existing indexes.

Usage:
    python scripts/ops/add_missing_indexes.py wind_tunnel_serving
    python scripts/ops/add_missing_indexes.py wind_tunnel_landing
    python scripts/ops/add_missing_indexes.py wind_tunnel_serving --dry-run
"""
import argparse
import sys
from typing import List, Set

import dldb
from wt_sdk.config import default_config
from wt_sdk.core.schemas import SERVING_SCALAR_INDEXES, LANDING_SCALAR_INDEXES


# Index definitions per table
TABLE_INDEXES = {
    "wind_tunnel_landing": LANDING_SCALAR_INDEXES,
    "wind_tunnel_serving": SERVING_SCALAR_INDEXES,
    "landing_test": LANDING_SCALAR_INDEXES,
    "serving_test": SERVING_SCALAR_INDEXES,
}


def _pin_exact_dldb_table(session, table_name: str) -> None:
    """Open the logical table by information_schema metadata, avoiding prefix collisions."""
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


def get_partitions(session, table_name: str) -> List:
    """Get all partition values or hash buckets for a partitioned table."""
    try:
        _pin_exact_dldb_table(session, table_name)
        table = session.tables[table_name]
        if hasattr(table, "list_partitions"):
            return sorted(table.list_partitions())
    except Exception:
        pass

    return []


def _get_partition_column(session, table_name: str) -> str:
    """Read partition column from dldb information_schema, with old fallbacks."""
    try:
        schema_table = getattr(session, "schema_table", None)
        record = schema_table.get(table_name) if schema_table is not None else None
        if record is not None and record.partition_column:
            return record.partition_column
    except Exception:
        pass

    if table_name in TABLE_INDEXES:
        return "job_id"
    return "dataset_type"


def get_existing_indexes(session, table_name: str, partition) -> Set[str]:
    """Get existing indexes for a specific partition."""
    try:
        indexes = session.list_indices(table_name, partition=partition)
        return {
            idx["name"] if isinstance(idx, dict) else idx.name
            for idx in indexes
        }
    except Exception:
        return set()


def add_missing_indexes(table_name: str, db_uri: str = None, dry_run: bool = False) -> int:
    """
    Add missing indexes to all partitions of a table.

    Returns:
        Number of indexes created
    """
    db_uri = db_uri or default_config.tables.db_uri

    print(f"Connecting to {db_uri}...")
    session = dldb.connect(
        db_uri,
        storage_options=default_config.s3.to_storage_options()
    )

    # Check if table exists
    if not session.table_exists(table_name):
        print(f"Error: Table '{table_name}' does not exist")
        session.shutdown()
        return 0

    # Get expected indexes for this table
    expected_indexes = TABLE_INDEXES.get(table_name, [])

    if not expected_indexes:
        print(f"Error: No index definitions found for '{table_name}'")
        print(f"Add index definitions to TABLE_INDEXES in this script.")
        session.shutdown()
        return 0

    print("=" * 70)
    print(f"Adding missing indexes to: {table_name}")
    print("=" * 70)

    if dry_run:
        print("\n[DRY RUN] - No actual changes will be made\n")

    print(f"\nExpected indexes ({len(expected_indexes)}):")
    for col, idx_type in expected_indexes:
        print(f"  - {col} ({idx_type})")

    # Get all partitions
    print(f"\nScanning for partitions...")
    partitions = get_partitions(session, table_name)

    if not partitions:
        print(f"  No partitions found (table is empty)")
        print("  HASH bucket indexes cannot exist before data creates physical buckets.")
        print("  Re-run this command after the first records are written.")
        session.shutdown()
        return 0

    print(f"  Found {len(partitions)} partition(s): {', '.join(map(str, partitions))}\n")

    # Create missing indexes for each partition
    total_created = 0

    for partition in partitions:
        print(f"Partition: {partition}")
        existing = get_existing_indexes(session, table_name, partition)

        for column, index_type in expected_indexes:
            index_name = f"{column}_idx"
            if index_name not in existing:
                print(f"  Creating index: {column} ({index_type})...", end=" ")
                if dry_run:
                    print("[SKIPPED - DRY RUN]")
                else:
                    try:
                        session.create_scalar_index(
                            table_name,
                            column,
                            partition=partition,
                            index_type=index_type
                        )
                        print("[OK]")
                        total_created += 1
                    except Exception as e:
                        print(f"[FAILED: {e}]")
            else:
                print(f"  Index exists: {column}")

    print("-" * 70)
    print(f"Total indexes created: {total_created}")

    session.shutdown()
    return total_created


def main():
    parser = argparse.ArgumentParser(
        description="Add missing scalar indexes to all partitions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show what would be done without making changes
  python scripts/ops/add_missing_indexes.py wind_tunnel_serving --dry-run

  # Add missing indexes to all partitions
  python scripts/ops/add_missing_indexes.py wind_tunnel_serving

  # Add missing indexes to landing table
  python scripts/ops/add_missing_indexes.py wind_tunnel_landing
        """
    )
    parser.add_argument(
        "table_name",
        help="Logical table name (e.g., wind_tunnel_serving, wind_tunnel_landing)"
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: s3://wind-tunnel-dldb)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )

    args = parser.parse_args()

    add_missing_indexes(args.table_name, args.db_uri, args.dry_run)


if __name__ == "__main__":
    main()
