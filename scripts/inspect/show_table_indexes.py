#!/usr/bin/env python3
"""
Show index status for a partitioned table.

Displays:
- All partitions for the logical table
- Which indexes exist on each partition
- Which indexes are missing

Usage:
    python scripts/inspect/show_table_indexes.py wind_tunnel_serving
    python scripts/inspect/show_table_indexes.py wind_tunnel_landing
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


def get_partitions(session, table_name: str) -> List:
    """Get all partition values or hash buckets for a partitioned table."""
    try:
        table = session._get_table(table_name)
        if hasattr(table, "list_partitions"):
            return sorted(table.list_partitions())
    except Exception:
        pass

    return []


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


def show_index_status(table_name: str, db_uri: str = None) -> None:
    """Show index status for all partitions of a table."""
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
        sys.exit(1)

    # Get expected indexes for this table
    expected_indexes = TABLE_INDEXES.get(table_name, [])
    expected_index_names = {f"{col}_idx" for col, _ in expected_indexes}

    print("=" * 70)
    print(f"Index Status for: {table_name}")
    print("=" * 70)

    if not expected_indexes:
        print(f"\nNo index definitions found for '{table_name}'")
        print(f"Add index definitions to TABLE_INDEXES in this script.")
        session.shutdown()
        return

    print(f"\nExpected indexes ({len(expected_indexes)}):")
    for col, idx_type in expected_indexes:
        print(f"  - {col} ({idx_type})")

    # Get all partitions
    print(f"\nScanning for partitions...")
    partitions = get_partitions(session, table_name)

    if not partitions:
        print(f"  No partitions found (table is empty)")
        session.shutdown()
        return

    print(f"  Found {len(partitions)} partition(s): {', '.join(map(str, partitions))}")

    # Check indexes for each partition
    print(f"\n{'Partition':<20} {'Existing Indexes':<40} {'Missing':<20}")
    print("-" * 70)

    total_existing = 0
    total_missing = 0

    for partition in partitions:
        existing = get_existing_indexes(session, table_name, partition)
        missing = expected_index_names - existing

        existing_str = ", ".join(sorted(existing)) if existing else "-"
        missing_str = ", ".join(sorted(missing)) if missing else "-"

        print(f"{partition:<20} {existing_str:<40} {missing_str:<20}")

        total_existing += len(existing)
        total_missing += len(missing)

    print("-" * 70)
    print(f"Total: {total_existing} indexes exist, {total_missing} missing")

    session.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Show index status for a partitioned table"
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

    args = parser.parse_args()

    show_index_status(args.table_name, args.db_uri)


if __name__ == "__main__":
    main()
