#!/usr/bin/env python3
"""
Add new columns to wind_tunnel_landing table using DLDB.

This script adds the following new columns to the production table:
- env_id: Environment ID (nullable string)
- job_id: Job ID (nullable string)
- is_truncated: Whether the response was truncated (nullable bool)
- is_trainable: Whether the row can be used as training data (nullable bool)

This is a schema evolution operation - it adds new columns without affecting existing data.
Existing records will have NULL values for these new columns.

DLDB is used instead of native LanceDB because:
1. DLDB handles the mapping from logical table to physical partition tables
2. For VALUE partitioned tables, new columns will be added to ALL partitions

Usage:
    # Add new columns to production table
    python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py

    # Dry run to see what would be done
    python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py --dry-run

    # Add to test table instead
    python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py --table landing_test

    # Add to a custom table
    python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py --table my_custom_table
"""
import argparse
import sys
from typing import Dict, Any

import dldb
import pyarrow as pa

from wt_sdk.config import default_config


# New columns to add
NEW_COLUMNS = [
    ("env_id", pa.string()),      # Environment ID
    ("job_id", pa.string()),      # Job ID
    ("is_truncated", pa.bool_()), # Whether response was truncated
    ("is_trainable", pa.bool_()), # Whether row is trainable
]


def _pin_exact_dldb_table(session, table_name: str) -> None:
    """Open the exact logical table by dldb metadata, avoiding prefix collisions."""
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


def add_columns_to_table(
    db_uri: str,
    table_name: str,
    storage_options: Dict[str, Any],
    dry_run: bool = False
) -> bool:
    """
    Add new columns to the specified table using DLDB.

    DLDB handles the mapping from logical table to physical partition tables,
    ensuring new columns are added to all partitions.

    Args:
        db_uri: Database URI (e.g., s3://wind-tunnel-dldb)
        table_name: Logical table name
        storage_options: S3 storage options
        dry_run: If True, only print what would be done

    Returns:
        True if successful
    """
    print(f"\n{'=' * 60}")
    print(f"Adding new columns to table: {table_name}")
    print(f"Database: {db_uri}")
    print(f"{'=' * 60}")

    if dry_run:
        print("\n[DRY RUN] - No changes will be made")

    # Connect to DLDB
    print(f"\nConnecting to {db_uri} using DLDB...")
    session = dldb.connect(db_uri, storage_options=storage_options)

    # Check if table exists
    if not session.table_exists(table_name):
        print(f"✗ Table '{table_name}' does not exist")
        session.shutdown()
        return False

    print(f"✓ Table '{table_name}' exists")
    _pin_exact_dldb_table(session, table_name)

    # Get current schema
    schema = session.get_schema(table_name)
    print(f"\nCurrent schema ({len(schema)} fields):")
    for field in schema:
        print(f"  - {field.name}: {field.type}")

    # Check which columns already exist
    existing_cols = {field.name for field in schema}
    cols_to_add = []

    print(f"\nNew columns to add:")
    for col_name, col_type in NEW_COLUMNS:
        if col_name in existing_cols:
            print(f"  - {col_name} ({col_type}): Already exists, skipping")
        else:
            print(f"  - {col_name} ({col_type}): Will be added")
            cols_to_add.append((col_name, col_type))

    if not cols_to_add:
        print("\n✓ All new columns already exist, nothing to do")
        session.shutdown()
        return True

    if dry_run:
        print("\n[DRY RUN] Would add the columns listed above")
        session.shutdown()
        return True

    # Add columns using DLDB
    # DLDB's add_columns will handle all partitions automatically
    try:
        print(f"\nAdding {len(cols_to_add)} new columns using DLDB...")

        # Create list of pyarrow fields for the new columns
        new_fields = [
            pa.field(col_name, col_type, nullable=True)
            for col_name, col_type in cols_to_add
        ]

        # Use DLDB's add_columns method
        # This will add the columns to all partitions of the logical table
        session.add_columns(table_name, new_fields)
        print(f"  ✓ Added {len(cols_to_add)} columns via DLDB")

        print(f"\n✓ Successfully added {len(cols_to_add)} columns to all partitions")

        # Verify the new schema
        new_schema = session.get_schema(table_name)
        print(f"\nNew schema ({len(new_schema)} fields):")
        for field in new_schema:
            print(f"  - {field.name}: {field.type}")

        print(f"\nNote: Run 'python scripts/ops/add_missing_indexes.py {table_name}' to add indexes")

        session.shutdown()
        return True

    except Exception as e:
        print(f"\n✗ Failed to add columns: {e}")
        import traceback
        traceback.print_exc()
        session.shutdown()
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Add new landing columns to a logical landing table using DLDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add new columns to production table
  python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py

  # Dry run to see what would be done
  python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py --dry-run

  # Add to test table
  python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py --table landing_test

  # Add to custom database
  python scripts/migrations/landing_schema_2026_06/add_new_columns_for_landing.py --db-uri s3://custom-bucket
        """
    )

    parser.add_argument(
        "--table",
        default="wind_tunnel_landing",
        help="Table name (default: wind_tunnel_landing)"
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

    # Get config
    db_uri = args.db_uri or default_config.tables.db_uri
    storage_options = default_config.s3.to_storage_options()

    success = add_columns_to_table(
        db_uri=db_uri,
        table_name=args.table,
        storage_options=storage_options,
        dry_run=args.dry_run
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
