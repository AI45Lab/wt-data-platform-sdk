#!/usr/bin/env python3
"""
Table Management Script for DLDB.

This script provides utilities to manage DLDB tables:
1. List all tables in the database
2. Drop a table (or specific partitions)
3. Show all physical tables for a given logical table
4. Show schema and indexes for a given table

Uses DLDB SDK to properly handle the mapping between logical tables
and their physical partition tables.

Physical table naming pattern for VALUE partitions:
    {table_name}_type_VALUE_column_{partition_column}_partition_{partition_value}

Physical table naming pattern for HASH partitions:
    {table_name}_type_HASH_column_{partition_column}_partitions_{n}_partition_{bucket}

Example:
    python scripts/ops/table_manager.py list
    python scripts/ops/table_manager.py list --db-uri s3://wind-tunnel-dldb
    python scripts/ops/table_manager.py drop v2_landing_test
    python scripts/ops/table_manager.py drop evaluation_env_config --db-uri s3://wind-tunnel-env-config
    python scripts/ops/table_manager.py drop v2_landing_test --partition SFT
    python scripts/ops/table_manager.py show-physical v2_landing_test
    python scripts/ops/table_manager.py show-schema wind_tunnel_landing
    python scripts/ops/table_manager.py show-schema evaluation_env_config --db-uri s3://wind-tunnel-env-config
"""
import argparse
import sys
from typing import List, Optional

import dldb
from wt_sdk.config import default_config


# Physical table naming patterns for partitioned tables
VALUE_PARTITION_PREFIX = "_type_VALUE_column_"
HASH_PARTITION_PREFIX = "_type_HASH_column_"


def list_tables(db_uri: str = None) -> List[str]:
    """
    List all logical tables in the database using DLDB SDK.

    Args:
        db_uri: Database URI (default: s3://wind-tunnel-dldb)

    Returns:
        List of table names
    """
    db_uri = db_uri or default_config.tables.db_uri

    print(f"Connecting to {db_uri}...")
    session = dldb.connect(
        db_uri,
        storage_options=default_config.s3.to_storage_options()
    )

    tables = session.list_tables()
    session.shutdown()

    return tables


def _get_exact_table_record(session, table_name: str):
    """Return the exact dldb metadata record for a logical table, if present."""
    schema_table = getattr(session, "schema_table", None)
    return schema_table.get(table_name) if schema_table is not None else None


def _confirm_drop(table_name: str, partition: Optional[str], force: bool, confirm_table: Optional[str]) -> bool:
    """Require deliberate confirmation before a destructive table operation."""
    if force:
        if confirm_table != table_name:
            print(
                "Refusing non-interactive drop: --force requires "
                f"--confirm-table {table_name!r}."
            )
            return False
        return True

    target = f"partition '{partition}' from table" if partition else "entire table"
    typed_table = input(
        f"About to drop {target} '{table_name}'. Type the exact table name to continue: "
    ).strip()
    if typed_table != table_name:
        print("Aborted: table name did not match.")
        return False

    typed_drop = input("This operation is irreversible. Type DROP to confirm: ").strip()
    if typed_drop != "DROP":
        print("Aborted.")
        return False
    return True


def drop_table(
    table_name: str,
    partition: str = None,
    db_uri: str = None,
    force: bool = False,
    confirm_table: Optional[str] = None,
) -> bool:
    """
    Drop a table or specific partition using DLDB SDK.

    Args:
        table_name: Logical table name to drop
        partition: Optional partition value to drop (drops entire table if None)
        db_uri: Database URI (default: s3://wind-tunnel-dldb)
        force: Use non-interactive confirmation. Requires confirm_table to match.
        confirm_table: Exact logical table name required with force=True.

    Returns:
        True if successful
    """
    db_uri = db_uri or default_config.tables.db_uri

    print(f"Connecting to {db_uri}...")
    session = dldb.connect(
        db_uri,
        storage_options=default_config.s3.to_storage_options()
    )

    # Check exact metadata rather than relying on dldb's cached table lookup.
    record = _get_exact_table_record(session, table_name)
    if record is None:
        print(f"Error: Table '{table_name}' does not exist")
        session.shutdown()
        return False

    if not _confirm_drop(table_name, partition, force, confirm_table):
        session.shutdown()
        return False

    try:
        session.drop_table(table_name, partition=partition)
        if partition:
            print(f"✓ Dropped partition '{partition}' from table '{table_name}'")
        else:
            print(f"✓ Dropped table '{table_name}' and all its partitions")
        session.shutdown()
        return True
    except Exception as e:
        print(f"Error dropping table: {e}")
        session.shutdown()
        return False


def show_physical_tables(table_name: str, db_uri: str = None) -> List[str]:
    """
    Show all physical tables for a given logical table.

    NOTE: This function uses LanceDB native SDK directly to list physical tables,
    because DLDB SDK only manages logical tables. Physical tables follow the
    naming patterns:
    - {table_name}_type_VALUE_column_{partition_column}_partition_{partition_value}
    - {table_name}_type_HASH_column_{partition_column}_partitions_{n}_partition_{bucket}

    Args:
        table_name: Logical table name
        db_uri: Database URI (default: s3://wind-tunnel-dldb)

    Returns:
        List of physical table names
    """
    import lancedb

    db_uri = db_uri or default_config.tables.db_uri

    print(f"Connecting to {db_uri}...")
    db = lancedb.connect(
        db_uri,
        storage_options=default_config.s3.to_storage_options()
    )

    # Get all physical tables
    # Use list_tables() instead of deprecated table_names()
    all_tables = db.list_tables().tables

    # Filter tables that belong to the given logical table.
    # Use the full partition marker so similarly named tables cannot be confused.
    physical_tables = [
        t for t in all_tables
        if (
            t.startswith(f"{table_name}{VALUE_PARTITION_PREFIX}")
            or t.startswith(f"{table_name}{HASH_PARTITION_PREFIX}")
        )
    ]

    # Note: LanceDB connection doesn't need explicit close

    return physical_tables


def parse_partition_from_physical(physical_table_name: str, table_name: str) -> str:
    """
    Extract partition value from physical table name.

    Physical table patterns:
    - {table_name}_type_VALUE_column_{partition_column}_partition_{partition_value}
    - {table_name}_type_HASH_column_{partition_column}_partitions_{n}_partition_{bucket}

    Args:
        physical_table_name: Full physical table name
        table_name: Logical table name

    Returns:
        Partition value
    """
    value_prefix = f"{table_name}{VALUE_PARTITION_PREFIX}"
    if physical_table_name.startswith(value_prefix):
        rest = physical_table_name[len(value_prefix):]
        parts = rest.split("_partition_", 1)
        if len(parts) == 2:
            partition_column = parts[0]
            partition_value = parts[1]
            return f"{partition_column}={partition_value}"

    hash_prefix = f"{table_name}{HASH_PARTITION_PREFIX}"
    if physical_table_name.startswith(hash_prefix):
        rest = physical_table_name[len(hash_prefix):]
        parts = rest.split("_partitions_", 1)
        if len(parts) == 2:
            partition_column = parts[0]
            count_and_bucket = parts[1].split("_partition_", 1)
            if len(count_and_bucket) == 2:
                partition_count, partition_bucket = count_and_bucket
                return f"hash({partition_column})={partition_bucket}/{partition_count}"

    return "unknown"


def _index_field(index, field: str):
    """Read an index field from current and older dldb return shapes."""
    if isinstance(index, dict):
        return index.get(field)
    if field == "type":
        return getattr(index, "index_type", getattr(index, "type", None))
    return getattr(index, field, None)


def show_schema(table_name: str, db_uri: str = None) -> dict:
    """
    Show schema and indexes for a given table using DLDB SDK.

    Displays:
    - Table name
    - All fields with their types and nullability
    - All scalar indexes

    Args:
        table_name: Logical table name
        db_uri: Database URI (default: s3://wind-tunnel-dldb)

    Returns:
        Dict with schema and indexes info
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
        return {"error": "Table not found"}

    result = {
        "table_name": table_name,
        "fields": [],
        "indexes": []
    }

    print("=" * 60)
    print(f"Schema for table: {table_name}")
    print("=" * 60)

    record = None
    try:
        record = session.schema_table.get(table_name)
        if record is not None:
            print("\nPartition:")
            print(f"  Column:     {record.partition_column or '(none)'}")
            print(f"  Type:       {record.partition_type or '(none)'}")
            if getattr(record, "partitions", -1) and record.partitions > 0:
                print(f"  Partitions: {record.partitions}")
    except Exception as e:
        print(f"\nPartition: error reading metadata: {e}")

    # Get schema using DLDB's get_schema method
    try:
        schema = session.get_schema(table_name)

        print(f"\nFields ({len(schema)}):")
        print(f"  {'Field':<25} {'Type':<30} {'Nullable':<10}")
        print(f"  {'-' * 25} {'-' * 30} {'-' * 10}")

        for field in schema:
            field_type = str(field.type)
            nullable = "Yes" if field.nullable else "No"
            print(f"  {field.name:<25} {field_type:<30} {nullable:<10}")
            result["fields"].append({
                "name": field.name,
                "type": field_type,
                "nullable": field.nullable
            })
    except Exception as e:
        print(f"Error getting schema: {e}")

    # Get indexes using DLDB's list_indices method
    print(f"\nScalar Indexes:")
    try:
        if record is not None and record.partition_column:
            table = session._get_table(table_name)
            partitions = sorted(table.list_partitions())
            if not partitions:
                print("  No physical partitions found")
            for partition in partitions:
                indexes = session.list_indices(table_name, partition=partition)
                if not indexes:
                    print(f"  Partition {partition}: no scalar indexes found")
                    continue
                print(f"  Partition {partition}:")
                for idx in indexes:
                    index_name = _index_field(idx, "name") or "unknown"
                    index_type = _index_field(idx, "type") or "unknown"
                    print(f"    - {index_name} ({index_type})")
                    result["indexes"].append({
                        "partition": partition,
                        "name": index_name,
                        "type": index_type,
                    })
        else:
            indexes = session.list_indices(table_name)
            if not indexes:
                print("  No scalar indexes found")
            else:
                for idx in indexes:
                    index_name = _index_field(idx, "name") or "unknown"
                    index_type = _index_field(idx, "type") or "unknown"
                    print(f"  - {index_name} ({index_type})")
                    result["indexes"].append({
                        "name": index_name,
                        "type": index_type,
                    })
    except Exception as e:
        print(f"  Error getting indexes: {e}")

    session.shutdown()
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Table Management Script for DLDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all tables
  python scripts/ops/table_manager.py list

  # List all tables in a custom bucket
  python scripts/ops/table_manager.py list --db-uri s3://my-bucket

  # Drop a table (with confirmation)
  python scripts/ops/table_manager.py drop v2_landing_test

  # Drop a table non-interactively (table name must be repeated exactly)
  python scripts/ops/table_manager.py drop v2_landing_test --force --confirm-table v2_landing_test

  # Drop a specific partition
  python scripts/ops/table_manager.py drop v2_landing_test --partition SFT

  # Show physical tables for a logical table
  python scripts/ops/table_manager.py show-physical v2_landing_test

  # Show schema and indexes for a table
  python scripts/ops/table_manager.py show-schema wind_tunnel_landing
  python scripts/ops/table_manager.py show-schema wind_tunnel_serving
        """
    )

    parser.add_argument(
        "command",
        choices=["list", "drop", "show-physical", "show-schema"],
        help="Command to execute"
    )

    parser.add_argument(
        "table_name",
        nargs="?",
        help="Table name (for drop and show-physical commands)"
    )

    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: s3://wind-tunnel-dldb)"
    )

    parser.add_argument(
        "--partition",
        default=None,
        help="Partition value (for drop command, drops entire table if not specified)"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Use non-interactive confirmation; requires --confirm-table for drop"
    )

    parser.add_argument(
        "--confirm-table",
        default=None,
        help="Exact table name required with --force for a non-interactive drop"
    )

    args = parser.parse_args()

    if args.command == "list":
        print("=" * 60)
        print("Listing all logical tables in database")
        print("=" * 60)
        tables = list_tables(args.db_uri)
        if tables:
            print(f"\nFound {len(tables)} table(s):")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
        else:
            print("\nNo tables found")
        return 0

    elif args.command == "drop":
        if not args.table_name:
            parser.error("--table-name is required for drop command")

        print("=" * 60)
        print("Drop table/partition")
        print("=" * 60)

        success = drop_table(
            args.table_name,
            partition=args.partition,
            db_uri=args.db_uri,
            force=args.force,
            confirm_table=args.confirm_table,
        )
        return 0 if success else 1

    elif args.command == "show-physical":
        if not args.table_name:
            parser.error("--table-name is required for show-physical command")

        print("=" * 60)
        print(f"Physical tables for logical table: {args.table_name}")
        print("=" * 60)

        physical_tables = show_physical_tables(args.table_name, args.db_uri)

        if physical_tables:
            print(f"\nFound {len(physical_tables)} physical table(s):")
            for i, physical in enumerate(physical_tables, 1):
                partition_info = parse_partition_from_physical(physical, args.table_name)
                print(f"  {i}. {physical}")
                print(f"     Partition: {partition_info}")
        else:
            print(f"\nNo physical tables found for '{args.table_name}'")
            print("Note: This could mean:")
            print("  - The logical table doesn't exist")
            print("  - The table exists but has no partitions yet")
            print("  - The table uses an unexpected physical naming pattern")
        return 0

    elif args.command == "show-schema":
        if not args.table_name:
            parser.error("table_name is required for show-schema command")

        show_schema(args.table_name, args.db_uri)
        return 0


if __name__ == "__main__":
    sys.exit(main())
