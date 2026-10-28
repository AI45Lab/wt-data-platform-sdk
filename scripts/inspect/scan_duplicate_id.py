#!/usr/bin/env python3
"""
Scan a dldb logical table for duplicate id values.

This is a read-only diagnostic helper. It uses the dldb API and only fetches
the id column from the logical table.

Usage:
    python scripts/inspect/scan_duplicate_id.py
    python scripts/inspect/scan_duplicate_id.py --table landing_test
    python scripts/inspect/scan_duplicate_id.py --table wt_landing --max-output 200
"""
import argparse
import sys

import dldb
import pandas as pd

from wt_sdk.config import TEST_LANDING_TABLE, db_uri_for_table, storage_options_for


def default_db_uri(table_name: str) -> str:
    """Database hosting a known landing/serving table, else the default db."""
    try:
        return db_uri_for_table(table_name)
    except ValueError:
        return db_uri_for_table(TEST_LANDING_TABLE)


def scan_duplicate_ids(
    table_name: str,
    db_uri: str | None = None,
    query: str = "",
    max_output: int = 100,
) -> int:
    """Scan table id column and print duplicate id counts."""
    db_uri = db_uri or default_db_uri(table_name)

    print(f"Database: {db_uri}")
    print(f"Table:    {table_name}")
    print(f"Filter:   {query or '(all rows)'}")
    print("=" * 80)

    print("Connecting...")
    connect_kwargs = {}
    storage_options = storage_options_for(db_uri)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
    session = dldb.connect(db_uri, **connect_kwargs)

    try:
        if not session.table_exists(table_name):
            print(f"Error: table '{table_name}' does not exist")
            print(f"Available tables: {session.list_tables()}")
            return 1

        print("Reading id column...")
        df = session.filter(
            table_name,
            query=query,
            limit=None,
            columns=["id"],
        )

        rows_scanned = len(df)
        if "id" not in df.columns:
            print("Error: result does not contain an id column")
            return 1

        ids = df["id"].dropna().astype(str)
        ids = ids[ids.str.strip() != ""]

        counts = ids.value_counts()
        duplicates = counts[counts > 1].sort_values(ascending=False)

        duplicate_id_count = len(duplicates)
        duplicate_row_count = int(duplicates.sum()) if duplicate_id_count else 0
        extra_duplicate_rows = duplicate_row_count - duplicate_id_count

        print("=" * 80)
        print("Duplicate ID Scan Summary")
        print("=" * 80)
        print(f"Rows scanned:                 {rows_scanned}")
        print(f"Non-empty id values scanned:  {len(ids)}")
        print(f"Duplicate id values:          {duplicate_id_count}")
        print(f"Rows involved in duplicates:  {duplicate_row_count}")
        print(f"Duplicate rows beyond first:  {extra_duplicate_rows}")

        if duplicate_id_count == 0:
            print("\nNo duplicate id values found.")
            return 0

        print(f"\nDuplicate ids (showing up to {max_output}):")
        for record_id, count in duplicates.head(max_output).items():
            print(f"  {record_id}\tcount={int(count)}")

        remaining = duplicate_id_count - min(duplicate_id_count, max_output)
        if remaining > 0:
            print(f"\n... {remaining} more duplicate id values not shown.")

        return 0
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scan a dldb logical table for duplicate id values",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--table",
        default=TEST_LANDING_TABLE,
        help=f"Logical table name to scan (default: {TEST_LANDING_TABLE})",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help=(
            "Database URI (default: derived from --table for known "
            "landing/serving names, else the landing database)"
        ),
    )
    parser.add_argument(
        "--query",
        default="",
        help="Optional SQL filter to narrow the scan",
    )
    parser.add_argument(
        "--max-output",
        type=int,
        default=100,
        help="Maximum duplicate ids to print (default: 100)",
    )

    args = parser.parse_args()
    return scan_duplicate_ids(
        table_name=args.table,
        db_uri=args.db_uri,
        query=args.query,
        max_output=args.max_output,
    )


if __name__ == "__main__":
    sys.exit(main())
