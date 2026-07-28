#!/usr/bin/env python3
"""
Scan a dldb logical table for duplicate id values.

This is a read-only diagnostic helper. It uses the dldb API and only fetches
the id column from the logical table.

Usage:
    AWS_EC2_METADATA_DISABLED=true python scripts/inspect/scan_duplicate_id.py
    AWS_EC2_METADATA_DISABLED=true python scripts/inspect/scan_duplicate_id.py --table landing_test
    AWS_EC2_METADATA_DISABLED=true python scripts/inspect/scan_duplicate_id.py --table wind_tunnel_landing --max-output 200
"""
import argparse
import sys

import dldb
import pandas as pd

from wt_sdk.config import default_config


def scan_duplicate_ids(
    table_name: str,
    db_uri: str | None = None,
    query: str = "",
    max_output: int = 100,
) -> int:
    """Scan table id column and print duplicate id counts."""
    db_uri = db_uri or default_config.tables.db_uri

    print(f"Database: {db_uri}")
    print(f"Table:    {table_name}")
    print(f"Filter:   {query or '(all rows)'}")
    print("=" * 80)

    print("Connecting...")
    session = dldb.connect(
        db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )

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
        default="landing_test",
        help="Logical table name to scan (default: landing_test)",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: from wt_sdk config)",
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
