"""
Get all unique tag values from a table.

Usage:
    python scripts/inspect/get_unique_tags.py --table wind_tunnel_serving
    python scripts/inspect/get_unique_tags.py --table wind_tunnel_landing
"""
import argparse
import sys
from collections import Counter

import dldb
from wt_sdk.config import default_config


def main():
    parser = argparse.ArgumentParser(description="Get unique tags from a table")
    parser.add_argument("--table", type=str, required=True,
                       help="Table name (e.g., wind_tunnel_serving, wind_tunnel_landing)")
    parser.add_argument("--limit", type=int, default=50000,
                       help="Max records to scan (default: 50000)")

    args = parser.parse_args()

    db_name = default_config.tables.db_uri
    table_name = args.table

    print(f"Database: {db_name}")
    print(f"Table: {table_name}")
    print(f"Scanning up to {args.limit} records...")
    print("=" * 80)

    # Initialize DLDB session
    try:
        session = dldb.connect(
            db_name,
            storage_options=default_config.s3.to_storage_options()
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return 1

    # Check if table exists
    if not session.table_exists(table_name):
        print(f"Error: Table '{table_name}' does not exist")
        print(f"Available tables: {session.list_tables()}")
        session.shutdown()
        return 1

    # Get total count
    try:
        total_count = session.count_rows(table_name)
        print(f"Total rows in table: {total_count}")
    except Exception as e:
        print(f"Could not get total count: {e}")

    # Fetch data
    print("Fetching records...")
    try:
        df = session.filter(table_name, query="", limit=args.limit, columns=["tags"])
    except Exception as e:
        print(f"Error querying data: {e}")
        session.shutdown()
        return 1

    if len(df) == 0:
        print("No records found.")
        session.shutdown()
        return 0

    print(f"Fetched {len(df)} records")
    print("=" * 80)

    # Extract all tags
    all_tags = []
    records_with_tags = 0

    for tags in df["tags"]:
        if tags is not None:
            tags_list = tags.tolist() if hasattr(tags, 'tolist') else list(tags)
            if tags_list:
                records_with_tags += 1
                all_tags.extend(tags_list)

    # Count unique tags and their frequencies
    tag_counter = Counter(all_tags)
    unique_tags = sorted(tag_counter.items(), key=lambda x: -x[1])

    print(f"\nRecords with tags: {records_with_tags} / {len(df)}")
    print(f"Unique tag values: {len(unique_tags)}")
    print("=" * 80)
    print("\nTag distribution:")
    print("-" * 80)
    print(f"{'Count':<10} {'Percentage':<12} {'Tag'}")
    print("-" * 80)

    for tag, count in unique_tags:
        percentage = (count / records_with_tags) * 100 if records_with_tags > 0 else 0
        print(f"{count:<10} {percentage:>10.2f}%   {tag}")

    print("-" * 80)
    print(f"\nAll unique tags ({len(unique_tags)}):")
    print(", ".join(tag for tag, _ in unique_tags))

    session.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
