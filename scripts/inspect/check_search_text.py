#!/usr/bin/env python3
"""Inspect search_text values in a serving table."""
import argparse

from wt_sdk import WTGatewayClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect search_text values")
    parser.add_argument(
        "--table",
        default=None,
        help="Serving logical table to inspect (default: selected by WT_SDK_PROFILE)",
    )
    parser.add_argument("--limit", type=int, default=3, help="Rows to display (default: 3)")
    args = parser.parse_args()

    with WTGatewayClient() as client:
        table_name = args.table or client.config.tables.serving_table
        print(f"Checking search_text field in {table_name}...")
        print("=" * 60)
        df = client._filter_table(
            table_name,
            query="id IS NOT NULL",
            limit=args.limit,
            columns=["id", "search_text"],
        )

    if df.empty:
        print("No records found.")
        return 0

    for index, row in df.iterrows():
        search_text = row.get("search_text") or ""
        print(f"\nRecord {index + 1}: {row.get('id', 'N/A')}")
        print(f"  search_text length: {len(search_text)} chars")
        print(f"  preview: {search_text[:200]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
