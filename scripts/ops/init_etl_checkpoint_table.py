"""Safely create the ETL checkpoint control table if it does not exist."""

import argparse

from wt_sdk.etl import (
    DldbCheckpointStore,
    PRODUCTION_CHECKPOINT_TABLE,
    TEST_CHECKPOINT_TABLE,
    resolve_etl_state_db_uri,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create ETL checkpoint tables")
    parser.add_argument("--db-uri", default=None)
    parser.add_argument(
        "--table",
        action="append",
        default=None,
        help="Optional custom table; repeat to initialize multiple tables.",
    )
    parser.add_argument("--confirm-create", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_uri = resolve_etl_state_db_uri(args.db_uri)
    tables = args.table or [TEST_CHECKPOINT_TABLE, PRODUCTION_CHECKPOINT_TABLE]
    print(f"ETL state database: {db_uri}")
    print(f"Checkpoint tables: {', '.join(tables)}")
    if args.dry_run:
        print("Dry run: no table or data was changed.")
        return 0
    if not args.confirm_create:
        print("Refusing to connect/create without --confirm-create.")
        return 2

    for table in tables:
        store = DldbCheckpointStore(db_uri, table_name=table)
        try:
            created = store.initialize()
        finally:
            store.close()
        state = "created" if created else "already exists and matches"
        print(f"{table}: {state}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
