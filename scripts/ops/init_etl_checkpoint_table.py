"""Safely create the ETL checkpoint control table if it does not exist."""

import argparse

from wt_sdk.etl import (
    DEFAULT_CHECKPOINT_TABLE,
    DldbCheckpointStore,
    resolve_etl_state_db_uri,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create the ETL checkpoint table")
    parser.add_argument("--db-uri", default=None)
    parser.add_argument("--table", default=DEFAULT_CHECKPOINT_TABLE)
    parser.add_argument("--confirm-create", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_uri = resolve_etl_state_db_uri(args.db_uri)
    print(f"ETL state database: {db_uri}")
    print(f"Checkpoint table: {args.table}")
    if args.dry_run:
        print("Dry run: no table or data was changed.")
        return 0
    if not args.confirm_create:
        print("Refusing to connect/create without --confirm-create.")
        return 2

    store = DldbCheckpointStore(db_uri, table_name=args.table)
    try:
        created = store.initialize()
    finally:
        store.close()
    message = (
        "Checkpoint table created."
        if created
        else "Checkpoint table already exists and matches."
    )
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
