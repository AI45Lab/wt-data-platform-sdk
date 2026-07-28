"""
Initialize evaluation_env_config table for evaluation platform.

This script creates the evaluation_env_config table in DLDB/LanceDB
for storing environment configurations with SQL-like query support.

The table is created in the separate environment-config database.
"""
import argparse

import dldb
from wt_sdk.config import S3Config, resolve_env_config_db_uri
from wt_sdk.core.evaluation_env_schema import EVALUATION_ENV_SCHEMA, SCALAR_INDEX_COLUMNS


TABLE_NAME = "evaluation_env_config"


def init_evaluation_env_table(*, confirm_recreate: bool = False, dry_run: bool = False) -> int:
    """Recreate the environment-config table after explicit confirmation."""
    db_uri = resolve_env_config_db_uri()
    print(f"Environment-config database: {db_uri}")
    print(f"Target table: {TABLE_NAME}")

    if dry_run:
        print("Dry run: no table or data was changed.")
        return 0
    if not confirm_recreate:
        print("Refusing to recreate without --confirm-recreate. Use --dry-run to inspect first.")
        return 2

    print(f"Connecting to {db_uri}...")

    # Use DLDB wrapper SDK with separate bucket for env configs
    session = dldb.connect(
        db_uri,
        storage_options=S3Config().to_storage_options()
    )

    # Check if table already exists and try to drop it
    # We try to drop directly first since the table existence check is unreliable
    print(f"Checking if table '{TABLE_NAME}' exists...")
    try:
        session.drop_table(TABLE_NAME)
        print(f"Dropped existing table '{TABLE_NAME}'")
    except Exception:
        print(f"Table '{TABLE_NAME}' does not exist or could not be dropped (this is OK)")

    # 1. Create table (no partition needed for this table)
    print(f"Creating table '{TABLE_NAME}'...")
    try:
        session.create_table(
            TABLE_NAME,
            EVALUATION_ENV_SCHEMA,
        )
        print(f"Table '{TABLE_NAME}' created successfully")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Table '{TABLE_NAME}' already exists with a different schema.")
            print(f"Please manually drop it first using LanceDB directly:")
            print(f"  db = lancedb.connect('{db_uri}', storage_options=...)")
            print(f"  db.drop_table('{TABLE_NAME}')")
            raise
        raise

    # 2. Create scalar indexes for query performance
    print("\nCreating scalar indexes for better query performance...")
    for column in SCALAR_INDEX_COLUMNS:
        print(f"  Creating index on '{column}'...")
        session.create_scalar_index(TABLE_NAME, column)
        print(f"  ✓ Index created on '{column}'")

    print("\n" + "=" * 80)
    print("Evaluation environment config table initialization complete!")
    print("=" * 80)
    print(f"  Table: {TABLE_NAME}")
    print(f"  Location: {db_uri}/{TABLE_NAME}.lance")
    print(f"  Schema: {len(EVALUATION_ENV_SCHEMA)} fields")
    print(f"  Indexes: {len(SCALAR_INDEX_COLUMNS)} scalar indexes")
    print(f"\n  Indexes created on:")
    for col in SCALAR_INDEX_COLUMNS:
        print(f"    - {col}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recreate the evaluation environment-config table."
    )
    parser.add_argument(
        "--confirm-recreate",
        action="store_true",
        help="Required: drop and recreate evaluation_env_config.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the target database and table without connecting.",
    )
    args = parser.parse_args()
    return init_evaluation_env_table(
        confirm_recreate=args.confirm_recreate,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
