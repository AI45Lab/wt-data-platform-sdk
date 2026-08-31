"""
Initialize a profile-selected environment-config table.

This script creates ``env_config_test`` for the test profile or
``evaluation_env_config`` for production in DLDB/LanceDB.

The table is created in the separate environment-config database.
"""
import argparse

import dldb
from wt_sdk.config import (
    S3Config,
    resolve_env_config_db_uri,
    resolve_env_config_table_name,
)
from wt_sdk.core.evaluation_env_schema import EVALUATION_ENV_SCHEMA, SCALAR_INDEX_COLUMNS


def init_evaluation_env_table(
    *,
    profile: str = "test",
    confirm_recreate: bool = False,
    dry_run: bool = False,
) -> int:
    """Recreate the environment-config table after explicit confirmation."""
    db_uri = resolve_env_config_db_uri()
    table_name = resolve_env_config_table_name(profile=profile)
    print(f"Environment-config database: {db_uri}")
    print(f"Profile: {'production' if profile in {'prod', 'production'} else 'test'}")
    print(f"Target table: {table_name}")

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
    print(f"Checking if table '{table_name}' exists...")
    try:
        session.drop_table(table_name)
        print(f"Dropped existing table '{table_name}'")
    except Exception:
        print(f"Table '{table_name}' does not exist or could not be dropped (this is OK)")

    # 1. Create table (no partition needed for this table)
    print(f"Creating table '{table_name}'...")
    try:
        session.create_table(
            table_name,
            EVALUATION_ENV_SCHEMA,
        )
        print(f"Table '{table_name}' created successfully")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Table '{table_name}' already exists with a different schema.")
            print(
                "Use this script with --confirm-recreate after verifying the "
                "selected profile and table."
            )
            raise
        raise

    # 2. Create scalar indexes for query performance
    print("\nCreating scalar indexes for better query performance...")
    for column in SCALAR_INDEX_COLUMNS:
        print(f"  Creating index on '{column}'...")
        session.create_scalar_index(table_name, column)
        print(f"  ✓ Index created on '{column}'")

    print("\n" + "=" * 80)
    print("Evaluation environment config table initialization complete!")
    print("=" * 80)
    print(f"  Table: {table_name}")
    print(f"  Location: {db_uri}/{table_name}.lance")
    print(f"  Schema: {len(EVALUATION_ENV_SCHEMA)} fields")
    print(f"  Indexes: {len(SCALAR_INDEX_COLUMNS)} scalar indexes")
    print(f"\n  Indexes created on:")
    for col in SCALAR_INDEX_COLUMNS:
        print(f"    - {col}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recreate the profile-selected environment-config table."
    )
    parser.add_argument(
        "--profile",
        choices=("test", "prod", "production"),
        default="test",
        help=(
            "Select env_config_test for test or evaluation_env_config for "
            "production. Defaults to test."
        ),
    )
    parser.add_argument(
        "--confirm-recreate",
        action="store_true",
        help="Required: drop and recreate the selected env-config table.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the target database and table without connecting.",
    )
    args = parser.parse_args()
    return init_evaluation_env_table(
        profile=args.profile,
        confirm_recreate=args.confirm_recreate,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
