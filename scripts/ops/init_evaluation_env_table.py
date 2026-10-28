"""
Initialize a profile-selected environment-config table (new-generation stack).

This script creates ``env_test`` for the test profile or ``wt_env_config``
for production, in the environment-config database (GPFS by default,
``WT_SDK_ENV_CONFIG_DB_URI`` override).
"""
import argparse

import dldb
from wt_sdk.config import (
    resolve_env_config_db_uri,
    resolve_env_config_table_name,
    storage_options_for,
)
from wt_sdk.core.evaluation_env_schema import (
    EVALUATION_ENV_PARTITIONS,
    EVALUATION_ENV_PARTITION_COLUMN,
    EVALUATION_ENV_PARTITION_TYPE,
    EVALUATION_ENV_SCHEMA,
    SCALAR_INDEX_COLUMNS,
)


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

    # Use DLDB wrapper SDK with the (GPFS or S3) env-config database
    connect_kwargs = {}
    storage_options = storage_options_for(db_uri)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
    session = dldb.connect(db_uri, **connect_kwargs)

    # Check if table already exists and try to drop it
    # We try to drop directly first since the table existence check is unreliable
    print(f"Checking if table '{table_name}' exists...")
    try:
        session.drop_table(table_name)
        print(f"Dropped existing table '{table_name}'")
    except Exception:
        print(f"Table '{table_name}' does not exist or could not be dropped (this is OK)")

    # 1. Create table, HASH-partitioned on job_id (2026.10 job splitting:
    #    every read path filters by job_id first, so buckets prune per job).
    print(f"Creating table '{table_name}'...")
    try:
        session.create_table(
            table_name,
            EVALUATION_ENV_SCHEMA,
            partition_column=EVALUATION_ENV_PARTITION_COLUMN,
            partition_type=EVALUATION_ENV_PARTITION_TYPE,
            partitions=EVALUATION_ENV_PARTITIONS,
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

    # 2. Scalar indexes are NOT created here: on a HASH-partitioned table
    #    indexes live per bucket and are created after data is ingested.
    print("\n" + "=" * 80)
    print("Evaluation environment config table initialization complete!")
    print("=" * 80)
    print(f"  Table: {table_name}")
    print(f"  Location: {db_uri}/{table_name}.lance")
    print(f"  Schema: {len(EVALUATION_ENV_SCHEMA)} fields")
    print(
        f"  Partitioning: {EVALUATION_ENV_PARTITION_TYPE}({EVALUATION_ENV_PARTITION_COLUMN})"
        f" x {EVALUATION_ENV_PARTITIONS}"
    )
    print(f"\n  Scalar indexes configured ({len(SCALAR_INDEX_COLUMNS)}):")
    for col in SCALAR_INDEX_COLUMNS:
        print(f"    - {col}")
    print("\n  NOTE: For HASH partitioned tables, indexes are created per bucket.")
    print("  After ingesting data, run:")
    print(f"    python scripts/ops/maintain_env_config_indexes.py --profile {'production' if profile in {'prod', 'production'} else 'test'}")
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
            "Select env_test for test or wt_env_config for "
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
