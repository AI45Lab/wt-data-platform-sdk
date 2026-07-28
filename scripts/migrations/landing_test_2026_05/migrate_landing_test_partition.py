#!/usr/bin/env python3
"""
Migrate landing_test table from dt partition to job_id partition.

Flow (avoids "table already exists" error):
1. Read data from old landing_test (dt partition) - only the allowlisted dt partitions
2. Create temporary table landing_test_backup (job_id partition)
3. Write transformed data to backup (dataset_type "Test" -> "RL")
4. Drop old landing_test (dt partition)
5. Create new landing_test (job_id partition)
6. Read from backup, write to new landing_test
7. Drop backup table (optional, use --drop-backup)

Rows outside the allowlisted dt partitions are intentionally not copied.
No indexes are created in this script (use tests/create_landing_test_indexes.py).

Usage:
    AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_partition.py
    AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_partition.py --dry-run
    AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_partition.py --drop-backup
"""
import argparse
import sys

import dldb
import numpy as np
import pandas as pd
import pyarrow as pa
from loguru import logger
from wt_sdk.core.schemas import LANDING_PARTITIONS, LANDING_SCHEMA, LANDING_PARTITION_COLUMN, LANDING_PARTITION_TYPE

# Remove default loguru handler to avoid duplicate logs
logger.remove()

# Re-add with custom format
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO",
)


# Partitions to migrate (only these dt partitions will be included)
DT_PARTITIONS_TO_MIGRATE = [
    "2026-03-18",
    "2026-03-19",
    "2026-03-20",
    "2026-03-23",
    "2026-03-24",
    "2026-03-26",
    "2026-04-01",
    "2026-04-02",
]

TABLE_NAME = "landing_test"
BACKUP_TABLE = "landing_test_backup"  # temporary table during migration
NESTED_COLUMNS = [
    "messages",
    "response",
    "chosen_response",
    "rejected_response",
    "blob_manifest",
]


def _is_null_or_empty(val) -> bool:
    """Check if a value is None, NaN, or empty string."""
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def _pythonize_nested_value(val):
    """Convert pandas/LanceDB nested values into plain Python containers."""
    if isinstance(val, np.ndarray):
        return [_pythonize_nested_value(item) for item in val.tolist()]
    if isinstance(val, dict):
        return {key: _pythonize_nested_value(item) for key, item in val.items()}
    if isinstance(val, list):
        return [_pythonize_nested_value(item) for item in val]
    return val


def _to_dldb_write_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return an Arrow-backed DataFrame matching LANDING_SCHEMA for dldb.add()."""
    schema_columns = [field.name for field in LANDING_SCHEMA]
    missing_columns = [column for column in schema_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Source DataFrame is missing LANDING_SCHEMA columns: {missing_columns}")

    normalized = df.loc[:, schema_columns].copy()
    for column in NESTED_COLUMNS:
        if column in normalized.columns:
            normalized[column] = normalized[column].map(_pythonize_nested_value)

    arrow_table = pa.Table.from_pandas(
        normalized,
        schema=LANDING_SCHEMA,
        preserve_index=False,
    )
    return arrow_table.to_pandas(types_mapper=pd.ArrowDtype)


def _drop_empty_existing_backup(session) -> None:
    """Remove an empty stale backup table left by an interrupted migration."""
    if not session.table_exists(BACKUP_TABLE):
        return

    backup_count = session.count_rows(BACKUP_TABLE)
    if backup_count == 0:
        logger.warning(f"Backup table '{BACKUP_TABLE}' already exists but is empty; dropping it and recreating.")
        session.drop_table(BACKUP_TABLE)
        return

    logger.error(f"Backup table '{BACKUP_TABLE}' already exists with {backup_count} rows.")
    logger.error("Keeping the existing backup for safety. Verify it manually before dropping/reusing it.")
    logger.error(
        "To remove it after verification: "
        f"python scripts/ops/table_manager.py drop {BACKUP_TABLE} "
        f"--force --confirm-table {BACKUP_TABLE}"
    )
    session.shutdown()
    sys.exit(1)


def migrate_partition(dry_run: bool = False, drop_backup: bool = False) -> None:
    """
    Migrate landing_test from dt partition to job_id partition.

    Args:
        dry_run: If True, read and validate but don't write anything
        drop_backup: If True, drop the backup table after migration completes.
                     Default False to keep a safety copy until you verify the result.
    """
    from wt_sdk.config import default_config

    db_uri = default_config.tables.db_uri
    storage_options = default_config.s3.to_storage_options()

    logger.info(f"Connecting to {db_uri}...")
    session = dldb.connect(db_uri, storage_options=storage_options)

    # =========================================================================
    # Step 1: Read data from specific dt partitions
    # =========================================================================
    logger.info(f"Reading data from dt partitions: {DT_PARTITIONS_TO_MIGRATE}")

    dt_filter = " OR ".join([f"dt = '{d}'" for d in DT_PARTITIONS_TO_MIGRATE])
    logger.info(f"Filter: {dt_filter}")

    df = session.filter(
        TABLE_NAME,
        query=dt_filter,
        limit=None,
    )

    total_rows = len(df)
    logger.info(f"Read {total_rows} rows from source partitions")

    if total_rows == 0:
        logger.warning("No data found in the specified partitions. Nothing to migrate.")
        session.shutdown()
        return

    # Show partition distribution
    dt_counts = df["dt"].value_counts().sort_index()
    logger.info(f"Rows per dt partition:")
    for dt_val, count in dt_counts.items():
        logger.info(f"  {dt_val}: {count}")

    # =========================================================================
    # Step 2: Validate job_id — log null/empty values, don't skip
    # =========================================================================
    logger.info("Validating job_id field...")

    null_or_empty_mask = df["job_id"].apply(_is_null_or_empty)
    null_or_empty_count = int(null_or_empty_mask.sum())
    valid_count = total_rows - null_or_empty_count

    logger.info(f"  Valid rows (non-null job_id): {valid_count}")
    logger.info(f"  Invalid rows (null/empty job_id): {null_or_empty_count}")

    if null_or_empty_count > 0:
        null_rows = df[null_or_empty_mask]
        logger.warning(f"=== RECORDS WITH NULL/EMPTY job_id ({null_or_empty_count}) ===")
        sample = null_rows[["id", "dt", "dataset_type", "job_id", "env_id"]].head(20)
        for _, row in sample.iterrows():
            logger.warning(
                f"  id={row['id']}, dt={row['dt']}, dataset_type={row['dataset_type']}, "
                f"job_id={repr(row['job_id'])}, env_id={row['env_id']}"
            )
        if null_or_empty_count > 20:
            logger.warning(f"  ... and {null_or_empty_count - 20} more null/empty job_id records")
        logger.warning("=== END OF NULL job_id RECORDS ===")

        if dry_run:
            logger.info("Dry run: stopping here, null records are logged above")
            session.shutdown()
            return
        else:
            # Filter out null job_id rows (logged above so user can investigate)
            df = df[~null_or_empty_mask].copy()
            logger.info(f"Proceeding with {len(df)} valid rows (null rows excluded)")

    # =========================================================================
    # Step 3: Fix dataset_type "Test" -> "RL"
    # =========================================================================
    logger.info("Transforming dataset_type 'Test' -> 'RL'...")

    test_count = int((df["dataset_type"] == "Test").sum())
    rl_count = int((df["dataset_type"] == "RL").sum())
    logger.info(f"  Before: Test={test_count}, RL={rl_count}")

    df["dataset_type"] = df["dataset_type"].replace("Test", "RL")

    test_count_after = int((df["dataset_type"] == "Test").sum())
    rl_count_after = int((df["dataset_type"] == "RL").sum())
    logger.info(f"  After:  Test={test_count_after}, RL={rl_count_after}")

    if test_count_after > 0:
        logger.warning(f"  Still have {test_count_after} rows with dataset_type='Test' after replacement")

    if dry_run:
        logger.info(f"[DRY RUN] Would create backup table '{BACKUP_TABLE}' with job_id partition")
        logger.info(f"[DRY RUN] Would write {len(df)} rows to backup")
        logger.info(f"[DRY RUN] Would drop old '{TABLE_NAME}' (dt partition)")
        logger.info(f"[DRY RUN] Would create new '{TABLE_NAME}' with job_id partition")
        logger.info(f"[DRY RUN] Would copy {len(df)} rows from backup to new table")
        logger.info("[DRY RUN] Rows outside the allowlisted dt partitions would be intentionally discarded")
        if drop_backup:
            logger.info(f"[DRY RUN] Would drop backup table '{BACKUP_TABLE}'")
        session.shutdown()
        return

    # =========================================================================
    # Step 4: Create backup table (job_id partition) and write data
    # =========================================================================
    logger.info(f"Creating backup table '{BACKUP_TABLE}' with partition_column='{LANDING_PARTITION_COLUMN}'...")

    _drop_empty_existing_backup(session)

    create_kwargs = {
        "partition_column": LANDING_PARTITION_COLUMN,
        "partition_type": LANDING_PARTITION_TYPE,
    }
    if LANDING_PARTITION_TYPE == "HASH":
        create_kwargs["partitions"] = LANDING_PARTITIONS

    session.create_table(
        BACKUP_TABLE,
        LANDING_SCHEMA,
        **create_kwargs,
    )
    logger.info(f"  Backup table '{BACKUP_TABLE}' created")

    df_write = _to_dldb_write_frame(df)
    session.add(BACKUP_TABLE, df_write)
    backup_count = session.count_rows(BACKUP_TABLE)
    logger.info(f"  Wrote {len(df_write)} rows to backup, verified: {backup_count} rows in backup")
    if backup_count != len(df_write):
        logger.error(f"Backup row count mismatch: expected {len(df_write)}, got {backup_count}")
        logger.error("Stopping before dropping the source table.")
        session.shutdown()
        sys.exit(1)

    # =========================================================================
    # Step 5: Drop old landing_test (dt partition)
    # =========================================================================
    logger.info(f"Dropping old '{TABLE_NAME}' (dt partition)...")
    try:
        session.drop_table(TABLE_NAME)
        logger.info(f"  Old table '{TABLE_NAME}' dropped")
    except Exception as e:
        logger.error(f"  Failed to drop old table: {e}")
        logger.error(
            "Manually drop it with: python scripts/ops/table_manager.py "
            "drop landing_test --force --confirm-table landing_test"
        )
        logger.error("Your data is safe in landing_test_backup. After fixing, re-run this script.")
        session.shutdown()
        sys.exit(1)

    # =========================================================================
    # Step 6: Create new landing_test (job_id partition) and copy data
    # =========================================================================
    logger.info(f"Creating new '{TABLE_NAME}' with partition_column='{LANDING_PARTITION_COLUMN}'...")

    create_kwargs = {
        "partition_column": LANDING_PARTITION_COLUMN,
        "partition_type": LANDING_PARTITION_TYPE,
    }
    if LANDING_PARTITION_TYPE == "HASH":
        create_kwargs["partitions"] = LANDING_PARTITIONS

    session.create_table(
        TABLE_NAME,
        LANDING_SCHEMA,
        **create_kwargs,
    )
    logger.info(f"  New table '{TABLE_NAME}' created")

    logger.info(f"Reading {len(df_write)} rows from backup and writing to new table...")
    session.add(TABLE_NAME, df_write)
    final_count = session.count_rows(TABLE_NAME)
    logger.info(f"  Wrote {len(df_write)} rows, verified: {final_count} rows in new table")
    if final_count != len(df_write):
        logger.error(f"Final row count mismatch: expected {len(df_write)}, got {final_count}")
        logger.error(f"Backup table '{BACKUP_TABLE}' is kept for recovery.")
        session.shutdown()
        sys.exit(1)

    # Show new job_id partition distribution
    logger.info("New job_id partition distribution:")
    job_counts = df["job_id"].value_counts()
    for job_id, count in job_counts.items():
        logger.info(f"  {job_id}: {count}")

    # =========================================================================
    # Step 7: Drop backup (optional)
    # =========================================================================
    if drop_backup:
        logger.info(f"Dropping backup table '{BACKUP_TABLE}'...")
        session.drop_table(BACKUP_TABLE)
        logger.info(f"  Backup table '{BACKUP_TABLE}' dropped")
    else:
        logger.info(f"Backup table '{BACKUP_TABLE}' kept on disk (use --drop-backup to remove it)")

    # =========================================================================
    # Summary
    # =========================================================================
    logger.info("=" * 60)
    logger.info("Migration Summary")
    logger.info("=" * 60)
    logger.info(f"  Source dt partitions migrated: {DT_PARTITIONS_TO_MIGRATE}")
    logger.info(f"  Total rows read from source:   {total_rows}")
    logger.info(f"  Rows with null/empty job_id:  {null_or_empty_count}")
    logger.info(f"  Rows written to new table:     {len(df)}")
    logger.info("  Rows outside source dt list:   intentionally discarded")
    logger.info(f"  dataset_type 'Test' -> 'RL':  {test_count} rows transformed")
    logger.info(f"  New table partition key:      job_id")
    logger.info(f"  Final row count:               {final_count}")
    logger.info("=" * 60)

    session.shutdown()
    logger.info("Migration complete!")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate landing_test from dt partition to job_id partition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run — read and validate, don't write anything
  AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_partition.py --dry-run

  # Full migration — backup -> drop old -> create new -> copy from backup
  # Backup table is kept for safety until you verify the result
  AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_partition.py

  # Full migration + drop backup table after completion
  AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_partition.py --drop-backup
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and validate data but don't write anything",
    )
    parser.add_argument(
        "--drop-backup",
        action="store_true",
        help="Drop the backup table after migration completes. "
             "Use only after verifying the new table is correct.",
    )

    args = parser.parse_args()
    migrate_partition(dry_run=args.dry_run, drop_backup=args.drop_backup)


if __name__ == "__main__":
    main()
