#!/usr/bin/env python3
"""
Rebuild landing_test as a HASH-partitioned table on job_id and restore data from backup.

This script is for the safe test table only:
1. Read all rows from landing_test_backup.
2. Validate that rows can be partitioned by job_id.
3. Create a staging HASH table and write the backup data there first.
4. Drop/recreate landing_test as HASH(job_id).
5. Write the same data into the new landing_test.

No indexes are created here. Run the index helper separately if needed.

Usage:
    AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_to_hash_partition.py --dry-run
    AWS_EC2_METADATA_DISABLED=true python tests/migrate_landing_test_to_hash_partition.py
"""
import argparse
import sys
from typing import Optional

import dldb
import numpy as np
import pandas as pd
import pyarrow as pa
from loguru import logger

from wt_sdk.config import default_config
from wt_sdk.core.schemas import (
    LANDING_PARTITIONS,
    LANDING_PARTITION_COLUMN,
    LANDING_SCHEMA,
)


TABLE_NAME = "landing_test"
BACKUP_TABLE = "landing_test_backup"
STAGING_TABLE = "hash_migration_landing_test_tmp"

NESTED_COLUMNS = [
    "messages",
    "response",
    "chosen_response",
    "rejected_response",
    "blob_manifest",
]


logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    level="INFO",
)


def _is_null_or_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _pythonize_nested_value(value):
    if isinstance(value, np.ndarray):
        return [_pythonize_nested_value(item) for item in value.tolist()]
    if isinstance(value, dict):
        return {key: _pythonize_nested_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_pythonize_nested_value(item) for item in value]
    return value


def _to_dldb_write_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return an Arrow-backed DataFrame matching LANDING_SCHEMA for dldb.add()."""
    schema_columns = [field.name for field in LANDING_SCHEMA]
    missing_columns = [column for column in schema_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Backup DataFrame is missing LANDING_SCHEMA columns: {missing_columns}")

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


def _pin_exact_dldb_table(session, table_name: str) -> None:
    """
    Pin a logical table into the dldb session by metadata.

    Current dldb opens disk tables with a broad startswith(table_name) scan. Because
    landing_test_backup also starts with landing_test, this avoids accidentally
    opening the backup when we intend to operate on landing_test.
    """
    from dldb.table import open_table_by_partition_type

    record = session.schema_table.get(table_name)
    if record is None:
        raise ValueError(f"Table '{table_name}' does not exist in dldb information_schema")
    session.tables[table_name] = open_table_by_partition_type(
        session.db_conn,
        session.schema_table,
        table_name,
        record.partition_type,
    )


def _drop_table_exact(session, table_name: str) -> None:
    if not session.table_exists(table_name):
        return
    _pin_exact_dldb_table(session, table_name)
    session.drop_table(table_name)


def _create_landing_hash_table(session, table_name: str, hash_buckets: int) -> None:
    session.create_table(
        table_name,
        LANDING_SCHEMA,
        partition_column=LANDING_PARTITION_COLUMN,
        partition_type="HASH",
        partitions=hash_buckets,
    )


def _read_backup(session, backup_table: str) -> pd.DataFrame:
    if not session.table_exists(backup_table):
        raise ValueError(f"Backup table '{backup_table}' does not exist")

    _pin_exact_dldb_table(session, backup_table)
    backup_count = session.count_rows(backup_table)
    if backup_count <= 0:
        raise ValueError(f"Backup table '{backup_table}' is empty")

    logger.info(f"Reading {backup_count} rows from '{backup_table}'...")
    df = session.filter(backup_table, query="id IS NOT NULL", limit=None)
    logger.info(f"Read {len(df)} rows from '{backup_table}'")
    if len(df) != backup_count:
        logger.warning(f"Read row count differs from count_rows: read={len(df)}, count_rows={backup_count}")
    return df


def _validate_and_transform(df: pd.DataFrame, drop_invalid_job_id: bool) -> pd.DataFrame:
    null_mask = df[LANDING_PARTITION_COLUMN].apply(_is_null_or_empty)
    null_count = int(null_mask.sum())
    logger.info(f"Rows with null/empty {LANDING_PARTITION_COLUMN}: {null_count}")

    if null_count > 0:
        sample = df.loc[null_mask, ["id", "dt", "dataset_type", LANDING_PARTITION_COLUMN, "env_id"]].head(20)
        for _, row in sample.iterrows():
            logger.warning(
                f"Invalid partition key row: id={row['id']}, dt={row['dt']}, "
                f"dataset_type={row['dataset_type']}, {LANDING_PARTITION_COLUMN}={repr(row[LANDING_PARTITION_COLUMN])}, "
                f"env_id={row['env_id']}"
            )
        if not drop_invalid_job_id:
            raise ValueError(
                f"Found {null_count} rows with null/empty {LANDING_PARTITION_COLUMN}. "
                "Re-run with --drop-invalid-job-id only if discarding them is acceptable."
            )
        df = df.loc[~null_mask].copy()
        logger.warning(f"Dropped {null_count} rows with invalid {LANDING_PARTITION_COLUMN}")

    test_count = int((df["dataset_type"] == "Test").sum())
    if test_count:
        logger.info(f"Converting dataset_type='Test' to 'RL' for {test_count} rows")
        df = df.copy()
        df["dataset_type"] = df["dataset_type"].replace("Test", "RL")

    return df


def migrate(
    *,
    backup_table: str = BACKUP_TABLE,
    hash_buckets: int = LANDING_PARTITIONS,
    dry_run: bool = False,
    staging_table: str = STAGING_TABLE,
    keep_staging: bool = False,
    drop_invalid_job_id: bool = False,
) -> None:
    logger.info(f"Connecting to {default_config.tables.db_uri}...")
    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )

    try:
        df = _read_backup(session, backup_table)
        df = _validate_and_transform(df, drop_invalid_job_id=drop_invalid_job_id)
        df_write = _to_dldb_write_frame(df)

        logger.info("=" * 60)
        logger.info("Planned landing_test HASH rebuild")
        logger.info("=" * 60)
        logger.info(f"Backup table:      {backup_table}")
        logger.info(f"Target table:      {TABLE_NAME}")
        logger.info(f"Partition column:  {LANDING_PARTITION_COLUMN}")
        logger.info(f"Partition type:    HASH")
        logger.info(f"Hash buckets:      {hash_buckets}")
        logger.info(f"Rows to restore:   {len(df_write)}")
        logger.info("Indexes:           not created by this script")
        logger.info("=" * 60)

        if dry_run:
            logger.info("[DRY RUN] No table changes were made")
            return

        if session.table_exists(staging_table):
            logger.info(f"Dropping stale staging table '{staging_table}'...")
            _drop_table_exact(session, staging_table)

        logger.info(f"Creating staging table '{staging_table}' as HASH({LANDING_PARTITION_COLUMN})...")
        _create_landing_hash_table(session, staging_table, hash_buckets)
        logger.info(f"Writing {len(df_write)} rows to staging table...")
        session.add(staging_table, df_write)
        staging_count = session.count_rows(staging_table)
        if staging_count != len(df_write):
            raise RuntimeError(f"Staging row count mismatch: expected={len(df_write)}, got={staging_count}")
        logger.info(f"Staging verification passed: {staging_count} rows")

        logger.info(f"Dropping existing '{TABLE_NAME}'...")
        _drop_table_exact(session, TABLE_NAME)

        logger.info(f"Creating new '{TABLE_NAME}' as HASH({LANDING_PARTITION_COLUMN})...")
        _create_landing_hash_table(session, TABLE_NAME, hash_buckets)
        logger.info(f"Restoring {len(df_write)} rows into '{TABLE_NAME}'...")
        session.add(TABLE_NAME, df_write)
        final_count = session.count_rows(TABLE_NAME)
        if final_count != len(df_write):
            raise RuntimeError(f"Final row count mismatch: expected={len(df_write)}, got={final_count}")

        logger.info(f"Final verification passed: {final_count} rows")

        if keep_staging:
            logger.info(f"Keeping staging table '{staging_table}' for manual verification")
        else:
            logger.info(f"Dropping staging table '{staging_table}'...")
            _drop_table_exact(session, staging_table)

        logger.info("landing_test HASH rebuild complete")
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild landing_test as HASH(job_id) from landing_test_backup",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--backup-table", default=BACKUP_TABLE)
    parser.add_argument("--hash-buckets", type=int, default=LANDING_PARTITIONS)
    parser.add_argument("--staging-table", default=STAGING_TABLE)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--keep-staging", action="store_true")
    parser.add_argument(
        "--drop-invalid-job-id",
        action="store_true",
        help="Drop rows whose job_id is null/empty instead of aborting.",
    )
    args = parser.parse_args()

    migrate(
        backup_table=args.backup_table,
        hash_buckets=args.hash_buckets,
        dry_run=args.dry_run,
        staging_table=args.staging_table,
        keep_staging=args.keep_staging,
        drop_invalid_job_id=args.drop_invalid_job_id,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
