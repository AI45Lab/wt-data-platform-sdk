#!/usr/bin/env python3
"""Create a dated archive table and copy production landing data into it.

This script implements the non-destructive half of the periodic landing archive
workflow:

1. Create ``archived_YYYYMMDD_wind_tunnel_landing`` with the same schema and
   HASH partition metadata as ``wind_tunnel_landing``.
2. Copy all existing HASH buckets into the archive table in bounded batches.
3. Verify schema, partition metadata, per-bucket counts, total row count, and
   that the source table did not change while the copy was running.

The source table must be quiescent. Stop writers and ETL before running the
real copy. The script never modifies ``wind_tunnel_landing``.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from typing import Any, Dict, List, Tuple

import dldb
import numpy as np
import pandas as pd
import pyarrow as pa

from wt_sdk.config import DEFAULT_LANDING_TABLE, default_config
from wt_sdk.core.schemas import (
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_PARTITIONS,
    LANDING_SCHEMA,
)


DEFAULT_BATCH_SIZE = 10_000
ALL_ROWS_QUERY = "id IS NOT NULL"


def _default_archive_table(today: str | None = None) -> str:
    date_str = today or datetime.now().strftime("%Y%m%d")
    return f"archived_{date_str}_{DEFAULT_LANDING_TABLE}"


def _partition_metadata(record) -> Dict[str, Any]:
    return {
        "partition_column": record.partition_column,
        "partition_type": record.partition_type,
        "partitions": record.partitions,
    }


def _expected_landing_metadata() -> Dict[str, Any]:
    return {
        "partition_column": LANDING_PARTITION_COLUMN,
        "partition_type": LANDING_PARTITION_TYPE,
        "partitions": LANDING_PARTITIONS,
    }


def _assert_landing_hash_metadata(table_name: str, record) -> None:
    actual = _partition_metadata(record)
    expected = _expected_landing_metadata()
    if actual != expected:
        raise ValueError(
            f"Table '{table_name}' is not the expected active landing layout: "
            f"expected={expected}, actual={actual}"
        )


def _assert_matching_metadata_and_schema(
    *,
    source_record,
    archive_record,
    source_schema: pa.Schema,
    archive_schema: pa.Schema,
) -> None:
    if _partition_metadata(source_record) != _partition_metadata(archive_record):
        raise ValueError(
            "Archive partition metadata differs from source: "
            f"source={_partition_metadata(source_record)}, "
            f"archive={_partition_metadata(archive_record)}"
        )
    if source_schema != archive_schema:
        raise ValueError("Archive schema differs from source schema")


def _list_partitions(session, table_name: str) -> List[int]:
    partitions = session._get_table(table_name).list_partitions()
    return sorted(int(partition) for partition in partitions)


def _pythonize_nested_value(value: Any) -> Any:
    """Convert NumPy nested values returned by dldb back to Python containers."""
    if isinstance(value, np.ndarray):
        return [_pythonize_nested_value(item) for item in value.tolist()]
    if isinstance(value, dict):
        return {key: _pythonize_nested_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_pythonize_nested_value(item) for item in value]
    return value


def _to_dldb_write_frame(df: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    """Return an Arrow-backed frame preserving the source schema exactly."""
    column_names = schema.names
    missing_columns = [column for column in column_names if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Source batch is missing schema columns: {missing_columns}")

    normalized = df.loc[:, column_names].copy()
    for field in schema:
        if (
            pa.types.is_list(field.type)
            or pa.types.is_large_list(field.type)
            or pa.types.is_struct(field.type)
        ):
            normalized[field.name] = normalized[field.name].map(_pythonize_nested_value)

    arrow_table = pa.Table.from_pandas(normalized, schema=schema, preserve_index=False)
    return arrow_table.to_pandas(types_mapper=pd.ArrowDtype)


def _copy_hash_partition(
    *,
    session,
    source_table: str,
    archive_table: str,
    partition: int,
    expected_count: int,
    schema: pa.Schema,
    batch_size: int,
) -> int:
    """Copy one HASH bucket in stable offsets while the source is quiescent."""
    copied = 0
    offset = 0

    while True:
        batch = session.filter(
            source_table,
            query=ALL_ROWS_QUERY,
            limit=batch_size,
            offset=offset,
            partitions=[partition],
        )
        if batch is None or batch.empty:
            break

        if len(batch) > batch_size:
            raise RuntimeError(f"dldb returned {len(batch)} rows, above batch size {batch_size}")

        session.add(
            archive_table,
            _to_dldb_write_frame(batch, schema),
            partition=partition,
        )
        copied += len(batch)
        offset += len(batch)
        print(f"  bucket {partition}: copied {copied}/{expected_count} rows", flush=True)

    return copied


def create_archive_and_copy(
    *,
    source_table: str,
    archive_table: str,
    batch_size: int,
    dry_run: bool,
    resume: bool,
) -> None:
    if source_table != DEFAULT_LANDING_TABLE:
        raise ValueError(
            f"This production archive script only supports {DEFAULT_LANDING_TABLE!r}; "
            f"got {source_table!r}"
        )
    if source_table == archive_table:
        raise ValueError("Source table and archive table must be different")
    if archive_table.startswith(source_table):
        raise ValueError(
            "Archive table name must not start with the source table name; "
            "use archived_YYYYMMDD_wind_tunnel_landing to avoid dldb prefix confusion."
        )
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )
    try:
        if not session.table_exists(source_table):
            raise ValueError(f"Source table '{source_table}' does not exist")

        source_record = session.schema_table.get(source_table)
        source_schema = session.get_schema(source_table)
        _assert_landing_hash_metadata(source_table, source_record)
        if source_schema != LANDING_SCHEMA:
            raise ValueError("Source table schema differs from current LANDING_SCHEMA")

        source_partitions = _list_partitions(session, source_table)
        source_counts: List[Tuple[int, int]] = [
            (partition, session.count_rows(source_table, partition=partition))
            for partition in source_partitions
        ]
        source_total = sum(count for _, count in source_counts)

        print("=" * 80)
        print("Production landing archive copy plan")
        print("=" * 80)
        print(f"Database:           {default_config.tables.db_uri}")
        print(f"Source table:       {source_table}")
        print(f"Archive table:      {archive_table}")
        print(f"Schema fields:      {len(source_schema)}")
        print(
            f"Partition:          {source_record.partition_type}"
            f"({source_record.partition_column}), buckets={source_record.partitions}"
        )
        print(f"Existing buckets:   {len(source_partitions)}")
        print(f"Source row count:   {source_total}")
        print(f"Batch size:         {batch_size}")
        print("Source modified:    never")
        print("=" * 80)
        for partition, count in source_counts:
            print(f"  bucket {partition}: {count}")

        archive_exists = session.table_exists(archive_table)
        if dry_run:
            if archive_exists:
                print(f"[DRY RUN] Archive table '{archive_table}' already exists; no writes made.")
            else:
                print(f"[DRY RUN] Would create '{archive_table}' and copy {source_total} rows.")
            return

        if archive_exists:
            if not resume:
                raise ValueError(
                    f"Archive table '{archive_table}' already exists. Refusing to append. "
                    "Use --resume only for a previous incomplete run of this script."
                )
            archive_record = session.schema_table.get(archive_table)
            archive_schema = session.get_schema(archive_table)
            _assert_matching_metadata_and_schema(
                source_record=source_record,
                archive_record=archive_record,
                source_schema=source_schema,
                archive_schema=archive_schema,
            )
        else:
            session.create_table(
                archive_table,
                source_schema,
                partition_column=source_record.partition_column,
                partition_type=source_record.partition_type,
                partitions=source_record.partitions,
            )
            print(f"Created archive table '{archive_table}'.")

        for partition, expected_count in source_counts:
            archive_partitions = set(_list_partitions(session, archive_table))
            archive_count = 0
            if partition in archive_partitions:
                archive_count = session.count_rows(archive_table, partition=partition)

            if archive_count == expected_count:
                print(f"bucket {partition}: already complete ({archive_count} rows), skipping.")
                continue
            if archive_count:
                raise RuntimeError(
                    f"Archive bucket {partition} is partial: {archive_count}/{expected_count} rows. "
                    "Refusing to append and risk duplicates. Recreate the archive table or "
                    "manually remove the partial bucket before --resume."
                )

            print(f"Copying bucket {partition}: {expected_count} rows...")
            copied = _copy_hash_partition(
                session=session,
                source_table=source_table,
                archive_table=archive_table,
                partition=partition,
                expected_count=expected_count,
                schema=source_schema,
                batch_size=batch_size,
            )
            archive_count = session.count_rows(archive_table, partition=partition)
            if copied != expected_count or archive_count != expected_count:
                raise RuntimeError(
                    f"Bucket {partition} verification failed: "
                    f"source={expected_count}, copied={copied}, archive={archive_count}"
                )
            print(f"bucket {partition}: verified {archive_count} rows.")

        source_total_after = session.count_rows(source_table)
        archive_total = session.count_rows(archive_table)
        archive_record = session.schema_table.get(archive_table)
        archive_schema = session.get_schema(archive_table)
        _assert_matching_metadata_and_schema(
            source_record=source_record,
            archive_record=archive_record,
            source_schema=source_schema,
            archive_schema=archive_schema,
        )
        if source_total_after != source_total:
            raise RuntimeError(
                "Source changed while the archive copy was running: "
                f"before={source_total}, after={source_total_after}. "
                "Archive is not declared complete."
            )
        if archive_total != source_total:
            raise RuntimeError(
                f"Archive total verification failed: source={source_total}, archive={archive_total}"
            )

        print("=" * 80)
        print("Archive copy complete")
        print(f"{archive_table}: {archive_total} rows")
        print(f"{source_table}: unchanged, {source_total_after} rows")
        print("Next step: run scripts/ops/drop_recreate_table.py after reviewing this result.")
        print("=" * 80)
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create archived_YYYYMMDD_wind_tunnel_landing and copy production landing data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--source-table", default=DEFAULT_LANDING_TABLE)
    parser.add_argument("--archive-table", default=_default_archive_table())
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Inspect only; do not create or write archive data.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume only if existing archive buckets are complete or empty.",
    )
    args = parser.parse_args()

    try:
        create_archive_and_copy(
            source_table=args.source_table,
            archive_table=args.archive_table,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            resume=args.resume,
        )
    except Exception as exc:
        print(f"Archive copy failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
