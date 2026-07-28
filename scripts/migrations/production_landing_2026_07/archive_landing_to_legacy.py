#!/usr/bin/env python3
"""Archive the legacy production landing table without modifying the source.

The current production ``wind_tunnel_landing`` table is VALUE-partitioned by
``dt``.  This script copies it, partition by partition, into
``wind_tunnel_landing_legacy`` so the original historical rows can be retained
outside the new HASH(job_id) online table.

The archive keeps the source table's current schema and partition metadata
unchanged.  In particular, it deliberately does not add ``is_trainable`` or
invent ``job_id`` values for historical records.

The source table must stay quiescent for the duration of the copy.  The script
verifies each source partition and the total source count again before
declaring success.

Usage:
    # Inspect source metadata and planned copy only.
    AWS_EC2_METADATA_DISABLED=true python scripts/migrations/production_landing_2026_07/archive_landing_to_legacy.py --dry-run

    # Create wind_tunnel_landing_legacy and copy all historical data.
    AWS_EC2_METADATA_DISABLED=true python scripts/migrations/production_landing_2026_07/archive_landing_to_legacy.py

    # Continue only a previously interrupted archive.  Complete partitions are
    # skipped; partially copied partitions cause the script to stop safely.
    AWS_EC2_METADATA_DISABLED=true python scripts/migrations/production_landing_2026_07/archive_landing_to_legacy.py --resume
"""

import argparse
import sys
from typing import Any, Dict, List, Tuple

import dldb
import numpy as np
import pandas as pd
import pyarrow as pa

from wt_sdk.config import default_config


DEFAULT_SOURCE_TABLE = "wind_tunnel_landing"
DEFAULT_ARCHIVE_TABLE = "wind_tunnel_landing_legacy"
DEFAULT_BATCH_SIZE = 10_000


def _pin_exact_dldb_table(session, table_name: str) -> None:
    """Open exactly ``table_name`` rather than a similarly prefixed table."""
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


def _sql_string(value: str) -> str:
    """Quote a dldb SQL string literal used for a partition predicate."""
    return "'" + value.replace("'", "''") + "'"


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
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type) or pa.types.is_struct(field.type):
            normalized[field.name] = normalized[field.name].map(_pythonize_nested_value)

    arrow_table = pa.Table.from_pandas(normalized, schema=schema, preserve_index=False)
    return arrow_table.to_pandas(types_mapper=pd.ArrowDtype)


def _partition_metadata(record) -> Dict[str, Any]:
    return {
        "partition_column": record.partition_column,
        "partition_type": record.partition_type,
        "partitions": record.partitions,
    }


def _assert_matching_archive_metadata(source_record, archive_record, source_schema: pa.Schema, archive_schema: pa.Schema) -> None:
    if _partition_metadata(source_record) != _partition_metadata(archive_record):
        raise ValueError(
            "Existing archive partition metadata differs from source: "
            f"source={_partition_metadata(source_record)}, archive={_partition_metadata(archive_record)}"
        )
    if source_schema != archive_schema:
        raise ValueError("Existing archive schema differs from source schema")


def _list_partitions(session, table_name: str) -> List[str]:
    _pin_exact_dldb_table(session, table_name)
    partitions = session.tables[table_name].list_partitions()
    return sorted(partitions)


def _copy_partition(
    session,
    source_table: str,
    archive_table: str,
    partition_column: str,
    partition_value: str,
    schema: pa.Schema,
    batch_size: int,
) -> int:
    """Copy one VALUE partition in stable offsets while the source is quiescent."""
    partition_cond = f"{partition_column} = {_sql_string(partition_value)}"
    copied = 0
    offset = 0

    while True:
        batch = session.filter(
            source_table,
            query="",
            limit=batch_size,
            offset=offset,
            partition_cond=partition_cond,
        )
        if batch is None or batch.empty:
            break

        if len(batch) > batch_size:
            raise RuntimeError(f"dldb returned {len(batch)} rows, above batch size {batch_size}")
        if set(batch[partition_column].dropna().unique()) != {partition_value}:
            raise RuntimeError(f"Source batch unexpectedly contains rows outside partition {partition_value!r}")

        session.add(archive_table, _to_dldb_write_frame(batch, schema), partition=partition_value)
        copied += len(batch)
        offset += len(batch)
        print(f"  {partition_value}: copied {copied} rows", flush=True)

    return copied


def archive_landing(
    *,
    source_table: str,
    archive_table: str,
    batch_size: int,
    dry_run: bool,
    resume: bool,
) -> None:
    if source_table == archive_table:
        raise ValueError("Source table and archive table must be different")
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )
    try:
        if not session.table_exists(source_table):
            raise ValueError(f"Source table '{source_table}' does not exist")

        _pin_exact_dldb_table(session, source_table)
        source_record = session.schema_table.get(source_table)
        source_schema = session.get_schema(source_table)
        if str(source_record.partition_type).upper() != "VALUE":
            raise ValueError(
                f"This archival script expects a VALUE-partitioned source, got {source_record.partition_type!r}"
            )
        if not source_record.partition_column:
            raise ValueError("Source metadata has no partition column")

        source_partitions = _list_partitions(session, source_table)
        source_counts: List[Tuple[str, int]] = [
            (partition, session.count_rows(source_table, partition=partition))
            for partition in source_partitions
        ]
        source_total = sum(count for _, count in source_counts)

        print("=" * 72)
        print("Legacy landing archive plan")
        print("=" * 72)
        print(f"Source table:       {source_table}")
        print(f"Archive table:      {archive_table}")
        print(f"Source schema:      {len(source_schema)} fields (preserved unchanged)")
        print(f"Partition:          VALUE({source_record.partition_column})")
        print(f"Source partitions:  {len(source_partitions)}")
        print(f"Source row count:   {source_total}")
        print(f"Batch size:         {batch_size}")
        print("Indexes:            not created for the archive")
        print("=" * 72)
        for partition, count in source_counts:
            print(f"  {partition}: {count}")

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
                    f"Archive table '{archive_table}' already exists. Refusing to append to it. "
                    "Use --resume only for a previous run of this script."
                )
            _pin_exact_dldb_table(session, archive_table)
            archive_record = session.schema_table.get(archive_table)
            archive_schema = session.get_schema(archive_table)
            _assert_matching_archive_metadata(source_record, archive_record, source_schema, archive_schema)
        else:
            session.create_table(
                archive_table,
                source_schema,
                partition_column=source_record.partition_column,
                partition_type=source_record.partition_type,
            )
            _pin_exact_dldb_table(session, archive_table)
            print(f"Created archive table '{archive_table}'.")

        for partition, expected_count in source_counts:
            archive_count = 0
            archive_partitions = set(_list_partitions(session, archive_table))
            if partition in archive_partitions:
                archive_count = session.count_rows(archive_table, partition=partition)

            if archive_count == expected_count:
                print(f"Partition {partition}: already complete ({archive_count} rows), skipping.")
                continue
            if archive_count:
                raise RuntimeError(
                    f"Archive partition {partition!r} is partial: {archive_count}/{expected_count} rows. "
                    "Refusing to append and risk duplicates. Restore/delete that archive partition manually, "
                    "then run --resume."
                )

            print(f"Copying partition {partition}: {expected_count} rows...")
            copied = _copy_partition(
                session,
                source_table,
                archive_table,
                source_record.partition_column,
                partition,
                source_schema,
                batch_size,
            )
            archive_count = session.count_rows(archive_table, partition=partition)
            if copied != expected_count or archive_count != expected_count:
                raise RuntimeError(
                    f"Partition {partition!r} verification failed: "
                    f"source={expected_count}, copied={copied}, archive={archive_count}"
                )
            print(f"Partition {partition}: verified {archive_count} rows.")

        source_total_after = session.count_rows(source_table)
        archive_total = session.count_rows(archive_table)
        if source_total_after != source_total:
            raise RuntimeError(
                "Source changed while the archive was running: "
                f"before={source_total}, after={source_total_after}. Archive is not declared complete."
            )
        if archive_total != source_total:
            raise RuntimeError(
                f"Archive total verification failed: source={source_total}, archive={archive_total}"
            )

        print("=" * 72)
        print(f"Archive complete: {archive_table} contains {archive_total} rows.")
        print("The source table was read only; no source data or metadata was modified.")
        print("=" * 72)
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Copy the legacy VALUE(dt) landing table into a read-only archive table.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--archive-table", default=DEFAULT_ARCHIVE_TABLE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="Inspect only; do not create or write the archive.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue only complete/empty archive partitions from a prior interrupted run.",
    )
    args = parser.parse_args()

    try:
        archive_landing(
            source_table=args.source_table,
            archive_table=args.archive_table,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            resume=args.resume,
        )
    except Exception as exc:
        print(f"Archive failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
