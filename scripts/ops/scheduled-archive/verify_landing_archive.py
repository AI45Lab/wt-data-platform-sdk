#!/usr/bin/env python3
"""Verify a production landing archive without modifying either table."""

from __future__ import annotations

import argparse
import hashlib
import sys
from typing import Any

import dldb
import pandas as pd

from wt_sdk.config import DEFAULT_LANDING_TABLE, default_config
from wt_sdk.core.schemas import (
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_PARTITIONS,
    LANDING_SCHEMA,
)


DEFAULT_ARCHIVE_TABLE = "archived_20260911_wind_tunnel_landing"
DEFAULT_BATCH_SIZE = 5_000
ID_QUERY = "id IS NOT NULL"
MODULUS = 1 << 128


def _metadata(record: Any) -> dict[str, Any]:
    return {
        "partition_column": record.partition_column,
        "partition_type": record.partition_type,
        "partitions": record.partitions,
    }


def _expected_metadata() -> dict[str, Any]:
    return {
        "partition_column": LANDING_PARTITION_COLUMN,
        "partition_type": LANDING_PARTITION_TYPE,
        "partitions": LANDING_PARTITIONS,
    }


def _validate_layout(session: Any, table_name: str):
    if not session.table_exists(table_name):
        raise ValueError(f"Required table does not exist: {table_name}")
    record = session.schema_table.get(table_name)
    if _metadata(record) != _expected_metadata():
        raise ValueError(
            f"{table_name}: expected {_expected_metadata()}, got {_metadata(record)}"
        )
    schema = session.get_schema(table_name)
    if schema != LANDING_SCHEMA:
        raise ValueError(f"{table_name}: schema differs from LANDING_SCHEMA")
    return record, schema


def _partitions(session: Any, table_name: str) -> set[int]:
    return {
        int(value)
        for value in session._get_table(table_name).list_partitions()
    }


def _id_digest(session: Any, table_name: str, partition: int, batch_size: int):
    """Stream IDs and return exact count plus order-independent 256-bit digest.

    The pair (xor, sum) is independent of row order, unlike a plain SHA over
    query output. Duplicate IDs are detected separately with an in-memory set
    only for one bucket's ID values; no full rows are loaded.
    """
    offset = 0
    count = 0
    xor_digest = 0
    sum_digest = 0
    seen: set[str] = set()
    while True:
        frame = session.filter(
            table_name,
            query=ID_QUERY,
            columns=["id"],
            limit=batch_size,
            offset=offset,
            partitions=[partition],
            checkout_latest=True,
        )
        if frame is None or frame.empty:
            break
        for value in frame["id"].tolist():
            if value is None or pd.isna(value):
                raise ValueError(f"{table_name} bucket {partition} contains a null id")
            record_id = str(value)
            if record_id in seen:
                raise ValueError(f"{table_name} bucket {partition} contains duplicate id {record_id!r}")
            seen.add(record_id)
            digest = int.from_bytes(
                hashlib.blake2b(record_id.encode("utf-8"), digest_size=16).digest(),
                "big",
            )
            xor_digest ^= digest
            sum_digest = (sum_digest + digest) % MODULUS
            count += 1
        offset += len(frame)
    return count, xor_digest, sum_digest


def verify(source_table: str, archive_table: str, batch_size: int) -> None:
    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )
    try:
        source_record, source_schema = _validate_layout(session, source_table)
        archive_record, archive_schema = _validate_layout(session, archive_table)
        if source_schema != archive_schema:
            raise ValueError("Source and archive schemas differ")
        if _metadata(source_record) != _metadata(archive_record):
            raise ValueError("Source and archive partition metadata differ")

        source_partitions = _partitions(session, source_table)
        archive_partitions = _partitions(session, archive_table)
        all_partitions = sorted(source_partitions | archive_partitions)
        print(f"Database: {default_config.tables.db_uri}")
        print(f"Source: {source_table}")
        print(f"Archive: {archive_table}")
        print(f"Schema: identical ({len(source_schema)} fields)")
        print(f"Partition metadata: identical HASH(job_id), {LANDING_PARTITIONS} buckets")
        print(f"Materialized buckets: source={len(source_partitions)}, archive={len(archive_partitions)}")

        total_source = total_archive = 0
        failures: list[str] = []
        for partition in all_partitions:
            source_count, source_xor, source_sum = _id_digest(
                session, source_table, partition, batch_size
            ) if partition in source_partitions else (0, 0, 0)
            archive_count, archive_xor, archive_sum = _id_digest(
                session, archive_table, partition, batch_size
            ) if partition in archive_partitions else (0, 0, 0)
            total_source += source_count
            total_archive += archive_count
            same = (
                source_count == archive_count
                and source_xor == archive_xor
                and source_sum == archive_sum
            )
            print(
                f"bucket {partition}: source={source_count}, archive={archive_count}, "
                f"id_checksum={'OK' if same else 'MISMATCH'}",
                flush=True,
            )
            if not same:
                failures.append(str(partition))

        print(f"Total rows: source={total_source}, archive={total_archive}")
        if total_source != total_archive:
            failures.append("total-row-count")
        if failures:
            raise RuntimeError("Archive verification failed for: " + ", ".join(failures))
        print("Archive verification passed; neither table was modified.")
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify schema, HASH metadata, bucket counts, and streaming ID checksums.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--source-table", default=DEFAULT_LANDING_TABLE)
    parser.add_argument("--archive-table", default=DEFAULT_ARCHIVE_TABLE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero")
    try:
        verify(args.source_table, args.archive_table, args.batch_size)
    except Exception as exc:
        print(f"Archive verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
