#!/usr/bin/env python3
"""Locate HASH buckets and candidate rows that fail nested-column decoding."""

import argparse
from typing import Dict, Iterable, List

import dldb

from wt_sdk.config import default_config


IDENTITY_COLUMNS = ["id", "job_id", "session_id", "created_at"]
NESTED_COLUMNS = [
    "messages",
    "response",
    "chosen_trace",
    "rejected_trace",
    "blob_manifest",
]


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _probe_ids(session, table_name: str, bucket: int, column: str, rows: List[Dict]) -> List[Dict]:
    """Bisect IDs until a failing nested read cannot be narrowed further."""
    if not rows:
        return []

    ids = [str(row["id"]) for row in rows]
    query = "id IN (" + ", ".join(_sql_literal(record_id) for record_id in ids) + ")"
    try:
        session.filter(
            table_name,
            query=query,
            columns=["id", column],
            partitions=[bucket],
        )
        return []
    except Exception:
        if len(rows) == 1:
            return rows
        midpoint = len(rows) // 2
        return _probe_ids(session, table_name, bucket, column, rows[:midpoint]) + _probe_ids(
            session,
            table_name,
            bucket,
            column,
            rows[midpoint:],
        )


def _batches(rows: List[Dict], size: int) -> Iterable[List[Dict]]:
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def scan_table(table_name: str, db_uri: str, batch_size: int, max_output: int) -> int:
    session = dldb.connect(db_uri, storage_options=default_config.s3.to_storage_options())
    try:
        record = session.schema_table.get(table_name)
        if record is None:
            raise ValueError(f"Logical table not found: {table_name}")
        if str(record.partition_type).upper() != "HASH":
            raise ValueError(f"{table_name} is {record.partition_type}, not a HASH table")

        table = session._get_table(table_name)
        buckets = table.list_partitions()
        print(f"Scanning {table_name}: {len(buckets)} existing HASH bucket(s)")

        failures = 0
        for bucket in buckets:
            try:
                identity_df = session.filter(
                    table_name,
                    query="id IS NOT NULL",
                    columns=IDENTITY_COLUMNS,
                    partitions=[bucket],
                )
            except Exception as exc:
                failures += 1
                print(f"Bucket {bucket}: cannot read identity columns: {exc}")
                continue

            rows = identity_df.to_dict("records")
            for column in NESTED_COLUMNS:
                try:
                    session.filter(
                        table_name,
                        query="id IS NOT NULL",
                        columns=["id", column],
                        partitions=[bucket],
                    )
                    continue
                except Exception as exc:
                    failures += 1
                    candidates: List[Dict] = []
                    for batch in _batches(rows, batch_size):
                        candidates.extend(_probe_ids(session, table_name, bucket, column, batch))

                    print(
                        f"Bucket {bucket}, column {column}: decode failed "
                        f"({len(candidates)} candidate record(s)); {exc}"
                    )
                    for candidate in candidates[:max_output]:
                        print(
                            "  "
                            + ", ".join(f"{key}={candidate.get(key)!r}" for key in IDENTITY_COLUMNS)
                        )
                    if len(candidates) > max_output:
                        print(f"  ... {len(candidates) - max_output} more candidate record(s)")

        if failures == 0:
            print("No nested-column decode failures found.")
            return 0
        print("Candidates identify unreadable row groups; a corrupt Lance fragment can implicate multiple rows.")
        return 1
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan a HASH landing table for nested decode failures")
    parser.add_argument("--table", default="v2_landing_test", help="HASH landing table to scan")
    parser.add_argument("--db-uri", default=None, help="Database URI (default: WT_SDK_DB_URI)")
    parser.add_argument("--batch-size", type=int, default=64, help="IDs per narrowing probe")
    parser.add_argument("--max-output", type=int, default=100, help="Maximum candidates to print per failure")
    args = parser.parse_args()
    return scan_table(
        args.table,
        args.db_uri or default_config.tables.db_uri,
        args.batch_size,
        args.max_output,
    )


if __name__ == "__main__":
    raise SystemExit(main())
