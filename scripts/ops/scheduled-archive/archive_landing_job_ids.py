#!/usr/bin/env python3
"""Copy selected production landing jobs into an archive table through wt-sdk.

The source table is read-only. Jobs are processed one at a time and rows are
read/written in bounded batches. The archive can therefore be resumed safely:
rows whose IDs are already present in the archive are skipped.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Iterable

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import DEFAULT_LANDING_TABLE, GatewayConfig, TableConfig, default_config
from wt_sdk.core.schemas import LANDING_SCHEMA
from wt_sdk.utils import dataframe_to_landing_records


DEFAULT_ARCHIVE_TABLE = "archived_20260911_wind_tunnel_landing"
DEFAULT_BATCH_SIZE = 1_000


def quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def exact_job_filter(job_id: str) -> str:
    return f"job_id = {quote_sql_string(job_id)}"


def _job_id_batches(
    client: WTGatewayClient,
    *,
    table_name: str,
    job_id: str,
    batch_size: int,
    columns: list[str],
) -> Iterable[Any]:
    """Yield SDK DataFrame batches for one exact job ID."""
    yield from client.export_data_batches(
        filter_query=exact_job_filter(job_id),
        batch_size=batch_size,
        columns=columns,
        table=table_name,
    )


def _collect_ids(
    client: WTGatewayClient,
    *,
    table_name: str,
    job_id: str,
    batch_size: int,
) -> list[str]:
    ids: list[str] = []
    for frame in _job_id_batches(
        client,
        table_name=table_name,
        job_id=job_id,
        batch_size=batch_size,
        columns=["id"],
    ):
        ids.extend(str(value) for value in frame["id"].tolist() if value is not None)
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate id values found in {table_name!r} for job_id={job_id!r}")
    return ids


def copy_job(
    source_client: WTGatewayClient,
    archive_client: WTGatewayClient,
    *,
    source_table: str,
    archive_table: str,
    job_id: str,
    batch_size: int,
    execute: bool,
) -> tuple[int, int]:
    source_ids = _collect_ids(
        source_client,
        table_name=source_table,
        job_id=job_id,
        batch_size=batch_size,
    )
    archive_ids = set(
        _collect_ids(
            archive_client,
            table_name=archive_table,
            job_id=job_id,
            batch_size=batch_size,
        )
    )
    missing_ids = [record_id for record_id in source_ids if record_id not in archive_ids]
    print(
        f"{job_id}: source_rows={len(source_ids)}, "
        f"already_archived={len(archive_ids)}, missing={len(missing_ids)}",
        flush=True,
    )
    if not execute or not missing_ids:
        return len(source_ids), 0

    wanted = set(missing_ids)
    copied = 0
    for frame in _job_id_batches(
        source_client,
        table_name=source_table,
        job_id=job_id,
        batch_size=batch_size,
        columns=LANDING_SCHEMA.names,
    ):
        selected = frame[frame["id"].astype(str).isin(wanted)]
        if selected.empty:
            continue
        records = dataframe_to_landing_records(selected)
        archive_client.ingest_landing_batch(records)
        copied += len(records)
        print(
            f"  {job_id}: copied {copied}/{len(missing_ids)} rows",
            flush=True,
        )

    final_ids = set(
        _collect_ids(
            archive_client,
            table_name=archive_table,
            job_id=job_id,
            batch_size=batch_size,
        )
    )
    source_id_set = set(source_ids)
    if final_ids != source_id_set:
        missing = sorted(source_id_set - final_ids)[:5]
        unexpected = sorted(final_ids - source_id_set)[:5]
        raise RuntimeError(
            f"Post-copy ID verification failed for {job_id!r}; "
            f"missing={missing}, unexpected={unexpected}"
        )
    return len(source_ids), copied


def _read_job_ids(args: argparse.Namespace) -> list[str]:
    values = list(args.job_id or [])
    if args.job_id_file:
        with open(args.job_id_file, encoding="utf-8") as handle:
            values.extend(line.strip() for line in handle if line.strip())
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = value.strip()
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    if not result:
        raise ValueError("Provide at least one --job-id or --job-id-file")
    return result


def _client_for_table(table_name: str) -> WTGatewayClient:
    return WTGatewayClient(
        GatewayConfig(
            s3=default_config.s3,
            tables=TableConfig(
                db_uri=default_config.tables.db_uri,
                landing_table=table_name,
                profile="production",
            ),
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Copy selected production landing job IDs into an archive table.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--job-id", action="append", help="Job ID to copy; repeatable.")
    parser.add_argument("--job-id-file", help="UTF-8 file containing one job ID per line.")
    parser.add_argument("--archive-table", default=DEFAULT_ARCHIVE_TABLE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually write archive rows. Without this flag the command is a preview.",
    )
    parser.add_argument(
        "--confirm-copy",
        action="store_true",
        help="Required together with --execute to enable writes.",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than zero")
    if args.execute and not args.confirm_copy:
        parser.error("--execute requires --confirm-copy")

    source_client = archive_client = None
    try:
        job_ids = _read_job_ids(args)
        source_client = _client_for_table(DEFAULT_LANDING_TABLE)
        archive_client = _client_for_table(args.archive_table)

        total_source = total_copied = 0
        for job_id in job_ids:
            source_rows, copied = copy_job(
                source_client,
                archive_client,
                source_table=DEFAULT_LANDING_TABLE,
                archive_table=args.archive_table,
                job_id=job_id,
                batch_size=args.batch_size,
                execute=args.execute,
            )
            total_source += source_rows
            total_copied += copied
        print(
            f"Complete: jobs={len(job_ids)}, source_rows={total_source}, "
            f"rows_copied={total_copied}, source_modified=no"
        )
    except Exception as exc:
        print(f"Archive job-ID copy failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if source_client is not None:
            source_client.close()
        if archive_client is not None:
            archive_client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
