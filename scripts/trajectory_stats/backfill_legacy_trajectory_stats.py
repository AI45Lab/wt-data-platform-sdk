#!/usr/bin/env python3
"""Backfill legacy delivered sessions into production trajectory statistics.

The legacy database is read-only. Session rows are upserted idempotently, then
categories are rebuilt from every row in the new session table. Neither target
table is dropped or truncated.
"""

import argparse
from typing import Iterable, Mapping, Optional

import dldb
import pandas as pd

import wt_sdk._time as sdk_time
from wt_sdk.config import TableConfig, storage_options_for
from wt_sdk.trajectory_stats.service import (
    TrajectoryStatsUpdate,
    rebuild_category_table,
)
from wt_sdk.trajectory_stats.session_metrics import SessionMetricsAccumulator
from wt_sdk.trajectory_stats.store import TrajectoryStatsStore


LEGACY_DB_URI = "s3://wind-tunnel-dldb"
LEGACY_LANDING_TABLE = "wind_tunnel_landing"
LEGACY_SERVING_TABLE = "wind_tunnel_serving"
LEGACY_LANDING_COLUMNS = [
    "id",
    "job_id",
    "session_id",
    "step_id",
    "meta_json",
    "response",
    "messages",
]
# Keep each Arrow JSON materialization below its byte-offset limit.
DEFAULT_SCAN_BATCH_SIZE = 100
# Reuse each pruned job-partition scan across many delivered sessions.
DEFAULT_SESSION_QUERY_BATCH_SIZE = 500
PROGRESS_INTERVAL_SESSIONS = 1000


def _delivered_session_keys(
    serving_rows: Iterable[Mapping[str, object]],
) -> list[tuple[str, str]]:
    return sorted(
        {
            (row.get("job_id"), row.get("session_id"))
            for row in serving_rows
            if isinstance(row.get("job_id"), str)
            and row.get("job_id")
            and isinstance(row.get("session_id"), str)
            and row.get("session_id")
        }
    )


def _accumulators_for(
    session_keys: Iterable[tuple[str, str]],
) -> dict[tuple[str, str], SessionMetricsAccumulator]:
    return {
        (job_id, session_id): SessionMetricsAccumulator(job_id, session_id)
        for job_id, session_id in session_keys
    }


def _accumulate_landing_rows(
    accumulators: Mapping[tuple[str, str], SessionMetricsAccumulator],
    landing_rows: Iterable[Mapping[str, object]],
) -> int:
    rows_read = 0
    if not accumulators:
        return rows_read
    for row in landing_rows:
        rows_read += 1
        key = (row.get("job_id"), row.get("session_id"))
        accumulator = accumulators.get(key)
        if accumulator is not None:
            accumulator.add_row(row)
    return rows_read


def _upsert_accumulators(
    accumulators: Mapping[tuple[str, str], SessionMetricsAccumulator],
    store: TrajectoryStatsStore,
    *,
    updated_at_ms: int,
    batch_size: int,
) -> int:
    sessions_updated = 0
    batch = []
    for accumulator in accumulators.values():
        batch.append(accumulator.finalize(updated_at_ms=updated_at_ms))
        if len(batch) >= batch_size:
            sessions_updated += store.upsert_session_rows(batch)
            batch = []
    if batch:
        sessions_updated += store.upsert_session_rows(batch)
    return sessions_updated


def backfill_legacy_rows(
    serving_rows: Iterable[Mapping[str, object]],
    landing_rows: Iterable[Mapping[str, object]],
    store: TrajectoryStatsStore,
    *,
    updated_at_ms: int,
    batch_size: int = 500,
) -> TrajectoryStatsUpdate:
    """Adapt legacy rows to bounded accumulators and the shared target store."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    delivered = _delivered_session_keys(serving_rows)
    accumulators = _accumulators_for(delivered)
    _accumulate_landing_rows(accumulators, landing_rows)

    sessions_updated = _upsert_accumulators(
        accumulators,
        store,
        updated_at_ms=updated_at_ms,
        batch_size=batch_size,
    )
    category_update = rebuild_category_table(store, updated_at_ms=updated_at_ms)
    return TrajectoryStatsUpdate(
        sessions_updated=sessions_updated,
        categories_upserted=category_update.categories_upserted,
        categories_deleted=category_update.categories_deleted,
    )


def _backfill_delivered_sessions(
    session,
    table_name: str,
    delivered: Iterable[tuple[str, str]],
    store: TrajectoryStatsStore,
    *,
    updated_at_ms: int,
    batch_size: int,
    session_query_batch_size: int,
    scan_batch_size: int,
) -> int:
    """Reverse-query only serving-delivered sessions, one parent job at a time."""
    if session_query_batch_size <= 0:
        raise ValueError("session_query_batch_size must be positive")
    delivered = sorted(set(delivered))
    sessions_by_job: dict[str, list[str]] = {}
    for job_id, session_id in delivered:
        sessions_by_job.setdefault(job_id, []).append(session_id)

    sessions_updated = 0
    landing_rows_read = 0
    next_progress = PROGRESS_INTERVAL_SESSIONS
    refresh_latest = True
    for job_id in sorted(sessions_by_job):
        session_ids = sessions_by_job[job_id]
        for start in range(0, len(session_ids), session_query_batch_size):
            batch_session_ids = session_ids[start : start + session_query_batch_size]
            session_keys = [(job_id, session_id) for session_id in batch_session_ids]
            accumulators = _accumulators_for(session_keys)
            landing_rows = _iter_landing_rows(
                session,
                table_name,
                job_id=job_id,
                session_ids=batch_session_ids,
                scan_batch_size=scan_batch_size,
                checkout_latest=refresh_latest,
            )
            landing_rows_read += _accumulate_landing_rows(accumulators, landing_rows)
            refresh_latest = False
            sessions_updated += _upsert_accumulators(
                accumulators,
                store,
                updated_at_ms=updated_at_ms,
                batch_size=batch_size,
            )
            if sessions_updated >= next_progress:
                print(
                    f"Upserted {sessions_updated}/{len(delivered)} delivered sessions; "
                    f"read {landing_rows_read} matching landing rows.",
                    flush=True,
                )
                next_progress += PROGRESS_INTERVAL_SESSIONS
    print(
        f"Finished {sessions_updated} delivered sessions; "
        f"read {landing_rows_read} matching landing rows.",
        flush=True,
    )
    return sessions_updated


def run_backfill(
    *,
    dry_run: bool = False,
    confirm_production: bool = False,
    confirm_backfill: bool = False,
    legacy_db_uri: str = LEGACY_DB_URI,
    legacy_landing_table: str = LEGACY_LANDING_TABLE,
    legacy_serving_table: str = LEGACY_SERVING_TABLE,
    target_db_uri: Optional[str] = None,
    batch_size: int = 500,
    scan_batch_size: int = DEFAULT_SCAN_BATCH_SIZE,
    session_query_batch_size: int = DEFAULT_SESSION_QUERY_BATCH_SIZE,
) -> int:
    target = target_db_uri or TableConfig(profile="production").serving_db_uri
    print(f"Legacy read-only database: {legacy_db_uri}", flush=True)
    print(f"Legacy serving table: {legacy_serving_table}", flush=True)
    print(f"Legacy landing table: {legacy_landing_table}", flush=True)
    print(f"Production statistics database: {target}", flush=True)
    print(
        f"Landing query page size: {scan_batch_size}; sessions per query: "
        f"{session_query_batch_size}; session upsert batch size: {batch_size}.",
        flush=True,
    )
    print(
        "Target tables are never dropped or truncated; categories are fully rebuilt.",
        flush=True,
    )
    if dry_run:
        print("Dry run: no database connection or data change was made.")
        return 0
    if not confirm_production or not confirm_backfill:
        print(
            "Refusing production backfill without both --confirm-production "
            "and --confirm-backfill. Use --dry-run to inspect first."
        )
        return 2
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if scan_batch_size <= 0:
        raise ValueError("scan_batch_size must be positive")
    if session_query_batch_size <= 0:
        raise ValueError("session_query_batch_size must be positive")

    connect_kwargs = {}
    storage_options = storage_options_for(legacy_db_uri)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
    legacy = dldb.connect(legacy_db_uri, **connect_kwargs)
    try:
        store = TrajectoryStatsStore(target)
    except Exception:
        legacy.shutdown()
        raise
    try:
        store.verify_ready()
        serving_rows = _read_rows(
            legacy,
            legacy_serving_table,
            ["id", "job_id", "session_id"],
        )
        delivered = _delivered_session_keys(serving_rows)
        print(
            f"Read {len(serving_rows)} legacy serving rows and "
            f"{len(delivered)} distinct delivered sessions.",
            flush=True,
        )
        updated_at_ms = sdk_time.now_ms()
        sessions_updated = _backfill_delivered_sessions(
            legacy,
            legacy_landing_table,
            delivered,
            store,
            updated_at_ms=updated_at_ms,
            batch_size=batch_size,
            session_query_batch_size=session_query_batch_size,
            scan_batch_size=scan_batch_size,
        )
        category_update = rebuild_category_table(store, updated_at_ms=updated_at_ms)
        result = TrajectoryStatsUpdate(
            sessions_updated=sessions_updated,
            categories_upserted=category_update.categories_upserted,
            categories_deleted=category_update.categories_deleted,
        )
        print(
            f"Upserted {result.sessions_updated} session rows and "
            f"{result.categories_upserted} categories; deleted "
            f"{result.categories_deleted} stale categories.",
            flush=True,
        )
        return 0
    finally:
        try:
            store.close()
        finally:
            legacy.shutdown()


def _iter_landing_rows(
    session,
    table_name: str,
    *,
    job_id: str,
    session_ids: list[str],
    scan_batch_size: int,
    checkout_latest: bool,
) -> Iterable[dict[str, object]]:
    """Read bounded id pages for exact serving-delivered session keys."""
    if scan_batch_size <= 0:
        raise ValueError("scan_batch_size must be positive")
    if not session_ids:
        return
    session_literals = ", ".join(
        f"'{_escape_sql(session_id)}'" for session_id in session_ids
    )
    base_query = (
        f"job_id = '{_escape_sql(job_id)}' "
        f"AND session_id IN ({session_literals}) AND id IS NOT NULL"
    )
    cursor = None
    refresh_latest = checkout_latest
    while True:
        query = base_query
        if cursor is not None:
            query += f" AND id > '{_escape_sql(cursor)}'"
        rows = _read_rows(
            session,
            table_name,
            LEGACY_LANDING_COLUMNS,
            query=query,
            limit=scan_batch_size,
            order_by="id",
            ascending=True,
            checkout_latest=refresh_latest,
        )
        refresh_latest = False
        if not rows:
            break
        for row in rows:
            row_id = row.get("id")
            if not isinstance(row_id, str) or not row_id:
                raise ValueError(
                    "legacy landing pagination requires non-empty string ids"
                )
            if cursor is not None and row_id <= cursor:
                raise ValueError(
                    f"legacy landing ids are not strictly increasing: "
                    f"{row_id!r} after {cursor!r}"
                )
            cursor = row_id
            yield row
        if len(rows) < scan_batch_size:
            break


def _read_rows(
    session,
    table_name: str,
    columns: list[str],
    *,
    query: str = "id IS NOT NULL",
    limit: Optional[int] = None,
    partitions: Optional[list[int]] = None,
    order_by: Optional[str] = None,
    ascending: bool = True,
    checkout_latest: bool = True,
) -> list[dict[str, object]]:
    frame = session.filter(
        table_name,
        query=query,
        columns=columns,
        limit=limit,
        partitions=partitions,
        order_by=order_by,
        ascending=ascending,
        checkout_latest=checkout_latest,
    )
    if frame is None or frame.empty:
        return []
    return [
        {key: _none_if_missing(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _none_if_missing(value):
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Idempotently backfill legacy trajectory metrics and rebuild categories."
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-production", action="store_true")
    parser.add_argument("--confirm-backfill", action="store_true")
    parser.add_argument("--legacy-db-uri", default=LEGACY_DB_URI)
    parser.add_argument("--legacy-landing-table", default=LEGACY_LANDING_TABLE)
    parser.add_argument("--legacy-serving-table", default=LEGACY_SERVING_TABLE)
    parser.add_argument("--target-db-uri", default=None)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--scan-batch-size",
        type=int,
        default=DEFAULT_SCAN_BATCH_SIZE,
        help="Maximum landing rows read per delivered-session query page.",
    )
    parser.add_argument(
        "--session-query-batch-size",
        type=int,
        default=DEFAULT_SESSION_QUERY_BATCH_SIZE,
        help="Number of serving-delivered sessions in one same-job landing query.",
    )
    args = parser.parse_args()
    return run_backfill(
        dry_run=args.dry_run,
        confirm_production=args.confirm_production,
        confirm_backfill=args.confirm_backfill,
        legacy_db_uri=args.legacy_db_uri,
        legacy_landing_table=args.legacy_landing_table,
        legacy_serving_table=args.legacy_serving_table,
        target_db_uri=args.target_db_uri,
        batch_size=args.batch_size,
        scan_batch_size=args.scan_batch_size,
        session_query_batch_size=args.session_query_batch_size,
    )


if __name__ == "__main__":
    raise SystemExit(main())
