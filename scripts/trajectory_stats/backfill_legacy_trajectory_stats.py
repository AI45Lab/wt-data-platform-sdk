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
from wt_sdk.trajectory_stats.session_metrics import compute_session_metrics
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


def backfill_legacy_rows(
    serving_rows: Iterable[Mapping[str, object]],
    landing_rows: Iterable[Mapping[str, object]],
    store: TrajectoryStatsStore,
    *,
    updated_at_ms: int,
    batch_size: int = 500,
) -> TrajectoryStatsUpdate:
    """Adapt already-read legacy rows to the shared computation and target store."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    delivered = sorted(
        {
            (row.get("job_id"), row.get("session_id"))
            for row in serving_rows
            if isinstance(row.get("job_id"), str)
            and row.get("job_id")
            and isinstance(row.get("session_id"), str)
            and row.get("session_id")
        }
    )
    delivered_set = set(delivered)
    landing_by_session = {key: [] for key in delivered}
    for row in landing_rows:
        key = (row.get("job_id"), row.get("session_id"))
        if key in delivered_set:
            landing_by_session[key].append(row)

    sessions_updated = 0
    batch = []
    for job_id, session_id in delivered:
        batch.append(
            compute_session_metrics(
                job_id,
                session_id,
                landing_by_session[(job_id, session_id)],
                updated_at_ms=updated_at_ms,
            )
        )
        if len(batch) >= batch_size:
            sessions_updated += store.upsert_session_rows(batch)
            batch = []
    if batch:
        sessions_updated += store.upsert_session_rows(batch)

    category_update = rebuild_category_table(store, updated_at_ms=updated_at_ms)
    return TrajectoryStatsUpdate(
        sessions_updated=sessions_updated,
        categories_upserted=category_update.categories_upserted,
        categories_deleted=category_update.categories_deleted,
    )


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
) -> int:
    target = target_db_uri or TableConfig(profile="production").serving_db_uri
    print(f"Legacy read-only database: {legacy_db_uri}")
    print(f"Legacy serving table: {legacy_serving_table}")
    print(f"Legacy landing table: {legacy_landing_table}")
    print(f"Production statistics database: {target}")
    print("Target tables are never dropped or truncated; categories are fully rebuilt.")
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
        delivered_count = len(
            {
                (row.get("job_id"), row.get("session_id"))
                for row in serving_rows
                if row.get("job_id") and row.get("session_id")
            }
        )
        landing_rows = (
            _read_rows(legacy, legacy_landing_table, LEGACY_LANDING_COLUMNS)
            if delivered_count
            else []
        )
        print(
            f"Read {len(serving_rows)} legacy serving rows, "
            f"{delivered_count} distinct sessions, and {len(landing_rows)} landing rows."
        )
        result = backfill_legacy_rows(
            serving_rows,
            landing_rows,
            store,
            updated_at_ms=sdk_time.now_ms(),
            batch_size=batch_size,
        )
        print(
            f"Upserted {result.sessions_updated} session rows and "
            f"{result.categories_upserted} categories; deleted "
            f"{result.categories_deleted} stale categories."
        )
        return 0
    finally:
        try:
            store.close()
        finally:
            legacy.shutdown()


def _read_rows(session, table_name: str, columns: list[str]) -> list[dict[str, object]]:
    frame = session.filter(
        table_name,
        query="id IS NOT NULL",
        columns=columns,
        limit=None,
        checkout_latest=True,
    )
    if frame is None or frame.empty:
        return []
    return [
        {key: _none_if_missing(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


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
    )


if __name__ == "__main__":
    raise SystemExit(main())
