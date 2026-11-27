#!/usr/bin/env python3
"""Refresh both production trajectory-statistics tables for one parent job."""

import argparse

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import GatewayConfig, TableConfig
from wt_sdk.trajectory_stats.service import TrajectoryStatsService, TrajectoryStatsUpdate
from wt_sdk.trajectory_stats.store import TrajectoryStatsStore


def refresh_job(job_id: str, *, batch_size: int = 100) -> TrajectoryStatsUpdate:
    """Recompute one job from serving-delivered sessions and all landing shards."""
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("job_id must be a non-empty string")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    job_id = job_id.strip()
    tables = TableConfig(profile="production")
    client = WTGatewayClient(GatewayConfig(tables=tables))
    try:
        store = TrajectoryStatsStore(tables.serving_db_uri)
    except Exception:
        client.close()
        raise

    try:
        store.verify_ready()
        service = TrajectoryStatsService(client, store)
        result = service.refresh_delivered_sessions(
            [job_id],
            landing_page_size=batch_size,
        )
        print(
            f"Job {job_id}: updated {result.sessions_updated} session rows; "
            f"upserted {result.categories_upserted} categories; deleted "
            f"{result.categories_deleted} stale categories.",
            flush=True,
        )
        return result
    finally:
        try:
            store.close()
        finally:
            client.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh production session and category trajectory statistics for one "
            "job without running ETL or changing landing/serving/checkpoint data."
        )
    )
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    args = parser.parse_args(argv)
    if not args.job_id.strip():
        parser.error("--job-id must be a non-empty string")
    if args.batch_size <= 0:
        parser.error("--batch-size must be a positive integer")
    refresh_job(args.job_id, batch_size=args.batch_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
