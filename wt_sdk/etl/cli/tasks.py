"""Manage the job-level ETL task queue.

This is intentionally separate from ``wt_sdk.etl.cli.run``: the existing ETL
CLI remains the execution engine, while this command owns discovery, queue
state, bootstrap, and the single serial worker.
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from dataclasses import asdict
from typing import Optional

from wt_sdk import GatewayConfig, TableConfig, WTGatewayClient
from wt_sdk.env_config_client import EnvConfigManager
from wt_sdk.etl.checkpoint import resolve_etl_state_db_uri
from wt_sdk.etl.task_management import (
    DldbTaskStore,
    TaskStatus,
    apply_bootstrap,
    build_bootstrap_plan,
    discover_and_enqueue,
    resolve_task_table,
)
from wt_sdk.etl.task_management.worker import ETLTaskWorker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the serial WT ETL task worker")
    parser.add_argument(
        "--profile",
        choices=("test", "prod", "production"),
        default="test",
        help="Select task/env/landing tables by profile (default: test).",
    )
    parser.add_argument(
        "--state-db-uri",
        default=None,
        help="ETL state database URI (default: WT_SDK_ETL_STATE_DB_URI).",
    )
    parser.add_argument(
        "--task-table",
        default=None,
        help="Override the profile-selected task table name.",
    )
    parser.add_argument(
        "--env-batch-size",
        type=int,
        default=1000,
        help="Rows per bounded env discovery query (default: 1000).",
    )
    parser.add_argument(
        "--report-root",
        default="etl_reports/tasks",
        help="Root directory for per-task reports (default: etl_reports/tasks).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Create and verify the task table")
    init.add_argument("--dry-run", action="store_true")
    init.add_argument("--confirm-create", action="store_true")

    discover = subparsers.add_parser("discover", help="Scan env once and enqueue ready jobs")
    discover.set_defaults(once=True)

    submit = subparsers.add_parser("submit", help="Validate one job and enqueue it")
    submit.add_argument("--job-id", required=True)

    status = subparsers.add_parser("status", help="Show one task")
    status.add_argument("--job-id", required=True)

    listing = subparsers.add_parser("list", help="List tasks")
    listing.add_argument("--status", choices=[status.value for status in TaskStatus])

    retry = subparsers.add_parser("retry", help="Requeue a FAILED task")
    retry.add_argument("--job-id", required=True)

    recover = subparsers.add_parser(
        "recover",
        help="Requeue a RUNNING task after confirming its worker is gone",
    )
    recover.add_argument("--job-id", required=True)

    rerun = subparsers.add_parser("rerun", help="Requeue a SUCCEEDED task")
    rerun.add_argument("--job-id", required=True)

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="Plan or apply the first-run historical task baseline",
    )
    bootstrap.add_argument("--execute", action="store_true")
    bootstrap.add_argument("--confirm-execute", action="store_true")
    bootstrap.add_argument("--baseline-from-serving", action="store_true")
    bootstrap.add_argument("--enqueue-job-id", action="append", default=[])

    worker = subparsers.add_parser("worker", help="Run the serial task worker")
    worker.add_argument(
        "--poll-seconds",
        "--scan-interval-seconds",
        dest="scan_interval_seconds",
        type=int,
        default=3600,
    )
    worker.add_argument("--once", action="store_true")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    profile = "production" if args.profile == "prod" else args.profile
    if args.env_batch_size <= 0:
        raise SystemExit("--env-batch-size must be positive")

    task_table = resolve_task_table(profile, args.task_table)
    if args.command == "init":
        print(
            json.dumps(
                {
                    "profile": profile,
                    "state_db_uri": args.state_db_uri or "WT_SDK_ETL_STATE_DB_URI",
                    "task_table": task_table,
                    "read_only": bool(args.dry_run),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if args.dry_run:
            return 0
        if not args.confirm_create:
            print("Refusing to create without --confirm-create")
            return 2
        with _open_store(args, profile, task_table) as store:
            created = store.initialize()
            print(json.dumps({"created": created, "task_table": task_table}))
        return 0

    if args.command == "bootstrap":
        return _bootstrap(args, profile, task_table)
    if args.command == "worker":
        return _run_worker(args, profile, task_table)

    with _open_store(args, profile, task_table) as store:
        if args.command == "discover":
            with EnvConfigManager(profile=profile) as env_manager:
                result = discover_and_enqueue(
                    env_manager,
                    store,
                    batch_size=args.env_batch_size,
                )
            print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if args.command == "submit":
            with EnvConfigManager(profile=profile) as env_manager:
                task, created, discovery = _submit(
                    env_manager,
                    store,
                    args.job_id,
                    args.env_batch_size,
                )
            payload = {
                "job_id": args.job_id,
                "created": created,
                "status": task.status.value if task else "NOT_READY",
                "discovery": {
                    "rows_scanned": discovery.rows_scanned,
                    "jobs": [asdict(job) for job in discovery.jobs],
                },
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
            return 0 if task else 1
        if args.command == "status":
            task = store.get(args.job_id)
            if task is None:
                print(json.dumps({"job_id": args.job_id, "status": "NOT_FOUND"}))
                return 1
            print(json.dumps(_task_to_dict(task), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if args.command == "list":
            status = TaskStatus(args.status) if args.status else None
            print(
                json.dumps(
                    [_task_to_dict(task) for task in store.list(status=status)],
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command in {"retry", "recover", "rerun"}:
            allowed = (
                (TaskStatus.FAILED,)
                if args.command == "retry"
                else (TaskStatus.RUNNING,)
                if args.command == "recover"
                else (TaskStatus.SUCCEEDED,)
            )
            task = store.requeue(args.job_id, allowed_statuses=allowed)
            print(json.dumps(_task_to_dict(task), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
    raise AssertionError(f"unhandled command: {args.command}")


def _open_store(args, profile: str, task_table: str) -> DldbTaskStore:
    _ = profile
    return DldbTaskStore(
        resolve_etl_state_db_uri(args.state_db_uri),
        table_name=task_table,
    )


def _submit(env_manager, store, job_id: str, batch_size: int):
    from wt_sdk.etl.task_management.service import submit_if_ready

    return submit_if_ready(env_manager, store, job_id, batch_size=batch_size)


def _bootstrap(args, profile: str, task_table: str) -> int:
    if args.enqueue_job_id and not args.execute:
        print("--enqueue-job-id requires --execute")
        return 2
    if args.execute and not args.confirm_execute:
        print("Refusing to apply bootstrap without --confirm-execute")
        return 2
    with EnvConfigManager(profile=profile) as env_manager:
        table_config = TableConfig(profile=profile)
        with WTGatewayClient(GatewayConfig(tables=table_config)) as gateway_client:
            plan = build_bootstrap_plan(
                env_manager,
                gateway_client,
                batch_size=args.env_batch_size,
            )
    payload = {"plan": plan.to_dict(), "read_only": not args.execute}
    if args.execute:
        with _open_store(args, profile, task_table) as store:
            applied = apply_bootstrap(
                store,
                baseline_job_ids=(
                    plan.baseline_job_ids if args.baseline_from_serving else ()
                ),
                enqueue_job_ids=args.enqueue_job_id,
            )
        payload["applied"] = applied
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _run_worker(args, profile: str, task_table: str) -> int:
    if args.scan_interval_seconds <= 0:
        raise SystemExit("--poll-seconds must be positive")
    with _open_store(args, profile, task_table) as store:
        with EnvConfigManager(profile=profile) as env_manager:
            worker = ETLTaskWorker(
                store,
                profile=profile,
                report_root=args.report_root,
            )

            def stop_handler(signum, frame):
                _ = signum, frame
                worker.request_stop()

            signal.signal(signal.SIGTERM, stop_handler)
            signal.signal(signal.SIGINT, stop_handler)
            if args.once:
                result = discover_and_enqueue(
                    env_manager,
                    store,
                    batch_size=args.env_batch_size,
                )
                print(
                    json.dumps(
                        {"event": "discovery", **result.to_dict()},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
                worker.drain()
                return 0

            next_discovery = 0.0
            while not worker.stop_requested:
                now = time.monotonic()
                if now >= next_discovery:
                    result = discover_and_enqueue(
                        env_manager,
                        store,
                        batch_size=args.env_batch_size,
                    )
                    print(
                        json.dumps(
                            {"event": "discovery", **result.to_dict()},
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        flush=True,
                    )
                    next_discovery = now + args.scan_interval_seconds
                worker.drain()
                if worker.stop_requested:
                    break
                wait_for = max(0.1, min(1.0, next_discovery - time.monotonic()))
                time.sleep(wait_for)
    return 0


def _task_to_dict(task) -> dict:
    payload = asdict(task)
    payload["status"] = task.status.value
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
