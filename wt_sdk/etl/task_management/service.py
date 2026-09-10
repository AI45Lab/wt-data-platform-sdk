"""Task discovery, enqueue, and bootstrap orchestration services."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Optional

import wt_sdk._time as sdk_time
from wt_sdk import WTGatewayClient
from wt_sdk.env_config_client import EnvConfigManager

from .discovery import EnvDiscoveryReport, discover_env_jobs
from .models import ETLTask
from .store import DldbTaskStore


@dataclass(frozen=True)
class EnqueueResult:
    discovery: EnvDiscoveryReport
    created_job_ids: tuple[str, ...]
    existing_job_ids: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            "rows_scanned": self.discovery.rows_scanned,
            "snapshot_max_id": self.discovery.snapshot_max_id,
            "invalid_job_id_rows": self.discovery.invalid_job_id_rows,
            "jobs_seen": len(self.discovery.jobs),
            "ready_jobs": len(self.discovery.ready_jobs),
            "created_job_ids": list(self.created_job_ids),
            "existing_job_ids": list(self.existing_job_ids),
            "not_ready": [
                {
                    **asdict(job),
                    "issues": list(job.issues),
                }
                for job in self.discovery.jobs
                if not job.ready
            ],
        }


@dataclass(frozen=True)
class BootstrapPlan:
    serving_job_ids: tuple[str, ...]
    baseline_job_ids: tuple[str, ...]
    completed_unserved_job_ids: tuple[str, ...]
    unfinished_job_ids: tuple[str, ...]
    invalid_job_ids: tuple[str, ...]
    env_rows_scanned: int

    def to_dict(self) -> dict:
        return asdict(self)


def discover_and_enqueue(
    env_manager: EnvConfigManager,
    task_store: DldbTaskStore,
    *,
    batch_size: int = 1000,
    job_ids: Optional[Iterable[str]] = None,
    now_ms: Optional[int] = None,
) -> EnqueueResult:
    discovery = discover_env_jobs(
        env_manager,
        batch_size=batch_size,
        job_ids=job_ids,
    )
    timestamp = now_ms if now_ms is not None else sdk_time.now_ms()
    created = []
    existing = []
    for job in discovery.ready_jobs:
        _, was_created = task_store.submit_if_missing(job.job_id, now_ms=timestamp)
        (created if was_created else existing).append(job.job_id)
    return EnqueueResult(
        discovery=discovery,
        created_job_ids=tuple(created),
        existing_job_ids=tuple(existing),
    )


def submit_if_ready(
    env_manager: EnvConfigManager,
    task_store: DldbTaskStore,
    job_id: str,
    *,
    batch_size: int = 1000,
) -> tuple[Optional[ETLTask], bool, EnvDiscoveryReport]:
    result = discover_and_enqueue(
        env_manager,
        task_store,
        batch_size=batch_size,
        job_ids=[job_id],
    )
    if not result.discovery.ready_jobs:
        return None, False, result.discovery
    task = task_store.get(job_id)
    if task is None:
        raise RuntimeError("ready ETL task was not readable after enqueue")
    return task, bool(result.created_job_ids), result.discovery


def build_bootstrap_plan(
    env_manager: EnvConfigManager,
    gateway_client: WTGatewayClient,
    *,
    batch_size: int = 1000,
) -> BootstrapPlan:
    discovery = discover_env_jobs(env_manager, batch_size=batch_size)
    serving_rows = gateway_client.query_data(
        filter_query="job_id IS NOT NULL",
        limit=None,
        columns=["job_id"],
        table=gateway_client.config.tables.serving_table,
        checkout_latest=True,
        exclude_none=False,
        deserialize_json=False,
    )
    serving_job_ids = {
        str(row.get("job_id") or "").strip()
        for row in serving_rows
        if str(row.get("job_id") or "").strip()
    }
    ready_job_ids = {job.job_id for job in discovery.ready_jobs}
    known_env_job_ids = {job.job_id for job in discovery.jobs}
    invalid_job_ids = {
        job.job_id
        for job in discovery.jobs
        if "empty_env_id" in job.issues or "duplicate_env_id" in job.issues
    }
    unfinished = {
        job.job_id
        for job in discovery.jobs
        if "env_not_all_finished" in job.issues
    }
    return BootstrapPlan(
        serving_job_ids=tuple(sorted(serving_job_ids)),
        baseline_job_ids=tuple(sorted(serving_job_ids)),
        completed_unserved_job_ids=tuple(
            sorted(ready_job_ids - serving_job_ids)
        ),
        unfinished_job_ids=tuple(sorted(unfinished)),
        invalid_job_ids=tuple(sorted(invalid_job_ids)),
        env_rows_scanned=discovery.rows_scanned,
    )


def apply_bootstrap(
    task_store: DldbTaskStore,
    *,
    baseline_job_ids: Iterable[str],
    enqueue_job_ids: Iterable[str],
) -> dict:
    timestamp = sdk_time.now_ms()
    baselined = []
    enqueued = []
    existing = []
    for job_id in sorted(set(baseline_job_ids)):
        _, created = task_store.submit_if_missing(
            job_id,
            now_ms=timestamp,
            baseline_succeeded=True,
        )
        (baselined if created else existing).append(job_id)
    for job_id in sorted(set(enqueue_job_ids)):
        _, created = task_store.submit_if_missing(job_id, now_ms=timestamp)
        (enqueued if created else existing).append(job_id)
    return {
        "baselined_job_ids": baselined,
        "enqueued_job_ids": enqueued,
        "existing_job_ids": sorted(set(existing)),
    }
