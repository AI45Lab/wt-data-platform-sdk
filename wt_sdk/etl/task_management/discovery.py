"""Bounded env-table scan used to discover job-level ETL readiness."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

import pandas as pd

from wt_sdk.env_config_client import EnvConfigManager


ENV_DISCOVERY_COLUMNS = ["id", "job_id", "env_id", "finished"]


@dataclass
class _MutableJobSummary:
    job_id: str
    total_envs: int = 0
    finished_envs: int = 0
    env_ids: set[str] = field(default_factory=set)
    duplicate_env_ids: set[str] = field(default_factory=set)
    empty_env_ids: int = 0


@dataclass(frozen=True)
class EnvJobSummary:
    job_id: str
    total_envs: int
    finished_envs: int
    unique_envs: int
    duplicate_env_ids: tuple[str, ...] = ()
    empty_env_ids: int = 0

    @property
    def ready(self) -> bool:
        return (
            self.total_envs > 0
            and self.finished_envs == self.total_envs
            and not self.duplicate_env_ids
            and self.empty_env_ids == 0
        )

    @property
    def issues(self) -> tuple[str, ...]:
        issues = []
        if self.finished_envs != self.total_envs:
            issues.append("env_not_all_finished")
        if self.empty_env_ids:
            issues.append("empty_env_id")
        if self.duplicate_env_ids:
            issues.append("duplicate_env_id")
        return tuple(issues)


@dataclass(frozen=True)
class EnvDiscoveryReport:
    snapshot_max_id: Optional[int]
    rows_scanned: int
    invalid_job_id_rows: int
    jobs: tuple[EnvJobSummary, ...]

    @property
    def ready_jobs(self) -> tuple[EnvJobSummary, ...]:
        return tuple(job for job in self.jobs if job.ready)


def discover_env_jobs(
    manager: EnvConfigManager,
    *,
    batch_size: int = 1000,
    job_ids: Optional[Iterable[str]] = None,
) -> EnvDiscoveryReport:
    """Scan a fixed env ``id`` window and aggregate readiness by ``job_id``.

    The scan reads only four small columns. New rows with IDs above the captured
    maximum are intentionally left for the next discovery run.
    """

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    selected_job_ids = _normalize_job_ids(job_ids)
    selection_query = _job_selection_query(selected_job_ids)
    max_query = _combine_queries("id IS NOT NULL", selection_query)
    latest = manager.session.filter(
        manager.table_name,
        query=max_query,
        limit=1,
        columns=["id"],
        order_by="id",
        ascending=False,
        checkout_latest=True,
    )
    if latest is None or latest.empty:
        return EnvDiscoveryReport(None, 0, 0, ())

    snapshot_max_id = int(latest.iloc[0]["id"])
    last_id = -1
    rows_scanned = 0
    invalid_job_id_rows = 0
    aggregates: dict[str, _MutableJobSummary] = {}

    while last_id < snapshot_max_id:
        window = f"id > {last_id} AND id <= {snapshot_max_id}"
        query = _combine_queries(window, selection_query)
        frame = manager.session.filter(
            manager.table_name,
            query=query,
            limit=batch_size,
            columns=ENV_DISCOVERY_COLUMNS,
            order_by="id",
            ascending=True,
            checkout_latest=True,
        )
        if frame is None or frame.empty:
            break

        batch_max_id = last_id
        for row in frame.to_dict(orient="records"):
            row_id = int(row["id"])
            batch_max_id = max(batch_max_id, row_id)
            rows_scanned += 1
            job_id = _nonempty_string(row.get("job_id"))
            if not job_id:
                invalid_job_id_rows += 1
                continue
            aggregate = aggregates.setdefault(job_id, _MutableJobSummary(job_id))
            aggregate.total_envs += 1
            if _is_true(row.get("finished")):
                aggregate.finished_envs += 1
            env_id = _nonempty_string(row.get("env_id"))
            if not env_id:
                aggregate.empty_env_ids += 1
            elif env_id in aggregate.env_ids:
                aggregate.duplicate_env_ids.add(env_id)
            else:
                aggregate.env_ids.add(env_id)

        if batch_max_id <= last_id:
            raise RuntimeError("env discovery did not advance its id cursor")
        last_id = batch_max_id

    jobs = tuple(
        EnvJobSummary(
            job_id=aggregate.job_id,
            total_envs=aggregate.total_envs,
            finished_envs=aggregate.finished_envs,
            unique_envs=len(aggregate.env_ids),
            duplicate_env_ids=tuple(sorted(aggregate.duplicate_env_ids)),
            empty_env_ids=aggregate.empty_env_ids,
        )
        for aggregate in sorted(aggregates.values(), key=lambda item: item.job_id)
    )
    return EnvDiscoveryReport(
        snapshot_max_id=snapshot_max_id,
        rows_scanned=rows_scanned,
        invalid_job_id_rows=invalid_job_id_rows,
        jobs=jobs,
    )


def _normalize_job_ids(values: Optional[Iterable[str]]) -> tuple[str, ...]:
    if values is None:
        return ()
    return tuple(
        sorted(
            {
                value.strip()
                for value in values
                if isinstance(value, str) and value.strip()
            }
        )
    )


def _job_selection_query(job_ids: tuple[str, ...]) -> str:
    if not job_ids:
        return ""
    literals = ", ".join(f"'{_escape_sql(job_id)}'" for job_id in job_ids)
    return f"job_id IN ({literals})"


def _combine_queries(*queries: str) -> str:
    effective = [query.strip() for query in queries if query and query.strip()]
    return " AND ".join(f"({query})" for query in effective)


def _nonempty_string(value: object) -> str:
    if value is None or _is_missing(value):
        return ""
    return str(value).strip()


def _is_true(value: object) -> bool:
    if value is True:
        return True
    if value is None or _is_missing(value):
        return False
    try:
        return bool(value == True)  # noqa: E712
    except (TypeError, ValueError):
        return False


def _is_missing(value: object) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    try:
        result = pd.isna(value)
        return bool(result)
    except (TypeError, ValueError):
        return False


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")
