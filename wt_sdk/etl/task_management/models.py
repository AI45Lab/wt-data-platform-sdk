"""Data model and Arrow schema for the job-level ETL task queue."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional

import pyarrow as pa


PRODUCTION_TASK_TABLE = "wind_tunnel_etl_tasks"
TEST_TASK_TABLE = "etl_tasks_test"


class TaskStatus(str, Enum):
    ENQUEUED = "ENQUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


ETL_TASK_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("job_id", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("attempt", pa.int32(), nullable=False),
        pa.field("created_at_ms", pa.int64(), nullable=False),
        pa.field("updated_at_ms", pa.int64(), nullable=False),
        pa.field("started_at_ms", pa.int64(), nullable=True),
        pa.field("finished_at_ms", pa.int64(), nullable=True),
        pa.field("current_pipeline", pa.string(), nullable=True),
        pa.field("last_run_id", pa.string(), nullable=True),
        pa.field("report_dir", pa.string(), nullable=True),
        pa.field("last_error", pa.string(), nullable=True),
        pa.field("summary_json", pa.string(), nullable=True),
    ]
)


@dataclass(frozen=True)
class ETLTask:
    job_id: str
    status: TaskStatus = TaskStatus.ENQUEUED
    attempt: int = 0
    created_at_ms: int = 0
    updated_at_ms: int = 0
    started_at_ms: Optional[int] = None
    finished_at_ms: Optional[int] = None
    current_pipeline: Optional[str] = None
    last_run_id: Optional[str] = None
    report_dir: Optional[str] = None
    last_error: Optional[str] = None
    summary_json: Optional[str] = None

    def __post_init__(self) -> None:
        normalized = self.job_id.strip() if isinstance(self.job_id, str) else ""
        if not normalized:
            raise ValueError("job_id is required")
        if normalized != self.job_id:
            object.__setattr__(self, "job_id", normalized)
        if not isinstance(self.status, TaskStatus):
            object.__setattr__(self, "status", TaskStatus(str(self.status)))
        if self.attempt < 0:
            raise ValueError("attempt must be non-negative")

    @property
    def id(self) -> str:
        return self.job_id

    def with_updates(self, **changes) -> "ETLTask":
        return replace(self, **changes)


def resolve_task_table(profile: Optional[str], explicit: Optional[str] = None) -> str:
    if explicit and explicit.strip():
        return explicit.strip()
    normalized = (profile or "test").strip().lower()
    if normalized == "prod":
        normalized = "production"
    if normalized == "production":
        return PRODUCTION_TASK_TABLE
    if normalized == "test":
        return TEST_TASK_TABLE
    raise ValueError("profile must be one of: production, prod, test")
