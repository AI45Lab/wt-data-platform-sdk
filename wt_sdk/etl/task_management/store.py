"""dldb-backed task store for the single-writer ETL task worker."""

from __future__ import annotations

from dataclasses import asdict
from typing import Iterable, Optional

import dldb
import pandas as pd
import pyarrow as pa

import wt_sdk._time as sdk_time
from wt_sdk.config import S3Config
from wt_sdk.etl.checkpoint import resolve_etl_state_db_uri
from wt_sdk.etl.exceptions import CheckpointError

from .models import ETL_TASK_SCHEMA, ETLTask, TaskStatus


class TaskStateError(ValueError):
    """Raised when an invalid task state transition is requested."""


class DldbTaskStore:
    """Persist latest job-level orchestration state in one SimpleTable.

    This class deliberately does not claim to be safe for concurrent workers.
    The v1 contract requires exactly one writer/worker process.
    """

    def __init__(
        self,
        db_uri: Optional[str],
        *,
        table_name: str,
        s3: Optional[S3Config] = None,
    ) -> None:
        self.db_uri = resolve_etl_state_db_uri(db_uri)
        normalized_table = table_name.strip() if isinstance(table_name, str) else ""
        if not normalized_table:
            raise ValueError("task table_name is required")
        self.table_name = normalized_table
        self.session = dldb.connect(
            self.db_uri,
            storage_options=(s3 or S3Config()).to_storage_options(),
        )

    def initialize(self) -> bool:
        """Create the task table if absent; never replace an existing table."""

        if self.session.table_exists(self.table_name):
            self.verify_ready()
            return False
        self.session.create_table(self.table_name, ETL_TASK_SCHEMA)
        return True

    def verify_ready(self) -> None:
        if not self.session.table_exists(self.table_name):
            raise CheckpointError(
                f"ETL task table '{self.table_name}' does not exist in {self.db_uri}; "
                "initialize it explicitly before starting the task worker"
            )
        actual = self.session.get_schema(self.table_name)
        if actual != ETL_TASK_SCHEMA:
            raise CheckpointError(
                f"ETL task table '{self.table_name}' schema does not match "
                "ETL_TASK_SCHEMA"
            )

    def get(self, job_id: str) -> Optional[ETLTask]:
        normalized = _normalize_job_id(job_id)
        self.verify_ready()
        frame = self.session.filter(
            self.table_name,
            query=f"id = '{_escape_sql(normalized)}'",
            limit=1,
            checkout_latest=True,
        )
        if frame is None or frame.empty:
            return None
        return _task_from_row(frame.iloc[0].to_dict())

    def list(self, *, status: Optional[TaskStatus] = None) -> list[ETLTask]:
        self.verify_ready()
        query = "id IS NOT NULL"
        if status is not None:
            effective_status = status if isinstance(status, TaskStatus) else TaskStatus(status)
            query = f"status = '{effective_status.value}'"
        frame = self.session.filter(
            self.table_name,
            query=query,
            limit=None,
            checkout_latest=True,
        )
        if frame is None or frame.empty:
            return []
        tasks = [_task_from_row(row) for row in frame.to_dict(orient="records")]
        return sorted(tasks, key=lambda task: (task.created_at_ms, task.job_id))

    def submit_if_missing(
        self,
        job_id: str,
        *,
        now_ms: Optional[int] = None,
        baseline_succeeded: bool = False,
    ) -> tuple[ETLTask, bool]:
        """Create one task if absent and return ``(task, created)``."""

        normalized = _normalize_job_id(job_id)
        existing = self.get(normalized)
        if existing is not None:
            return existing, False
        timestamp = now_ms if now_ms is not None else sdk_time.now_ms()
        task = ETLTask(
            job_id=normalized,
            status=(TaskStatus.SUCCEEDED if baseline_succeeded else TaskStatus.ENQUEUED),
            attempt=0,
            created_at_ms=timestamp,
            updated_at_ms=timestamp,
            finished_at_ms=(timestamp if baseline_succeeded else None),
        )
        self.save(task)
        return task, True

    def save(self, task: ETLTask) -> None:
        self.verify_ready()
        row = asdict(task)
        row["id"] = task.id
        row["status"] = task.status.value
        table = pa.Table.from_pylist([row], schema=ETL_TASK_SCHEMA)
        frame = table.to_pandas(types_mapper=pd.ArrowDtype)
        self.session.upsert(self.table_name, columns=["id"], datas=frame)

    def claim_next(
        self,
        *,
        run_id: str,
        report_dir: str,
        now_ms: Optional[int] = None,
    ) -> Optional[ETLTask]:
        """Claim the oldest queued task under the v1 single-writer contract."""

        run_id = _normalize_required("run_id", run_id)
        report_dir = _normalize_required("report_dir", report_dir)
        queued = self.list(status=TaskStatus.ENQUEUED)
        if not queued:
            return None
        return self.claim(
            queued[0].job_id,
            run_id=run_id,
            report_dir=report_dir,
            now_ms=now_ms,
        )

    def claim(
        self,
        job_id: str,
        *,
        run_id: str,
        report_dir: str,
        now_ms: Optional[int] = None,
    ) -> ETLTask:
        """Claim one exact queued task under the v1 single-writer contract."""

        run_id = _normalize_required("run_id", run_id)
        report_dir = _normalize_required("report_dir", report_dir)
        task = self.get(job_id)
        if task is None:
            raise KeyError(f"ETL task not found for job_id={job_id!r}")
        self._require_status(task, TaskStatus.ENQUEUED)
        timestamp = now_ms if now_ms is not None else sdk_time.now_ms()
        claimed = task.with_updates(
            status=TaskStatus.RUNNING,
            attempt=task.attempt + 1,
            updated_at_ms=timestamp,
            started_at_ms=timestamp,
            finished_at_ms=None,
            current_pipeline=None,
            last_run_id=run_id,
            report_dir=report_dir,
            last_error=None,
            summary_json=None,
        )
        self.save(claimed)
        return claimed

    def set_current_pipeline(
        self,
        task: ETLTask,
        pipeline_name: str,
        *,
        now_ms: Optional[int] = None,
    ) -> ETLTask:
        self._require_status(task, TaskStatus.RUNNING)
        updated = task.with_updates(
            current_pipeline=_normalize_required("pipeline_name", pipeline_name),
            updated_at_ms=now_ms if now_ms is not None else sdk_time.now_ms(),
        )
        self.save(updated)
        return updated

    def mark_succeeded(
        self,
        task: ETLTask,
        *,
        summary_json: str,
        now_ms: Optional[int] = None,
    ) -> ETLTask:
        self._require_status(task, TaskStatus.RUNNING)
        timestamp = now_ms if now_ms is not None else sdk_time.now_ms()
        updated = task.with_updates(
            status=TaskStatus.SUCCEEDED,
            updated_at_ms=timestamp,
            finished_at_ms=timestamp,
            current_pipeline=None,
            last_error=None,
            summary_json=summary_json,
        )
        self.save(updated)
        return updated

    def mark_failed(
        self,
        task: ETLTask,
        *,
        error: str,
        summary_json: Optional[str] = None,
        now_ms: Optional[int] = None,
    ) -> ETLTask:
        self._require_status(task, TaskStatus.RUNNING)
        timestamp = now_ms if now_ms is not None else sdk_time.now_ms()
        updated = task.with_updates(
            status=TaskStatus.FAILED,
            updated_at_ms=timestamp,
            finished_at_ms=timestamp,
            current_pipeline=None,
            last_error=_truncate_error(error),
            summary_json=summary_json,
        )
        self.save(updated)
        return updated

    def requeue(
        self,
        job_id: str,
        *,
        allowed_statuses: Iterable[TaskStatus],
        now_ms: Optional[int] = None,
    ) -> ETLTask:
        task = self.get(job_id)
        if task is None:
            raise KeyError(f"ETL task not found for job_id={job_id!r}")
        allowed = {
            status if isinstance(status, TaskStatus) else TaskStatus(status)
            for status in allowed_statuses
        }
        if task.status not in allowed:
            names = ", ".join(sorted(status.value for status in allowed))
            raise TaskStateError(
                f"task {task.job_id!r} is {task.status.value}; expected one of: {names}"
            )
        timestamp = now_ms if now_ms is not None else sdk_time.now_ms()
        updated = task.with_updates(
            status=TaskStatus.ENQUEUED,
            updated_at_ms=timestamp,
            started_at_ms=None,
            finished_at_ms=None,
            current_pipeline=None,
            last_error=None,
        )
        self.save(updated)
        return updated

    @staticmethod
    def _require_status(task: ETLTask, expected: TaskStatus) -> None:
        if task.status is not expected:
            raise TaskStateError(
                f"task {task.job_id!r} is {task.status.value}; "
                f"expected {expected.value}"
            )

    def close(self) -> None:
        self.session.shutdown()

    def __enter__(self) -> "DldbTaskStore":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        _ = exc_type, exc_value, traceback
        self.close()


def _task_from_row(row: dict) -> ETLTask:
    return ETLTask(
        job_id=str(row["job_id"]),
        status=TaskStatus(str(row["status"])),
        attempt=int(row["attempt"]),
        created_at_ms=int(row["created_at_ms"]),
        updated_at_ms=int(row["updated_at_ms"]),
        started_at_ms=_optional_int(row.get("started_at_ms")),
        finished_at_ms=_optional_int(row.get("finished_at_ms")),
        current_pipeline=_optional_string(row.get("current_pipeline")),
        last_run_id=_optional_string(row.get("last_run_id")),
        report_dir=_optional_string(row.get("report_dir")),
        last_error=_optional_string(row.get("last_error")),
        summary_json=_optional_string(row.get("summary_json")),
    )


def _normalize_job_id(job_id: str) -> str:
    return _normalize_required("job_id", job_id)


def _normalize_required(name: str, value: str) -> str:
    normalized = value.strip() if isinstance(value, str) else ""
    if not normalized:
        raise ValueError(f"{name} is required")
    return normalized


def _optional_string(value) -> Optional[str]:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    normalized = str(value)
    return normalized if normalized else None


def _optional_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return int(value)


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _truncate_error(value: str, limit: int = 8000) -> str:
    normalized = str(value).strip()
    return normalized[:limit] if normalized else "unknown ETL task failure"
