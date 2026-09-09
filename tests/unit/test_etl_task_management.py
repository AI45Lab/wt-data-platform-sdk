import json
from types import SimpleNamespace

import pandas as pd
import pytest

from wt_sdk.etl.cli.tasks import build_parser
from wt_sdk.etl.task_management.discovery import discover_env_jobs
from wt_sdk.etl.task_management.models import (
    ETL_TASK_SCHEMA,
    ETLTask,
    TaskStatus,
    format_task_time,
)
from wt_sdk.etl.task_management.store import DldbTaskStore, TaskStateError
from wt_sdk.etl.task_management.worker import (
    ETLTaskWorker,
    PipelineExecution,
    build_task_summary,
    task_report_dir,
)


class FakeEnvSession:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def filter(
        self,
        table,
        *,
        query,
        limit,
        columns,
        order_by=None,
        ascending=True,
        checkout_latest=True,
    ):
        self.calls.append(
            {
                "query": query,
                "limit": limit,
                "columns": columns,
                "order_by": order_by,
            }
        )
        rows = list(self.rows)
        if "job_id IN" in query:
            selected = query.split("job_id IN (", 1)[1].split(")", 1)[0]
            selected = {part.strip().strip("'") for part in selected.split(",")}
            rows = [row for row in rows if row["job_id"] in selected]
        if "id >" in query:
            lower = int(query.split("id >", 1)[1].split()[0])
            rows = [row for row in rows if row["id"] > lower]
        if "id <=" in query:
            upper = int(query.split("id <=", 1)[1].split(")", 1)[0].strip())
            rows = [row for row in rows if row["id"] <= upper]
        rows.sort(key=lambda row: row["id"], reverse=not ascending)
        if limit is not None:
            rows = rows[:limit]
        return pd.DataFrame(rows, columns=columns)


def test_discovery_uses_bounded_keyset_batches_and_aggregates_jobs():
    rows = [
        {"id": 1, "job_id": "job-a", "env_id": "a1", "finished": True},
        {"id": 2, "job_id": "job-a", "env_id": "a2", "finished": True},
        {"id": 3, "job_id": "job-b", "env_id": "b1", "finished": False},
        {"id": 4, "job_id": "job-b", "env_id": "b2", "finished": True},
    ]
    session = FakeEnvSession(rows)
    manager = SimpleNamespace(session=session, table_name="env_config_test")

    result = discover_env_jobs(manager, batch_size=2)

    assert result.snapshot_max_id == 4
    assert result.rows_scanned == 4
    assert [job.job_id for job in result.ready_jobs] == ["job-a"]
    assert [job.total_envs for job in result.jobs] == [2, 2]
    assert any(call["limit"] == 2 for call in session.calls)


class FakeTaskSession:
    def __init__(self):
        self.rows = {}
        self.shutdown_called = False

    def table_exists(self, table_name):
        return bool(self.rows) or getattr(self, "created", False)

    def create_table(self, table_name, schema):
        assert schema == ETL_TASK_SCHEMA
        self.created = True

    def get_schema(self, table_name):
        return ETL_TASK_SCHEMA

    def filter(self, table_name, *, query, limit, checkout_latest=True):
        rows = list(self.rows.values())
        if query.startswith("id = "):
            key = query.split("'", 2)[1].replace("''", "'")
            rows = [self.rows[key]] if key in self.rows else []
        if query.startswith("status = "):
            status = query.split("'", 2)[1]
            rows = [row for row in rows if row["status"] == status]
        return pd.DataFrame(rows)

    def upsert(self, table_name, *, columns, datas):
        for row in datas.to_dict(orient="records"):
            self.rows[row["id"]] = row

    def shutdown(self):
        self.shutdown_called = True


def test_task_store_is_idempotent_and_supports_retry(monkeypatch):
    session = FakeTaskSession()
    monkeypatch.setattr("wt_sdk.etl.task_management.store.dldb.connect", lambda *a, **k: session)
    store = DldbTaskStore("s3://state", table_name="etl_tasks_test")
    assert store.initialize() is True

    first, created = store.submit_if_missing("job-a", now_ms=10)
    second, created_again = store.submit_if_missing("job-a", now_ms=20)
    assert created is True
    assert created_again is False
    assert second.created_at_ms == 10
    assert second.created_at_text == "1970-01-01 08:00"

    claimed = store.claim("job-a", run_id="run-1", report_dir="reports", now_ms=30)
    assert claimed.status is TaskStatus.RUNNING
    assert claimed.attempt == 1
    failed = store.mark_failed(claimed, error="boom", now_ms=40)
    assert failed.status is TaskStatus.FAILED
    retried = store.requeue("job-a", allowed_statuses=(TaskStatus.FAILED,), now_ms=50)
    assert retried.status is TaskStatus.ENQUEUED
    assert retried.attempt == 1

    with pytest.raises(TaskStateError):
        store.requeue("job-a", allowed_statuses=(TaskStatus.SUCCEEDED,))
    store.close()
    assert session.shutdown_called is True


def test_task_time_text_uses_shanghai_time_and_minute_precision():
    assert format_task_time(0) == "1970-01-01 08:00"
    assert format_task_time(None) is None


def test_worker_default_discovery_interval_is_three_hours():
    args = build_parser().parse_args(["worker"])
    assert args.scan_interval_seconds == 10800


def test_task_report_dir_is_unique_and_summary_contains_pipeline_metrics(tmp_path):
    first = task_report_dir(tmp_path, "production", "a#job", 1, "run-1")
    second = task_report_dir(tmp_path, "production", "a#job", 2, "run-2")
    assert first != second
    task = ETLTask("a#job", status=TaskStatus.RUNNING, attempt=1, last_run_id="run-1")
    report = {
        "pipeline_name": "landing_enrichment_pipeline",
        "status": "SUCCEEDED",
        "selected_rows": 4,
        "successful_rows": 3,
        "duration_ms": 12,
        "warning_count": 1,
    }
    summary = build_task_summary(
        task,
        [
            PipelineExecution(
                "landing_enrichment_pipeline", 0, report, str(first / "etl.log")
            )
        ],
        status="SUCCEEDED",
    )
    assert summary["pipelines"]["landing_enrichment_pipeline"]["success_rate"] == 0.75
    assert summary["pipelines"]["landing_enrichment_pipeline"]["warning_count"] == 1


def test_worker_runs_one_job_per_task_and_stops_after_queue_drains(tmp_path):
    class FakeStore:
        def __init__(self):
            self.task = ETLTask("job-a", status=TaskStatus.ENQUEUED, created_at_ms=1)
            self.saved = []

        def list(self, *, status=None):
            return [self.task] if self.task.status is status else []

        def claim(self, job_id, *, run_id, report_dir):
            self.task = self.task.with_updates(
                status=TaskStatus.RUNNING,
                attempt=self.task.attempt + 1,
                last_run_id=run_id,
                report_dir=report_dir,
            )
            return self.task

        def set_current_pipeline(self, task, pipeline_name):
            self.task = task.with_updates(current_pipeline=pipeline_name)
            return self.task

        def mark_succeeded(self, task, *, summary_json):
            self.task = task.with_updates(status=TaskStatus.SUCCEEDED, summary_json=summary_json)
            self.saved.append(self.task)
            return self.task

        def mark_failed(self, *args, **kwargs):
            raise AssertionError("unexpected failure")

    calls = []

    def runner(pipeline, job_id, profile, report_dir):
        calls.append((pipeline, job_id))
        return PipelineExecution(
            pipeline,
            0,
            {"pipeline_name": pipeline, "status": "SUCCEEDED"},
            str(report_dir / f"{pipeline}.log"),
        )

    store = FakeStore()
    worker = ETLTaskWorker(
        store,
        profile="test",
        report_root=str(tmp_path),
        process_runner=runner,
    )
    completed = worker.drain()
    assert len(completed) == 1
    assert calls == [
        ("landing_enrichment_pipeline", "job-a"),
        ("landing_to_serving_pipeline", "job-a"),
    ]
    assert completed[0].status is TaskStatus.SUCCEEDED


def test_worker_drains_all_queued_jobs_serially(tmp_path):
    class FakeStore:
        def __init__(self):
            self.tasks = {
                job_id: ETLTask(
                    job_id,
                    status=TaskStatus.ENQUEUED,
                    created_at_ms=created_at_ms,
                )
                for job_id, created_at_ms in (("job-a", 1), ("job-b", 2))
            }

        def list(self, *, status=None):
            return sorted(
                (task for task in self.tasks.values() if task.status is status),
                key=lambda task: task.created_at_ms,
            )

        def claim(self, job_id, *, run_id, report_dir):
            task = self.tasks[job_id].with_updates(
                status=TaskStatus.RUNNING,
                attempt=1,
                last_run_id=run_id,
                report_dir=report_dir,
            )
            self.tasks[job_id] = task
            return task

        def set_current_pipeline(self, task, pipeline_name):
            task = task.with_updates(current_pipeline=pipeline_name)
            self.tasks[task.job_id] = task
            return task

        def mark_succeeded(self, task, *, summary_json):
            task = task.with_updates(
                status=TaskStatus.SUCCEEDED,
                summary_json=summary_json,
            )
            self.tasks[task.job_id] = task
            return task

        def mark_failed(self, *args, **kwargs):
            raise AssertionError("unexpected failure")

    calls = []

    def runner(pipeline, job_id, profile, report_dir):
        calls.append((job_id, pipeline))
        return PipelineExecution(
            pipeline,
            0,
            {"pipeline_name": pipeline, "status": "SUCCEEDED"},
            str(report_dir / f"{pipeline}.log"),
        )

    worker = ETLTaskWorker(
        FakeStore(),
        profile="test",
        report_root=str(tmp_path),
        process_runner=runner,
    )
    completed = worker.drain()

    assert [task.job_id for task in completed] == ["job-a", "job-b"]
    assert calls == [
        ("job-a", "landing_enrichment_pipeline"),
        ("job-a", "landing_to_serving_pipeline"),
        ("job-b", "landing_enrichment_pipeline"),
        ("job-b", "landing_to_serving_pipeline"),
    ]
