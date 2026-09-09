"""Single-process, single-task-at-a-time ETL task worker."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional
from uuid import uuid4

from .models import ETLTask, TaskStatus
from .store import DldbTaskStore


PIPELINES = (
    "landing_enrichment_pipeline",
    "landing_to_serving_pipeline",
)


@dataclass(frozen=True)
class PipelineExecution:
    pipeline_name: str
    return_code: int
    report: Optional[dict]
    log_path: str
    error: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return (
            self.return_code == 0
            and self.report is not None
            and self.report.get("status") == "SUCCEEDED"
        )


class ETLTaskWorker:
    """Drain queued jobs serially and update their latest orchestration state."""

    def __init__(
        self,
        task_store: DldbTaskStore,
        *,
        profile: str,
        report_root: str = "etl_reports/tasks",
        process_runner: Optional[
            Callable[[str, str, str, Path], PipelineExecution]
        ] = None,
    ) -> None:
        normalized_profile = profile.strip().lower()
        if normalized_profile == "prod":
            normalized_profile = "production"
        if normalized_profile not in {"production", "test"}:
            raise ValueError("profile must be production or test")
        self.task_store = task_store
        self.profile = normalized_profile
        self.report_root = Path(report_root).expanduser()
        self.process_runner = process_runner or run_pipeline_process
        self.stop_requested = False

    def request_stop(self) -> None:
        """Stop after the current job; do not interrupt an active pipeline."""

        self.stop_requested = True

    def drain(self) -> list[ETLTask]:
        """Run queued tasks in FIFO order until the queue is empty or stopped."""

        completed = []
        while not self.stop_requested:
            queued = self.task_store.list(status=TaskStatus.ENQUEUED)
            if not queued:
                break
            completed.append(self.run_job(queued[0]))
        return completed

    def run_job(self, queued_task: ETLTask) -> ETLTask:
        if queued_task.status is not TaskStatus.ENQUEUED:
            raise ValueError("run_job requires an ENQUEUED task")
        run_id = new_task_run_id()
        report_dir = task_report_dir(
            self.report_root,
            self.profile,
            queued_task.job_id,
            queued_task.attempt + 1,
            run_id,
        )
        report_dir.mkdir(parents=True, exist_ok=False)
        task = self.task_store.claim(
            queued_task.job_id,
            run_id=run_id,
            report_dir=str(report_dir.resolve()),
        )
        executions = []
        try:
            for pipeline_name in PIPELINES:
                task = self.task_store.set_current_pipeline(task, pipeline_name)
                execution = self.process_runner(
                    pipeline_name,
                    task.job_id,
                    self.profile,
                    report_dir,
                )
                executions.append(execution)
                if not execution.succeeded:
                    message = execution.error or (
                        f"{pipeline_name} exited with code {execution.return_code}"
                    )
                    summary = build_task_summary(task, executions, status="FAILED")
                    write_task_summary(report_dir, summary)
                    return self.task_store.mark_failed(
                        task,
                        error=message,
                        summary_json=json.dumps(
                            summary, ensure_ascii=False, sort_keys=True
                        ),
                    )

            summary = build_task_summary(task, executions, status="SUCCEEDED")
            write_task_summary(report_dir, summary)
            return self.task_store.mark_succeeded(
                task,
                summary_json=json.dumps(summary, ensure_ascii=False, sort_keys=True),
            )
        except Exception as exc:
            summary = build_task_summary(
                task,
                executions,
                status="FAILED",
                worker_error=f"{type(exc).__name__}: {exc}",
            )
            write_task_summary(report_dir, summary)
            return self.task_store.mark_failed(
                task,
                error=f"{type(exc).__name__}: {exc}",
                summary_json=json.dumps(summary, ensure_ascii=False, sort_keys=True),
            )


def run_pipeline_process(
    pipeline_name: str,
    job_id: str,
    profile: str,
    report_dir: Path,
) -> PipelineExecution:
    """Run one existing ETL CLI pipeline without a shell and stream its log."""

    command = [
        sys.executable,
        "-m",
        "wt_sdk.etl.cli.run",
        "--pipeline",
        pipeline_name,
        "--profile",
        profile,
        "--job-id",
        job_id,
        "--report-dir",
        str(report_dir),
    ]
    if profile == "production":
        command.append("--confirm-production")

    existing_reports = set(report_dir.glob("*.json"))
    log_path = report_dir / f"{pipeline_name}.log"
    try:
        with log_path.open("a", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            assert process.stdout is not None
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
                print(line, end="", flush=True)
            return_code = process.wait()
    except Exception as exc:
        return PipelineExecution(
            pipeline_name=pipeline_name,
            return_code=1,
            report=None,
            log_path=str(log_path.resolve()),
            error=f"{type(exc).__name__}: {exc}",
        )

    report = _find_new_pipeline_report(
        report_dir,
        pipeline_name,
        existing_reports,
    )
    error = None
    if report is None:
        error = f"{pipeline_name} produced no readable JSON report"
    elif report.get("status") != "SUCCEEDED":
        error = _report_error(report) or f"{pipeline_name} report status is FAILED"
    return PipelineExecution(
        pipeline_name=pipeline_name,
        return_code=return_code,
        report=report,
        log_path=str(log_path.resolve()),
        error=error,
    )


def build_task_summary(
    task: ETLTask,
    executions: list[PipelineExecution],
    *,
    status: str,
    worker_error: Optional[str] = None,
) -> dict:
    pipelines = {}
    totals = {
        "source_rows": 0,
        "selected_rows": 0,
        "successful_rows": 0,
        "failed_rows": 0,
        "warning_count": 0,
    }
    for execution in executions:
        report = execution.report or {}
        selected_rows = int(report.get("selected_rows") or 0)
        successful_rows = int(report.get("successful_rows") or 0)
        payload = {
            "status": report.get("status", "FAILED"),
            "return_code": execution.return_code,
            "pipeline_run_id": report.get("pipeline_run_id"),
            "duration_ms": report.get("duration_ms"),
            "source_rows": int(report.get("source_rows") or 0),
            "selected_rows": selected_rows,
            "successful_rows": successful_rows,
            "failed_rows": int(report.get("failed_rows") or 0),
            "success_rate": (
                successful_rows / selected_rows if selected_rows else None
            ),
            "sessions_processed": int(report.get("sessions_processed") or 0),
            "sessions_failed": int(report.get("sessions_failed") or 0),
            "warning_count": int(report.get("warning_count") or 0),
            "landing_rows_updated": int(report.get("landing_rows_updated") or 0),
            "serving_rows_upserted": int(report.get("serving_rows_upserted") or 0),
            "log_path": execution.log_path,
        }
        if execution.error:
            payload["error"] = execution.error
        pipelines[execution.pipeline_name] = payload
        for field in totals:
            totals[field] += int(payload[field] or 0)

    return {
        "task_run_id": task.last_run_id,
        "job_id": task.job_id,
        "attempt": task.attempt,
        "status": status,
        "report_dir": task.report_dir,
        "pipelines": pipelines,
        "totals": totals,
        "totals_semantics": "sum_of_pipeline_counters_not_unique_rows",
        "worker_error": worker_error,
    }


def write_task_summary(report_dir: Path, summary: dict) -> Path:
    path = report_dir / "task_summary.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def task_report_dir(
    report_root: Path,
    profile: str,
    job_id: str,
    attempt: int,
    run_id: str,
) -> Path:
    readable = re.sub(r"[^A-Za-z0-9._-]+", "_", job_id).strip("._-")
    readable = (readable or "job")[:100]
    digest = hashlib.sha256(job_id.encode("utf-8")).hexdigest()[:8]
    return (
        report_root
        / profile
        / f"{readable}--{digest}"
        / f"attempt-{attempt:04d}--{run_id}"
    )


def new_task_run_id() -> str:
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    return f"etl-task__{timestamp}__{uuid4().hex[:12]}"


def _find_new_pipeline_report(
    report_dir: Path,
    pipeline_name: str,
    existing_reports: set[Path],
) -> Optional[dict]:
    candidates = []
    for path in report_dir.glob("*.json"):
        if path in existing_reports or path.name == "task_summary.json":
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("pipeline_name") == pipeline_name:
            candidates.append((path.stat().st_mtime_ns, payload))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def _report_error(report: dict) -> Optional[str]:
    failures = report.get("failures") or []
    if not failures:
        return None
    first = failures[0]
    return (
        f"{first.get('error_type', 'ETLFailure')}: "
        f"{first.get('message', 'pipeline failed')}"
    )


def wait_until(deadline: float, *, should_stop: Callable[[], bool]) -> None:
    """Interruptible bounded wait used by the hourly worker loop."""

    while not should_stop():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 1.0))
