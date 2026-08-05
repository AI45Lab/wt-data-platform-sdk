"""Data models shared by the ETL engine, pipelines, and checkpoint stores."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from wt_sdk.models import ServingRecord

from .stage import SessionKey


class PipelineMode(str, Enum):
    LANDING = "landing"
    SERVING = "serving"


@dataclass(frozen=True)
class LandingRowPatch:
    record_id: str
    job_id: str
    session_id: str
    updates: dict[str, Any]


@dataclass(frozen=True)
class RecordFailure:
    record_id: Optional[str]
    job_id: str
    session_id: str
    stage_name: str
    error_type: str
    message: str


@dataclass(frozen=True)
class SessionResult:
    session_key: SessionKey
    source_rows: int
    selected_rows: int
    successful_rows: int
    landing_patches: tuple[LandingRowPatch, ...] = ()
    serving_records: tuple[ServingRecord, ...] = ()
    failures: tuple[RecordFailure, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.landing_patches or self.serving_records)


@dataclass
class RunSummary:
    pipeline_name: str
    pipeline_version: str
    mode: PipelineMode
    buckets_scanned: int = 0
    discovery_rows: int = 0
    sessions_processed: int = 0
    sessions_failed: int = 0
    source_rows: int = 0
    selected_rows: int = 0
    successful_rows: int = 0
    failed_rows: int = 0
    landing_rows_updated: int = 0
    serving_rows_upserted: int = 0
    failures: list[RecordFailure] = field(default_factory=list)
    dirty_sessions: set[SessionKey] = field(default_factory=set)

    def add_session(self, result: SessionResult, *, dry_run: bool) -> None:
        self.sessions_processed += 1
        self.source_rows += result.source_rows
        self.selected_rows += result.selected_rows
        self.successful_rows += result.successful_rows
        self.failed_rows += len(result.failures)
        self.failures.extend(result.failures)
        if result.failures:
            self.sessions_failed += 1
        self.landing_rows_updated += len(result.landing_patches)
        self.serving_rows_upserted += len(result.serving_records)
        if result.landing_patches and not dry_run:
            self.dirty_sessions.add(result.session_key)

    def add_failure(self, failure: RecordFailure) -> None:
        self.failed_rows += 1
        self.failures.append(failure)

    def merge(self, other: "RunSummary") -> None:
        if (
            self.pipeline_name,
            self.pipeline_version,
            self.mode,
        ) != (
            other.pipeline_name,
            other.pipeline_version,
            other.mode,
        ):
            raise ValueError("cannot merge summaries from different pipelines")
        self.buckets_scanned += other.buckets_scanned
        self.discovery_rows += other.discovery_rows
        self.sessions_processed += other.sessions_processed
        self.sessions_failed += other.sessions_failed
        self.source_rows += other.source_rows
        self.selected_rows += other.selected_rows
        self.successful_rows += other.successful_rows
        self.failed_rows += other.failed_rows
        self.landing_rows_updated += other.landing_rows_updated
        self.serving_rows_upserted += other.serving_rows_upserted
        self.failures.extend(other.failures)
        self.dirty_sessions.update(other.dirty_sessions)

    @property
    def status(self) -> str:
        return "FAILED" if self.failed_rows else "SUCCEEDED"


@dataclass(frozen=True)
class Checkpoint:
    pipeline_name: str
    pipeline_version: str
    source_table: str
    target_table: str
    bucket: int
    committed_until_ms: int
    last_run_id: Optional[str] = None
    active_window_start_ms: Optional[int] = None
    active_window_end_ms: Optional[int] = None
    last_processed_id: Optional[str] = None
    status: str = "IDLE"
    updated_at_ms: int = 0

    @property
    def checkpoint_id(self) -> str:
        return checkpoint_identity(
            self.pipeline_name,
            self.pipeline_version,
            self.source_table,
            self.target_table,
            self.bucket,
        )


def checkpoint_identity(
    pipeline_name: str,
    pipeline_version: str,
    source_table: str,
    target_table: str,
    bucket: int,
) -> str:
    return "|".join(
        (
            pipeline_name,
            pipeline_version,
            source_table,
            target_table,
            str(bucket),
        )
    )
