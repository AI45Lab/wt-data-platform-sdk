"""Incremental, range, and targeted execution for ETL pipelines."""

from dataclasses import replace
from typing import Callable, Iterable, Optional, Sequence
from uuid import uuid4

import wt_sdk._time as sdk_time
from wt_sdk.client import WTGatewayClient

from .checkpoint import CheckpointStore
from .exceptions import CheckpointError, ETLRunFailed, SessionValidationError
from .models import (
    Checkpoint,
    PipelineMode,
    RecordFailure,
    RunSummary,
    SessionResult,
)
from .pipeline import PipelineDefinition
from .stage import SessionKey


DISCOVERY_COLUMNS = ["id", "job_id", "session_id", "source_updated_at"]


class ETLEngine:
    """Execute validated pipelines through the supported WT SDK client."""

    def __init__(
        self,
        client: WTGatewayClient,
        *,
        checkpoint_store: Optional[CheckpointStore] = None,
    ) -> None:
        self.client = client
        self.checkpoint_store = checkpoint_store

    def run_incremental(
        self,
        pipeline: PipelineDefinition,
        *,
        settle_delay_ms: int = 2 * 60 * 60 * 1000,
        page_size: int = 1000,
        start_from_ms: Optional[int] = None,
        dry_run: bool = False,
        run_started_at_ms: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> RunSummary:
        """Scan every existing landing HASH bucket from durable checkpoints."""

        _validate_page_size(page_size)
        if settle_delay_ms < 0:
            raise ValueError("settle_delay_ms must be non-negative")
        if start_from_ms is not None and start_from_ms < 0:
            raise ValueError("start_from_ms must be non-negative")
        if self.checkpoint_store is None:
            raise CheckpointError("incremental ETL requires a durable checkpoint store")

        started_at = (
            sdk_time.now_ms() if run_started_at_ms is None else run_started_at_ms
        )
        cutoff = started_at - settle_delay_ms
        effective_run_id = _run_id(run_id)
        if cutoff < 0:
            raise ValueError("settle delay produces a negative run cutoff")

        summary = self._new_summary(pipeline)
        source_table, target_table = self._table_names(pipeline)
        buckets = self.client.list_table_partitions(table=source_table)
        has_failures = False
        for raw_bucket in buckets:
            if not isinstance(raw_bucket, int):
                raise CheckpointError(
                    f"v1 incremental ETL requires integer HASH buckets, got {raw_bucket!r}"
                )
            try:
                bucket_summary = self._run_incremental_bucket(
                    pipeline,
                    bucket=raw_bucket,
                    cutoff_ms=cutoff,
                    page_size=page_size,
                    start_from_ms=start_from_ms,
                    source_table=source_table,
                    target_table=target_table,
                    dry_run=dry_run,
                    run_id=effective_run_id,
                )
            except ETLRunFailed as exc:
                bucket_summary = exc.summary
                has_failures = True
            summary.merge(bucket_summary)
        if has_failures:
            raise ETLRunFailed(summary)
        return summary

    def run_filter(
        self,
        pipeline: PipelineDefinition,
        filter_query: str,
        *,
        page_size: int = 1000,
        buckets: Optional[Sequence[int]] = None,
        dry_run: bool = False,
    ) -> RunSummary:
        """Run a manual dldb WHERE predicate without changing checkpoints."""

        _validate_page_size(page_size)
        normalized_filter = filter_query.strip()
        if not normalized_filter:
            raise ValueError("source filter must be a non-empty WHERE expression")
        source_table, _ = self._table_names(pipeline)
        selected_buckets = (
            list(buckets)
            if buckets is not None
            else list(self.client.list_table_partitions(table=source_table))
        )
        summary = self._new_summary(pipeline)
        for bucket in selected_buckets:
            if not isinstance(bucket, int):
                raise ValueError(
                    f"manual source filter requires integer HASH buckets, got {bucket!r}"
                )
            bucket_summary = self._scan_bucket_filter(
                pipeline,
                bucket=bucket,
                filter_query=normalized_filter,
                page_size=page_size,
                dry_run=dry_run,
            )
            summary.merge(bucket_summary)
        if summary.failed_rows:
            raise ETLRunFailed(summary)
        return summary

    def run_range(
        self,
        pipeline: PipelineDefinition,
        *,
        start_ms: int,
        end_ms: int,
        page_size: int = 1000,
        buckets: Optional[Sequence[int]] = None,
        dry_run: bool = False,
    ) -> RunSummary:
        """Run an inclusive manual backfill without changing global checkpoints."""

        _validate_page_size(page_size)
        if start_ms < 0 or end_ms < start_ms:
            raise ValueError("manual range requires 0 <= start_ms <= end_ms")
        source_table, _ = self._table_names(pipeline)
        selected_buckets = (
            list(buckets)
            if buckets is not None
            else list(self.client.list_table_partitions(table=source_table))
        )
        summary = self._new_summary(pipeline)
        has_failures = False
        for bucket in selected_buckets:
            if not isinstance(bucket, int):
                raise ValueError(f"manual range requires integer HASH buckets, got {bucket!r}")
            bucket_summary = self._scan_bucket_window(
                pipeline,
                bucket=bucket,
                start_ms=start_ms,
                end_ms=end_ms,
                page_size=page_size,
                last_processed_id=None,
                include_start=True,
                dry_run=dry_run,
            )
            summary.merge(bucket_summary)
            has_failures = has_failures or bucket_summary.failed_rows > 0
        if has_failures:
            raise ETLRunFailed(summary)
        return summary

    def run_sessions(
        self,
        pipeline: PipelineDefinition,
        session_keys: Iterable[SessionKey],
        *,
        dry_run: bool = False,
    ) -> RunSummary:
        """Immediately process explicit sessions without changing checkpoints."""

        summary = self._new_summary(pipeline)
        for key in sorted(set(session_keys)):
            result = self._process_one_session(pipeline, key, dry_run=dry_run)
            summary.add_session(result, dry_run=dry_run)
        if summary.failed_rows:
            raise ETLRunFailed(summary)
        return summary

    def run_job(
        self,
        pipeline: PipelineDefinition,
        job_id: str,
        *,
        dry_run: bool = False,
    ) -> RunSummary:
        """Immediately process every non-null session currently in one job."""

        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            raise ValueError("job_id is required")
        source_table = self.client.config.tables.landing_table
        rows = self.client.query_data(
            filter_query=f"job_id = '{_escape_sql(normalized_job_id)}'",
            columns=["id", "job_id", "session_id"],
            partition=normalized_job_id,
            checkout_latest=True,
            table=source_table,
            exclude_none=False,
        )
        keys: set[SessionKey] = set()
        discovery_failures: list[RecordFailure] = []
        for row in rows:
            session_id = str(row.get("session_id") or "").strip()
            if not session_id:
                discovery_failures.append(
                    RecordFailure(
                        record_id=_optional_string(row.get("id")),
                        job_id=normalized_job_id,
                        session_id="",
                        stage_name="__discovery__",
                        error_type="SessionValidationError",
                        message="job discovery row has empty session_id",
                    )
                )
                continue
            keys.add(SessionKey(normalized_job_id, session_id))
        try:
            summary = self.run_sessions(pipeline, keys, dry_run=dry_run)
        except ETLRunFailed as exc:
            summary = exc.summary
        for failure in discovery_failures:
            summary.add_failure(failure)
        if summary.failed_rows:
            raise ETLRunFailed(summary)
        return summary

    def run_jobs(
        self,
        pipeline: PipelineDefinition,
        job_ids: Iterable[str],
        *,
        dry_run: bool = False,
    ) -> RunSummary:
        """Immediately process all sessions in multiple explicit jobs."""

        normalized_job_ids = sorted(
            {job_id.strip() for job_id in job_ids if job_id.strip()}
        )
        if not normalized_job_ids:
            raise ValueError("at least one non-empty job_id is required")
        summary = self._new_summary(pipeline)
        for job_id in normalized_job_ids:
            try:
                job_summary = self.run_job(pipeline, job_id, dry_run=dry_run)
            except ETLRunFailed as exc:
                job_summary = exc.summary
            summary.merge(job_summary)
        if summary.failed_rows:
            raise ETLRunFailed(summary)
        return summary

    def _run_incremental_bucket(
        self,
        pipeline: PipelineDefinition,
        *,
        bucket: int,
        cutoff_ms: int,
        page_size: int,
        start_from_ms: Optional[int],
        source_table: str,
        target_table: str,
        dry_run: bool,
        run_id: str,
    ) -> RunSummary:
        assert self.checkpoint_store is not None
        checkpoint = self.checkpoint_store.load(
            pipeline_name=pipeline.name,
            pipeline_version=pipeline.version,
            source_table=source_table,
            target_table=target_table,
            bucket=bucket,
        )

        if checkpoint and checkpoint.active_window_end_ms is not None:
            window_start = checkpoint.active_window_start_ms
            window_end = checkpoint.active_window_end_ms
            last_processed_id = checkpoint.last_processed_id
            if window_start is None:
                raise CheckpointError(
                    f"checkpoint {checkpoint.checkpoint_id} has an active end without a start"
                )
        else:
            if checkpoint is None:
                if start_from_ms is None:
                    raise CheckpointError(
                        f"bucket {bucket} has no checkpoint; provide start_from_ms for first run"
                    )
                # Incremental windows are normally (watermark, cutoff]. Moving
                # the first watermark back by one millisecond makes the user-
                # supplied bootstrap timestamp inclusive without adding a
                # second checkpoint cursor mode that must survive crashes.
                window_start = start_from_ms - 1
                checkpoint = Checkpoint(
                    pipeline_name=pipeline.name,
                    pipeline_version=pipeline.version,
                    source_table=source_table,
                    target_table=target_table,
                    bucket=bucket,
                    committed_until_ms=window_start,
                )
            else:
                window_start = checkpoint.committed_until_ms
            window_end = cutoff_ms
            last_processed_id = None

        if window_end <= window_start:
            return self._new_summary(pipeline)

        active = replace(
            checkpoint,
            last_run_id=run_id,
            active_window_start_ms=window_start,
            active_window_end_ms=window_end,
            last_processed_id=last_processed_id,
            status="RUNNING",
            updated_at_ms=sdk_time.now_ms(),
        )
        if not dry_run:
            self.checkpoint_store.save(active)

        latest = active

        def page_committed(last_id: str) -> None:
            nonlocal latest
            latest = replace(
                latest,
                last_processed_id=last_id,
                status="RUNNING",
                updated_at_ms=sdk_time.now_ms(),
            )
            if not dry_run:
                self.checkpoint_store.save(latest)

        try:
            summary = self._scan_bucket_window(
                pipeline,
                bucket=bucket,
                start_ms=window_start,
                end_ms=window_end,
                page_size=page_size,
                last_processed_id=last_processed_id,
                include_start=False,
                dry_run=dry_run,
                on_page_committed=page_committed,
            )
            if summary.failed_rows:
                raise ETLRunFailed(summary)
        except Exception:
            if not dry_run:
                self.checkpoint_store.save(
                    replace(latest, status="FAILED", updated_at_ms=sdk_time.now_ms())
                )
            raise

        if not dry_run:
            completed = replace(
                latest,
                committed_until_ms=window_end,
                active_window_start_ms=None,
                active_window_end_ms=None,
                last_processed_id=None,
                status="IDLE",
                updated_at_ms=sdk_time.now_ms(),
            )
            self.checkpoint_store.save(completed)
            if window_end < cutoff_ms:
                catch_up = self._run_incremental_bucket(
                    pipeline,
                    bucket=bucket,
                    cutoff_ms=cutoff_ms,
                    page_size=page_size,
                    start_from_ms=start_from_ms,
                    source_table=source_table,
                    target_table=target_table,
                    dry_run=False,
                    run_id=run_id,
                )
                # The recursive call represents another window for the same
                # physical bucket, not another distinct bucket.
                catch_up.buckets_scanned = 0
                summary.merge(catch_up)
        return summary

    def _scan_bucket_filter(
        self,
        pipeline: PipelineDefinition,
        *,
        bucket: int,
        filter_query: str,
        page_size: int,
        dry_run: bool,
    ) -> RunSummary:
        summary = self._new_summary(pipeline)
        summary.buckets_scanned = 1
        source_table, _ = self._table_names(pipeline)
        cursor: Optional[str] = None
        processed_sessions: set[SessionKey] = set()

        while True:
            filters = [f"({filter_query})"]
            if cursor is not None:
                filters.append(f"id > '{_escape_sql(cursor)}'")
            page = self.client.query_data(
                filter_query=" AND ".join(filters),
                limit=page_size,
                columns=DISCOVERY_COLUMNS,
                partition=bucket,
                order_by="id",
                ascending=True,
                checkout_latest=True,
                table=source_table,
                exclude_none=False,
                deserialize_json=False,
            )
            if not page:
                break
            summary.discovery_rows += len(page)

            session_keys: set[SessionKey] = set()
            for row in page:
                job_id = str(row.get("job_id") or "").strip()
                session_id = str(row.get("session_id") or "").strip()
                if not job_id or not session_id:
                    summary.add_failure(
                        RecordFailure(
                            record_id=_optional_string(row.get("id")),
                            job_id=job_id,
                            session_id=session_id,
                            stage_name="__discovery__",
                            error_type="SessionValidationError",
                            message="discovery row has empty job_id/session_id",
                        )
                    )
                    continue
                session_keys.add(SessionKey(job_id=job_id, session_id=session_id))

            for key in sorted(session_keys - processed_sessions):
                result = self._process_one_session(pipeline, key, dry_run=dry_run)
                summary.add_session(result, dry_run=dry_run)
                processed_sessions.add(key)

            cursor = str(page[-1]["id"])
            if len(page) < page_size:
                break
        return summary

    def _scan_bucket_window(
        self,
        pipeline: PipelineDefinition,
        *,
        bucket: int,
        start_ms: int,
        end_ms: int,
        page_size: int,
        last_processed_id: Optional[str],
        include_start: bool,
        dry_run: bool,
        on_page_committed: Optional[Callable[[str], None]] = None,
    ) -> RunSummary:
        summary = self._new_summary(pipeline)
        summary.buckets_scanned = 1
        source_table, _ = self._table_names(pipeline)
        cursor = last_processed_id
        start_operator = ">=" if include_start else ">"
        processed_sessions: set[SessionKey] = set()

        while True:
            filters = [
                f"source_updated_at {start_operator} {start_ms}",
                f"source_updated_at <= {end_ms}",
            ]
            if cursor is not None:
                filters.append(f"id > '{_escape_sql(cursor)}'")
            page = self.client.query_data(
                filter_query=" AND ".join(filters),
                limit=page_size,
                columns=DISCOVERY_COLUMNS,
                partition=bucket,
                order_by="id",
                ascending=True,
                checkout_latest=True,
                table=source_table,
                exclude_none=False,
                deserialize_json=False,
            )
            if not page:
                break
            summary.discovery_rows += len(page)

            session_keys: set[SessionKey] = set()
            for row in page:
                job_id = str(row.get("job_id") or "").strip()
                session_id = str(row.get("session_id") or "").strip()
                if not job_id or not session_id:
                    summary.add_failure(
                        RecordFailure(
                            record_id=_optional_string(row.get("id")),
                            job_id=job_id,
                            session_id=session_id,
                            stage_name="__discovery__",
                            error_type="SessionValidationError",
                            message="discovery row has empty job_id/session_id",
                        )
                    )
                    continue
                session_keys.add(SessionKey(job_id=job_id, session_id=session_id))

            for key in sorted(session_keys - processed_sessions):
                result = self._process_one_session(pipeline, key, dry_run=dry_run)
                summary.add_session(result, dry_run=dry_run)
                processed_sessions.add(key)

            cursor = str(page[-1]["id"])
            if on_page_committed is not None and summary.failed_rows == 0:
                on_page_committed(cursor)
            if len(page) < page_size:
                break
        return summary

    def _process_one_session(
        self,
        pipeline: PipelineDefinition,
        key: SessionKey,
        *,
        dry_run: bool,
    ) -> SessionResult:
        try:
            rows = self._load_session(key)
        except SessionValidationError as exc:
            return _session_failure_result(key, (), exc, "__session_load__")
        try:
            result = pipeline.process_session(rows, collect_failures=True)
        except SessionValidationError as exc:
            return _session_failure_result(key, rows, exc, "__session_validation__")
        if dry_run:
            return result

        if pipeline.mode is PipelineMode.LANDING:
            successful_patches = []
            failures = list(result.failures)
            successful_rows = result.successful_rows
            for patch in result.landing_patches:
                query = (
                    f"job_id = '{_escape_sql(patch.job_id)}' "
                    f"AND session_id = '{_escape_sql(patch.session_id)}' "
                    f"AND id = '{_escape_sql(patch.record_id)}'"
                )
                try:
                    self.client.update_landing(
                        query,
                        patch.updates,
                        partition=patch.job_id,
                    )
                except Exception as exc:
                    failures.append(
                        _sink_failure(
                            patch.record_id,
                            key,
                            "__landing_sink__",
                            exc,
                        )
                    )
                    successful_rows -= 1
                else:
                    successful_patches.append(patch)
            result = replace(
                result,
                successful_rows=successful_rows,
                landing_patches=tuple(successful_patches),
                failures=tuple(failures),
            )
        elif result.serving_records:
            try:
                self.client.upsert_serving_batch(list(result.serving_records))
            except Exception as exc:
                failures = list(result.failures)
                failures.extend(
                    _sink_failure(record.id, key, "__serving_sink__", exc)
                    for record in result.serving_records
                )
                result = replace(
                    result,
                    successful_rows=(
                        result.successful_rows - len(result.serving_records)
                    ),
                    serving_records=(),
                    failures=tuple(failures),
                )
        return result

    def _load_session(self, key: SessionKey) -> list[dict[str, object]]:
        source_table = self.client.config.tables.landing_table
        query = (
            f"job_id = '{_escape_sql(key.job_id)}' "
            f"AND session_id = '{_escape_sql(key.session_id)}'"
        )
        rows = self.client.query_data(
            filter_query=query,
            partition=key.job_id,
            order_by="step_id",
            ascending=True,
            checkout_latest=True,
            table=source_table,
            exclude_none=False,
            deserialize_json=False,
        )
        if not rows:
            raise SessionValidationError(f"session not found: {key}")
        return rows

    def _table_names(self, pipeline: PipelineDefinition) -> tuple[str, str]:
        source = self.client.config.tables.landing_table
        target = (
            source
            if pipeline.mode is PipelineMode.LANDING
            else self.client.config.tables.serving_table
        )
        return source, target

    @staticmethod
    def _new_summary(pipeline: PipelineDefinition) -> RunSummary:
        return RunSummary(
            pipeline_name=pipeline.name,
            pipeline_version=pipeline.version,
            mode=pipeline.mode,
        )


def _validate_page_size(page_size: int) -> None:
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size <= 0:
        raise ValueError("page_size must be a positive integer")


def _run_id(value: Optional[str]) -> str:
    if value is None:
        return uuid4().hex
    normalized = value.strip()
    if not normalized:
        raise ValueError("run_id must be a non-empty string")
    return normalized


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _optional_string(value: object) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _sink_failure(
    record_id: Optional[str],
    key: SessionKey,
    stage_name: str,
    exc: Exception,
) -> RecordFailure:
    return RecordFailure(
        record_id=record_id,
        job_id=key.job_id,
        session_id=key.session_id,
        stage_name=stage_name,
        error_type=type(exc).__name__,
        message=str(exc),
    )


def _session_failure_result(
    key: SessionKey,
    rows: Sequence[dict[str, object]],
    exc: Exception,
    stage_name: str,
) -> SessionResult:
    row_ids = [_optional_string(row.get("id")) for row in rows] or [None]
    failures = tuple(
        RecordFailure(
            record_id=record_id,
            job_id=key.job_id,
            session_id=key.session_id,
            stage_name=stage_name,
            error_type=type(exc).__name__,
            message=str(exc),
        )
        for record_id in row_ids
    )
    return SessionResult(
        session_key=key,
        source_rows=len(rows),
        selected_rows=0,
        successful_rows=0,
        failures=failures,
    )


__all__ = ["DISCOVERY_COLUMNS", "ETLEngine"]
