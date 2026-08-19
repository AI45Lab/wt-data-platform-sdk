"""Incremental, range, and targeted execution for ETL pipelines."""

import json
import logging
import time
from collections import defaultdict
from dataclasses import replace
from typing import Any, Callable, Iterable, Optional, Sequence
from uuid import uuid4

import wt_sdk._time as sdk_time
from wt_sdk.client import WTGatewayClient

from .checkpoint import CheckpointStore
from .exceptions import CheckpointError, ETLRunFailed, SessionValidationError
from .models import (
    Checkpoint,
    PipelineInputScope,
    PipelineMode,
    RecordFailure,
    RunSummary,
    SessionResult,
)
from .pipeline import PipelineDefinition
from .stage import SessionKey


DISCOVERY_COLUMNS = ["id", "job_id", "session_id", "source_updated_at"]
DEFAULT_SESSION_BATCH_SIZE = 25
DEFAULT_SINK_BATCH_SIZE = 100
DEFAULT_READ_MAX_ATTEMPTS = 3
DEFAULT_READ_RETRY_BASE_DELAY_SECONDS = 1.0

logger = logging.getLogger(__name__)


class ETLEngine:
    """Execute validated pipelines through the supported WT SDK client."""

    def __init__(
        self,
        client: WTGatewayClient,
        *,
        checkpoint_store: Optional[CheckpointStore] = None,
        session_batch_size: int = DEFAULT_SESSION_BATCH_SIZE,
        sink_batch_size: int = DEFAULT_SINK_BATCH_SIZE,
        read_max_attempts: int = DEFAULT_READ_MAX_ATTEMPTS,
        read_retry_base_delay_seconds: float = DEFAULT_READ_RETRY_BASE_DELAY_SECONDS,
    ) -> None:
        _validate_positive_int(session_batch_size, "session_batch_size")
        _validate_positive_int(sink_batch_size, "sink_batch_size")
        _validate_positive_int(read_max_attempts, "read_max_attempts")
        if read_retry_base_delay_seconds < 0:
            raise ValueError("read_retry_base_delay_seconds must be non-negative")
        self.client = client
        self.checkpoint_store = checkpoint_store
        self.session_batch_size = session_batch_size
        self.sink_batch_size = sink_batch_size
        self.read_max_attempts = read_max_attempts
        self.read_retry_base_delay_seconds = read_retry_base_delay_seconds

    def run_incremental(
        self,
        pipeline: PipelineDefinition,
        *,
        settle_delay_ms: int = 0,
        page_size: int = 1000,
        start_from_ms: Optional[int] = None,
        dry_run: bool = False,
        run_started_at_ms: Optional[int] = None,
        run_id: Optional[str] = None,
        buckets: Optional[Sequence[int]] = None,
    ) -> RunSummary:
        """Scan selected or all existing landing HASH buckets from checkpoints."""

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
        if buckets is not None:
            selected_buckets = list(buckets)
        else:
            discovery_started_ns = time.perf_counter_ns()
            try:
                selected_buckets = list(
                    self.client.list_table_partitions(table=source_table)
                )
            finally:
                summary.discovery_duration_ms += _elapsed_ms(discovery_started_ns)
        has_failures = False
        for raw_bucket in selected_buckets:
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
        summary = self._new_summary(pipeline)
        if buckets is not None:
            selected_buckets = list(buckets)
        else:
            discovery_started_ns = time.perf_counter_ns()
            try:
                selected_buckets = list(
                    self.client.list_table_partitions(table=source_table)
                )
            finally:
                summary.discovery_duration_ms += _elapsed_ms(discovery_started_ns)
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
        summary = self._new_summary(pipeline)
        if buckets is not None:
            selected_buckets = list(buckets)
        else:
            discovery_started_ns = time.perf_counter_ns()
            try:
                selected_buckets = list(
                    self.client.list_table_partitions(table=source_table)
                )
            finally:
                summary.discovery_duration_ms += _elapsed_ms(discovery_started_ns)
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

        summary = self._process_session_keys(
            pipeline,
            session_keys,
            dry_run=dry_run,
            refresh_latest=True,
        )
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
        discovery_filters = [f"job_id = '{_escape_sql(normalized_job_id)}'"]
        if pipeline.job_discovery_filter is not None:
            discovery_filters.append(f"({pipeline.job_discovery_filter})")
        summary = self._new_summary(pipeline)
        discovery_started_ns = time.perf_counter_ns()
        try:
            rows = self._query_source_with_retry(
                description=f"discover job {normalized_job_id!r}",
                filter_query=" AND ".join(discovery_filters),
                columns=["id", "job_id", "session_id"],
                partition=normalized_job_id,
                checkout_latest=True,
                table=source_table,
                exclude_none=False,
                deserialize_json=False,
            )
        except Exception as exc:
            summary.add_failure(
                _read_failure(
                    SessionKey(normalized_job_id, ""),
                    "__discovery__",
                    exc,
                )
            )
            raise ETLRunFailed(summary) from exc
        finally:
            summary.discovery_duration_ms += _elapsed_ms(discovery_started_ns)
        summary.discovery_rows += len(rows)
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
        processed = self._process_session_keys(
            pipeline,
            keys,
            dry_run=dry_run,
            refresh_latest=False,
        )
        summary.merge(processed)
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
        refresh_latest = True

        while True:
            filters = [f"({filter_query})"]
            if cursor is not None:
                filters.append(f"id > '{_escape_sql(cursor)}'")
            try:
                discovery_started_ns = time.perf_counter_ns()
                page = self._query_source_with_retry(
                    description=f"discover source bucket {bucket}",
                    filter_query=" AND ".join(filters),
                    limit=page_size,
                    columns=DISCOVERY_COLUMNS,
                    partition=bucket,
                    order_by="id",
                    ascending=True,
                    checkout_latest=refresh_latest,
                    table=source_table,
                    exclude_none=False,
                    deserialize_json=False,
                )
            except Exception as exc:
                summary.add_failure(
                    _read_failure(SessionKey("", ""), "__discovery__", exc)
                )
                break
            finally:
                summary.discovery_duration_ms += _elapsed_ms(discovery_started_ns)
            refresh_latest = False
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

            pending = session_keys - processed_sessions
            batch_summary = self._process_session_keys(
                pipeline,
                pending,
                dry_run=dry_run,
                refresh_latest=False,
            )
            summary.merge(batch_summary)
            processed_sessions.update(pending)

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
        refresh_latest = True

        while True:
            filters = [
                f"source_updated_at {start_operator} {start_ms}",
                f"source_updated_at <= {end_ms}",
            ]
            if cursor is not None:
                filters.append(f"id > '{_escape_sql(cursor)}'")
            try:
                discovery_started_ns = time.perf_counter_ns()
                page = self._query_source_with_retry(
                    description=f"discover source bucket {bucket}",
                    filter_query=" AND ".join(filters),
                    limit=page_size,
                    columns=DISCOVERY_COLUMNS,
                    partition=bucket,
                    order_by="id",
                    ascending=True,
                    checkout_latest=refresh_latest,
                    table=source_table,
                    exclude_none=False,
                    deserialize_json=False,
                )
            except Exception as exc:
                summary.add_failure(
                    _read_failure(SessionKey("", ""), "__discovery__", exc)
                )
                break
            finally:
                summary.discovery_duration_ms += _elapsed_ms(discovery_started_ns)
            refresh_latest = False
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

            pending = session_keys - processed_sessions
            batch_summary = self._process_session_keys(
                pipeline,
                pending,
                dry_run=dry_run,
                refresh_latest=False,
            )
            summary.merge(batch_summary)
            processed_sessions.update(pending)

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
            rows_by_key = self._load_session_batch(
                pipeline,
                (key,),
                checkout_latest=True,
            )
            rows = rows_by_key[key]
        except Exception as exc:
            return _session_failure_result(key, (), exc, "__session_load__")
        if not rows and pipeline.input_scope is PipelineInputScope.MATCHED_ROWS:
            return SessionResult(
                session_key=key,
                source_rows=0,
                selected_rows=0,
                successful_rows=0,
            )
        result = self._transform_loaded_session(pipeline, key, rows)
        if dry_run:
            return result
        return self._persist_session_batch(pipeline, (result,))[0]

    def _process_session_keys(
        self,
        pipeline: PipelineDefinition,
        session_keys: Iterable[SessionKey],
        *,
        dry_run: bool,
        refresh_latest: bool,
    ) -> RunSummary:
        """Batch-load sessions while preserving session-at-a-time stage execution."""

        summary = self._new_summary(pipeline)
        keys_by_job: dict[str, list[SessionKey]] = defaultdict(list)
        for key in sorted(set(session_keys)):
            keys_by_job[key.job_id].append(key)

        for job_id in sorted(keys_by_job):
            checkout_latest = refresh_latest
            job_keys = keys_by_job[job_id]
            for offset in range(0, len(job_keys), self.session_batch_size):
                batch = tuple(job_keys[offset : offset + self.session_batch_size])
                batch_failed = False
                load_started_ns = time.perf_counter_ns()
                try:
                    rows_by_key = self._load_session_batch(
                        pipeline,
                        batch,
                        checkout_latest=checkout_latest,
                    )
                except Exception as exc:
                    batch_failed = True
                    for key in batch:
                        summary.add_session(
                            _session_failure_result(
                                key,
                                (),
                                exc,
                                "__session_load__",
                            ),
                            dry_run=dry_run,
                        )
                    if checkout_latest:
                        for key in job_keys[offset + len(batch) :]:
                            summary.add_session(
                                _session_failure_result(
                                    key,
                                    (),
                                    exc,
                                    "__session_load__",
                                ),
                                dry_run=dry_run,
                            )
                finally:
                    summary.load_duration_ms += _elapsed_ms(load_started_ns)
                if not batch_failed:
                    transformed: list[SessionResult] = []
                    for key in batch:
                        rows = rows_by_key.get(key, [])
                        if not rows:
                            if pipeline.input_scope is PipelineInputScope.MATCHED_ROWS:
                                result = SessionResult(
                                    session_key=key,
                                    source_rows=0,
                                    selected_rows=0,
                                    successful_rows=0,
                                )
                            else:
                                result = _session_failure_result(
                                    key,
                                    (),
                                    SessionValidationError(f"session not found: {key}"),
                                    "__session_load__",
                                )
                        else:
                            transform_started_ns = time.perf_counter_ns()
                            try:
                                result = self._transform_loaded_session(
                                    pipeline,
                                    key,
                                    rows,
                                )
                            finally:
                                summary.transform_duration_ms += _elapsed_ms(
                                    transform_started_ns
                                )
                        transformed.append(result)
                    if dry_run:
                        persisted = transformed
                    else:
                        sink_started_ns = time.perf_counter_ns()
                        try:
                            persisted = self._persist_session_batch(
                                pipeline,
                                transformed,
                            )
                        finally:
                            summary.sink_duration_ms += _elapsed_ms(sink_started_ns)
                    for result in persisted:
                        summary.add_session(result, dry_run=dry_run)
                checkout_latest = False
                logger.info(
                    "ETL session batch completed: pipeline=%s job_id=%s "
                    "batch_sessions=%d processed_sessions=%d failed_sessions=%d",
                    pipeline.name,
                    job_id,
                    len(batch),
                    summary.sessions_processed,
                    summary.sessions_failed,
                )
                if batch_failed and refresh_latest and offset == 0:
                    break
        return summary

    def _transform_loaded_session(
        self,
        pipeline: PipelineDefinition,
        key: SessionKey,
        rows: Sequence[dict[str, object]],
    ) -> SessionResult:
        try:
            return pipeline.process_session(rows, collect_failures=True)
        except SessionValidationError as exc:
            return _session_failure_result(key, rows, exc, "__session_validation__")

    def _persist_session_batch(
        self,
        pipeline: PipelineDefinition,
        results: Sequence[SessionResult],
    ) -> list[SessionResult]:
        if pipeline.mode is PipelineMode.LANDING:
            return self._persist_landing_batch(results)
        return self._persist_serving_batch(results)

    def _persist_landing_batch(
        self,
        results: Sequence[SessionResult],
    ) -> list[SessionResult]:
        successful_patches: dict[int, list] = defaultdict(list)
        sink_failures: dict[int, list[RecordFailure]] = defaultdict(list)
        failed_counts: dict[int, int] = defaultdict(int)
        grouped: dict[str, list[tuple[int, object]]] = defaultdict(list)
        updates_by_group: dict[str, dict[str, object]] = {}

        for result_index, result in enumerate(results):
            for patch in result.landing_patches:
                group_key = json.dumps(
                    patch.updates,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                updates_by_group[group_key] = patch.updates
                grouped[group_key].append((result_index, patch))

        for group_key, entries in grouped.items():
            updates = updates_by_group[group_key]
            for offset in range(0, len(entries), self.sink_batch_size):
                chunk = entries[offset : offset + self.sink_batch_size]
                job_ids = {patch.job_id for _, patch in chunk}
                if len(job_ids) != 1:
                    raise ValueError("one landing sink batch must contain one job_id")
                job_id = next(iter(job_ids))
                ids = ", ".join(
                    f"'{_escape_sql(patch.record_id)}'" for _, patch in chunk
                )
                query = (
                    f"job_id = '{_escape_sql(job_id)}' "
                    f"AND id IN ({ids})"
                )
                try:
                    self.client.update_landing(
                        query,
                        updates,
                        partition=job_id,
                    )
                except Exception as exc:
                    for result_index, patch in chunk:
                        sink_failures[result_index].append(
                            _sink_failure(
                                patch.record_id,
                                results[result_index].session_key,
                                "__landing_sink__",
                                exc,
                            )
                        )
                        failed_counts[result_index] += 1
                else:
                    for result_index, patch in chunk:
                        successful_patches[result_index].append(patch)

        return [
            replace(
                result,
                successful_rows=result.successful_rows - failed_counts[index],
                landing_patches=tuple(successful_patches[index]),
                failures=tuple((*result.failures, *sink_failures[index])),
            )
            for index, result in enumerate(results)
        ]

    def _persist_serving_batch(
        self,
        results: Sequence[SessionResult],
    ) -> list[SessionResult]:
        entries = [
            (result_index, record)
            for result_index, result in enumerate(results)
            for record in result.serving_records
        ]
        successful_records: dict[int, list] = defaultdict(list)
        sink_failures: dict[int, list[RecordFailure]] = defaultdict(list)
        failed_counts: dict[int, int] = defaultdict(int)

        for offset in range(0, len(entries), self.sink_batch_size):
            chunk = entries[offset : offset + self.sink_batch_size]
            try:
                self.client.upsert_serving_batch(
                    [record for _, record in chunk]
                )
            except Exception as exc:
                for result_index, record in chunk:
                    sink_failures[result_index].append(
                        _sink_failure(
                            record.id,
                            results[result_index].session_key,
                            "__serving_sink__",
                            exc,
                        )
                    )
                    failed_counts[result_index] += 1
            else:
                for result_index, record in chunk:
                    successful_records[result_index].append(record)

        return [
            replace(
                result,
                successful_rows=result.successful_rows - failed_counts[index],
                serving_records=tuple(successful_records[index]),
                failures=tuple((*result.failures, *sink_failures[index])),
            )
            for index, result in enumerate(results)
        ]

    def _load_session_batch(
        self,
        pipeline: PipelineDefinition,
        keys: Sequence[SessionKey],
        *,
        checkout_latest: bool,
    ) -> dict[SessionKey, list[dict[str, object]]]:
        if not keys:
            return {}
        job_ids = {key.job_id for key in keys}
        if len(job_ids) != 1:
            raise ValueError("one session read batch must contain exactly one job_id")
        job_id = keys[0].job_id
        source_table = self.client.config.tables.landing_table
        session_values = ", ".join(
            f"'{_escape_sql(key.session_id)}'" for key in keys
        )
        query = (
            f"job_id = '{_escape_sql(job_id)}' "
            f"AND session_id IN ({session_values})"
        )
        if pipeline.input_scope is PipelineInputScope.MATCHED_ROWS:
            # MATCHED_ROWS is validated to have one safe pipeline predicate.
            # The canonical serving pipeline uses this to avoid loading the
            # non-trainable majority of each discovered session.
            query = f"{query} AND ({pipeline.job_discovery_filter})"
        rows = self._query_source_with_retry(
            description=(
                f"load {len(keys)} session(s) for job {job_id!r}"
            ),
            filter_query=query,
            partition=job_id,
            checkout_latest=checkout_latest,
            table=source_table,
            exclude_none=False,
            deserialize_json=False,
        )
        requested = set(keys)
        grouped: dict[SessionKey, list[dict[str, object]]] = {
            key: [] for key in keys
        }
        for row in rows:
            row_key = SessionKey(
                str(row.get("job_id") or "").strip(),
                str(row.get("session_id") or "").strip(),
            )
            if row_key not in requested:
                raise SessionValidationError(
                    f"batched session query returned unexpected session: {row_key}"
                )
            grouped[row_key].append(row)
        return grouped

    def _query_source_with_retry(
        self,
        *,
        description: str,
        **query_kwargs: Any,
    ) -> list[dict[str, object]]:
        for attempt in range(1, self.read_max_attempts + 1):
            try:
                return self.client.query_data(**query_kwargs)
            except Exception as exc:
                if (
                    attempt >= self.read_max_attempts
                    or not _is_transient_read_error(exc)
                ):
                    raise
                delay = self.read_retry_base_delay_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "Transient ETL source read failed; retrying: operation=%s "
                    "attempt=%d/%d delay_seconds=%.1f error=%s",
                    description,
                    attempt,
                    self.read_max_attempts,
                    delay,
                    exc,
                )
                if delay:
                    time.sleep(delay)
        raise AssertionError("unreachable read retry state")

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
    _validate_positive_int(page_size, "page_size")


def _elapsed_ms(started_ns: int) -> float:
    return max(0, time.perf_counter_ns() - started_ns) / 1_000_000


def _validate_positive_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


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


def _read_failure(
    key: SessionKey,
    stage_name: str,
    exc: Exception,
) -> RecordFailure:
    return RecordFailure(
        record_id=None,
        job_id=key.job_id,
        session_id=key.session_id,
        stage_name=stage_name,
        error_type=type(exc).__name__,
        message=str(exc),
    )


def _is_transient_read_error(exc: Exception) -> bool:
    """Best-effort classification for dldb/Lance/S3 transient read failures."""

    message = " ".join(
        str(item).lower()
        for item in (
            exc,
            getattr(exc, "__cause__", None),
            getattr(exc, "__context__", None),
        )
        if item is not None
    )
    markers = (
        "generic s3 error",
        "error sending request",
        "request timeout",
        "timed out",
        "timeout",
        "connection reset",
        "connection closed",
        "connection refused",
        "bad gateway",
        "service unavailable",
        "temporarily unavailable",
        "http status 429",
        "status code: 429",
        "http status 500",
        "status code: 500",
        "http status 502",
        "status code: 502",
        "http status 503",
        "status code: 503",
        "http status 504",
        "status code: 504",
    )
    return any(marker in message for marker in markers)


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


__all__ = [
    "DEFAULT_READ_MAX_ATTEMPTS",
    "DEFAULT_SESSION_BATCH_SIZE",
    "DEFAULT_SINK_BATCH_SIZE",
    "DISCOVERY_COLUMNS",
    "ETLEngine",
]
