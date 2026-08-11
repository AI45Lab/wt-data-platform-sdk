import re
from types import SimpleNamespace

import pytest

from wt_sdk.etl import (
    ETLEngine,
    ETLRunFailed,
    ETLStage,
    InMemoryCheckpointStore,
    PipelineDefinition,
    PipelineMode,
    SessionKey,
    load_pipeline,
)

from wt_sdk.etl.tests.unit.test_pipeline import _row


class FakeGatewayClient:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]
        self.config = SimpleNamespace(
            tables=SimpleNamespace(
                landing_table="landing_test",
                serving_table="serving_test",
            )
        )
        self.updated = []
        self.serving = {}
        self.serving_batches = []
        self.next_update_time = 8_000
        self.query_calls = []

    def list_table_partitions(self, table=None):
        del table
        return sorted({row["_bucket"] for row in self.rows})

    def query_data(
        self,
        filter_query,
        *,
        limit=None,
        columns=None,
        partition=None,
        order_by=None,
        ascending=True,
        **kwargs,
    ):
        self.query_calls.append(
            {
                "filter_query": filter_query,
                "partition": partition,
                "checkout_latest": kwargs.get("checkout_latest"),
                "columns": columns,
            }
        )
        rows = list(self.rows)
        if isinstance(partition, int):
            rows = [row for row in rows if row["_bucket"] == partition]

        for field in ("job_id", "session_id", "id"):
            equal = re.search(rf"\b{field} = '((?:''|[^'])*)'", filter_query)
            if equal:
                expected = equal.group(1).replace("''", "'")
                rows = [row for row in rows if str(row.get(field)) == expected]

        session_in = re.search(r"\bsession_id IN \((.*?)\)", filter_query)
        if session_in:
            expected_sessions = {
                value.replace("''", "'")
                for value in re.findall(r"'((?:''|[^'])*)'", session_in.group(1))
            }
            rows = [
                row for row in rows
                if str(row.get("session_id")) in expected_sessions
            ]
        if re.search(r"\bis_trainable\s*=\s*true\b", filter_query, re.IGNORECASE):
            rows = [row for row in rows if row.get("is_trainable") is True]
        if re.search(
            r"\bis_session_completed\s*=\s*true\b",
            filter_query,
            re.IGNORECASE,
        ):
            rows = [row for row in rows if row.get("is_session_completed") is True]

        lower = re.search(r"source_updated_at (>=|>) (\d+)", filter_query)
        upper = re.search(r"source_updated_at <= (\d+)", filter_query)
        id_cursor = re.search(r"\bid > '((?:''|[^'])*)'", filter_query)
        if lower:
            threshold = int(lower.group(2))
            if lower.group(1) == ">=":
                rows = [row for row in rows if row["source_updated_at"] >= threshold]
            else:
                rows = [row for row in rows if row["source_updated_at"] > threshold]
        if upper:
            rows = [row for row in rows if row["source_updated_at"] <= int(upper.group(1))]
        if id_cursor:
            cursor = id_cursor.group(1).replace("''", "'")
            rows = [row for row in rows if row["id"] > cursor]

        if order_by:
            rows.sort(key=lambda row: row[order_by], reverse=not ascending)
        if limit is not None:
            rows = rows[:limit]
        if columns:
            return [{column: row.get(column) for column in columns} for row in rows]
        return [{key: value for key, value in row.items() if key != "_bucket"} for row in rows]

    def update_landing(self, filter_query, updates, partition=None):
        del partition
        id_in = re.search(r"\bid IN \((.*?)\)", filter_query)
        if id_in:
            record_ids = [
                value.replace("''", "'")
                for value in re.findall(r"'((?:''|[^'])*)'", id_in.group(1))
            ]
        else:
            record_ids = [
                re.search(r"\bid = '((?:''|[^'])*)'", filter_query)
                .group(1)
                .replace("''", "'")
            ]
        for row in self.rows:
            if row["id"] in record_ids:
                row.update(updates)
                row["source_updated_at"] = self.next_update_time
        self.updated.append((tuple(record_ids), dict(updates)))
        return {"updated": True}

    def upsert_serving_batch(self, records):
        self.serving_batches.append(tuple(record.id for record in records))
        for record in records:
            self.serving[record.id] = record


class FailingServingClient(FakeGatewayClient):
    def __init__(self, rows):
        super().__init__(rows)
        self.fail = True

    def upsert_serving_batch(self, records):
        if self.fail:
            raise RuntimeError("simulated serving failure")
        super().upsert_serving_batch(records)


class MarkTrainableStage(ETLStage):
    name = "mark_trainable"
    output_fields = ("is_trainable",)

    def transform_session(self, session, context):
        del context
        return {
            record["id"]: {"is_trainable": True}
            for record in session
        }


def _checkpoint(store, pipeline, bucket=3):
    return store.load(
        pipeline_name=pipeline.name,
        pipeline_version=pipeline.version,
        source_table="landing_test",
        target_table=(
            "landing_test"
            if pipeline.mode is PipelineMode.LANDING
            else "serving_test"
        ),
        bucket=bucket,
    )


def test_incremental_serving_run_commits_checkpoint_after_upsert():
    rows = [
        _row(id="row-1", step_id=0, source_updated_at=1_000, _bucket=3),
        _row(id="row-2", step_id=1, source_updated_at=1_100, _bucket=3),
    ]
    client = FakeGatewayClient(rows)
    store = InMemoryCheckpointStore()
    pipeline = load_pipeline("landing_to_serving_pipeline")
    engine = ETLEngine(client, checkpoint_store=store)

    summary = engine.run_incremental(
        pipeline,
        settle_delay_ms=0,
        page_size=1,
        start_from_ms=0,
        run_started_at_ms=5_000,
        run_id="serving-publish-run-1",
    )

    assert summary.discovery_rows == 2
    assert summary.sessions_processed == 1
    assert summary.serving_rows_upserted == 2
    assert set(client.serving) == {"row-1", "row-2"}
    checkpoint = _checkpoint(store, pipeline)
    assert checkpoint.committed_until_ms == 5_000
    assert checkpoint.status == "IDLE"
    assert checkpoint.last_run_id == "serving-publish-run-1"
    assert checkpoint.active_window_end_ms is None

    second = engine.run_incremental(
        pipeline,
        settle_delay_ms=0,
        run_started_at_ms=6_000,
    )
    assert second.discovery_rows == 0
    assert _checkpoint(store, pipeline).committed_until_ms == 6_000


def test_incremental_default_cutoff_is_the_frozen_run_start_without_delay():
    rows = [
        _row(
            id="at-cutoff",
            session_id="at-cutoff-session",
            source_updated_at=5_000,
            _bucket=3,
        ),
        _row(
            id="after-cutoff",
            session_id="after-cutoff-session",
            source_updated_at=5_001,
            _bucket=3,
        ),
    ]
    client = FakeGatewayClient(rows)
    store = InMemoryCheckpointStore()
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(client, checkpoint_store=store).run_incremental(
        pipeline,
        start_from_ms=0,
        run_started_at_ms=5_000,
        buckets=[3],
    )

    assert summary.discovery_rows == 1
    assert set(client.serving) == {"at-cutoff"}
    assert _checkpoint(store, pipeline).committed_until_ms == 5_000


def test_incremental_can_be_limited_to_selected_hash_buckets():
    rows = [
        _row(
            id="bucket-3",
            session_id="session-3",
            source_updated_at=1_000,
            _bucket=3,
        ),
        _row(
            id="bucket-4",
            session_id="session-4",
            source_updated_at=1_000,
            _bucket=4,
        ),
    ]
    client = FakeGatewayClient(rows)
    store = InMemoryCheckpointStore()
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(client, checkpoint_store=store).run_incremental(
        pipeline,
        settle_delay_ms=0,
        start_from_ms=0,
        run_started_at_ms=5_000,
        buckets=[4],
    )

    assert summary.buckets_scanned == 1
    assert set(client.serving) == {"bucket-4"}
    assert _checkpoint(store, pipeline, bucket=3) is None
    assert _checkpoint(store, pipeline, bucket=4) is not None


def test_landing_timestamp_echo_is_harmless_because_diff_is_empty():
    client = FakeGatewayClient(
        [_row(is_trainable=False, source_updated_at=1_000, _bucket=3)]
    )
    store = InMemoryCheckpointStore()
    pipeline = PipelineDefinition(
        name="landing_enrichment",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(MarkTrainableStage(),),
    )
    engine = ETLEngine(client, checkpoint_store=store)

    first = engine.run_incremental(
        pipeline,
        settle_delay_ms=0,
        start_from_ms=0,
        run_started_at_ms=5_000,
    )
    assert first.landing_rows_updated == 1
    assert first.dirty_sessions == {
        SessionKey(
            "dataset#harness#model#task#20260805#owner#extra",
            "session-1",
        )
    }
    assert len(client.updated) == 1

    second = engine.run_incremental(
        pipeline,
        settle_delay_ms=0,
        run_started_at_ms=9_000,
    )
    assert second.discovery_rows == 1
    assert second.landing_rows_updated == 0
    assert len(client.updated) == 1
    assert _checkpoint(store, pipeline).committed_until_ms == 9_000


def test_serving_checkpoint_rediscovers_enriched_old_rows_and_new_rows():
    old_rows = [
        _row(
            id=f"old-{step_id}",
            step_id=step_id,
            is_trainable=False,
            source_updated_at=1_000,
            _bucket=3,
        )
        for step_id in range(3)
    ]
    client = FakeGatewayClient(old_rows)
    store = InMemoryCheckpointStore()
    serving_pipeline = load_pipeline("landing_to_serving_pipeline")
    enrichment_pipeline = PipelineDefinition(
        name="landing_enrichment_pipeline",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(MarkTrainableStage(),),
    )
    engine = ETLEngine(client, checkpoint_store=store)

    first = engine.run_incremental(
        serving_pipeline,
        settle_delay_ms=0,
        page_size=2,
        start_from_ms=1_000,
        run_started_at_ms=5_000,
        buckets=[3],
    )
    assert first.discovery_rows == 3
    assert first.selected_rows == 0
    assert client.serving == {}
    assert _checkpoint(store, serving_pipeline).committed_until_ms == 5_000

    enrichment = engine.run_sessions(
        enrichment_pipeline,
        [SessionKey(old_rows[0]["job_id"], old_rows[0]["session_id"])],
    )
    assert enrichment.landing_rows_updated == 3
    assert {row["source_updated_at"] for row in client.rows} == {8_000}

    client.rows.extend(
        [
            _row(
                id=f"new-{step_id}",
                step_id=step_id,
                is_trainable=True,
                source_updated_at=8_100,
                _bucket=3,
            )
            for step_id in range(3, 6)
        ]
    )
    second = engine.run_incremental(
        serving_pipeline,
        settle_delay_ms=0,
        page_size=2,
        run_started_at_ms=9_000,
        buckets=[3],
    )

    assert second.discovery_rows == 6
    assert second.source_rows == 6
    assert second.selected_rows == 6
    assert second.serving_rows_upserted == 6
    assert set(client.serving) == {
        "old-0",
        "old-1",
        "old-2",
        "new-3",
        "new-4",
        "new-5",
    }
    assert _checkpoint(store, serving_pipeline).committed_until_ms == 9_000


def test_manual_range_does_not_create_or_advance_checkpoint():
    client = FakeGatewayClient([_row(source_updated_at=1_000, _bucket=3)])
    store = InMemoryCheckpointStore()
    pipeline = load_pipeline("landing_to_serving_pipeline")
    engine = ETLEngine(client, checkpoint_store=store)

    summary = engine.run_range(pipeline, start_ms=0, end_ms=2_000)

    assert summary.serving_rows_upserted == 1
    assert _checkpoint(store, pipeline) is None


def test_manual_source_filter_loads_each_full_session_only_once_across_pages():
    rows = [
        _row(id="row-1", step_id=0, session_id="session-1", _bucket=3),
        _row(id="row-2", step_id=1, session_id="session-1", _bucket=3),
        _row(id="row-3", step_id=0, session_id="session-2", _bucket=3),
    ]
    client = FakeGatewayClient(rows)
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(client).run_filter(
        pipeline,
        "session_id = 'session-1'",
        page_size=1,
        dry_run=True,
    )

    assert summary.discovery_rows == 2
    assert summary.sessions_processed == 1
    assert summary.source_rows == 2
    assert summary.successful_rows == 2


def test_multiple_jobs_are_supported_in_one_manual_run():
    first = _row(id="row-1", job_id="job-a", session_id="session-a", _bucket=3)
    second = _row(id="row-2", job_id="job-b", session_id="session-b", _bucket=4)
    client = FakeGatewayClient([first, second])
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(client).run_jobs(
        pipeline,
        ["job-a", "job-b"],
        dry_run=True,
    )

    assert summary.sessions_processed == 2
    assert summary.source_rows == 2


def test_failed_write_keeps_resumable_active_window_and_does_not_advance_watermark():
    client = FailingServingClient([_row(source_updated_at=1_000, _bucket=3)])
    store = InMemoryCheckpointStore()
    pipeline = load_pipeline("landing_to_serving_pipeline")
    engine = ETLEngine(client, checkpoint_store=store)

    try:
        engine.run_incremental(
            pipeline,
            settle_delay_ms=0,
            start_from_ms=0,
            run_started_at_ms=5_000,
        )
    except ETLRunFailed as exc:
        assert exc.summary.failed_rows == 1
        assert exc.summary.successful_rows == 0
        assert exc.summary.failures[0].record_id == "row-1"
        assert exc.summary.failures[0].stage_name == "__serving_sink__"
    else:
        raise AssertionError("expected the simulated serving write to fail")

    failed = _checkpoint(store, pipeline)
    assert failed.status == "FAILED"
    assert failed.committed_until_ms == -1
    assert failed.active_window_start_ms == -1
    assert failed.active_window_end_ms == 5_000

    client.fail = False
    resumed = engine.run_incremental(
        pipeline,
        settle_delay_ms=0,
        run_started_at_ms=9_000,
    )
    assert resumed.serving_rows_upserted == 1
    assert resumed.buckets_scanned == 1
    assert _checkpoint(store, pipeline).committed_until_ms == 9_000


def test_targeted_session_uses_job_and_session_scope():
    rows = [
        _row(id="wanted", session_id="session-1", _bucket=3),
        _row(id="other", session_id="session-2", _bucket=3),
    ]
    client = FakeGatewayClient(rows)
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(client).run_sessions(
        pipeline,
        [SessionKey(rows[0]["job_id"], "session-1")],
    )

    assert summary.sessions_processed == 1
    assert set(client.serving) == {"wanted"}


def test_job_discovery_uses_stage_hint_and_batches_complete_session_reads():
    rows = []
    for session_number in range(3):
        for step_id in range(2):
            rows.append(
                _row(
                    id=f"row-{session_number}-{step_id}",
                    session_id=f"session-{session_number}",
                    step_id=step_id,
                    is_trainable=session_number < 2,
                    _bucket=3,
                )
            )
    client = FakeGatewayClient(rows)
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(client, session_batch_size=2).run_job(
        pipeline,
        rows[0]["job_id"],
        dry_run=True,
    )

    assert summary.discovery_rows == 4
    assert summary.sessions_processed == 2
    assert summary.source_rows == 4
    assert len(client.query_calls) == 2
    discovery, batch_load = client.query_calls
    assert "is_trainable = true" in discovery["filter_query"]
    assert discovery["checkout_latest"] is True
    assert "session_id IN" in batch_load["filter_query"]
    assert batch_load["checkout_latest"] is False


def test_landing_job_discovery_reads_only_completed_session_markers():
    rows = []
    for session_number in range(3):
        for step_id in range(2):
            rows.append(
                _row(
                    id=f"row-{session_number}-{step_id}",
                    session_id=f"session-{session_number}",
                    step_id=step_id,
                    is_session_completed=step_id == 1,
                    _bucket=3,
                )
            )
    client = FakeGatewayClient(rows)
    pipeline = load_pipeline("landing_enrichment_pipeline")

    summary = ETLEngine(client, session_batch_size=2).run_job(
        pipeline,
        rows[0]["job_id"],
        dry_run=True,
    )

    assert summary.discovery_rows == 3
    assert summary.sessions_processed == 3
    assert summary.source_rows == 6
    assert "is_session_completed = true" in client.query_calls[0]["filter_query"]
    assert len(client.query_calls) == 3


def test_bucket_scan_refreshes_latest_snapshot_only_once():
    rows = [
        _row(id="row-1", session_id="session-1", _bucket=3),
        _row(id="row-2", session_id="session-2", _bucket=3),
    ]
    client = FakeGatewayClient(rows)
    pipeline = load_pipeline("landing_to_serving_pipeline")

    ETLEngine(client, session_batch_size=1).run_filter(
        pipeline,
        "is_trainable = true",
        page_size=1,
        dry_run=True,
    )

    assert sum(call["checkout_latest"] is True for call in client.query_calls) == 1
    assert client.query_calls[0]["checkout_latest"] is True
    assert all(
        call["checkout_latest"] is False
        for call in client.query_calls[1:]
    )


def test_transient_session_read_is_retried_then_succeeds():
    class FlakyReadClient(FakeGatewayClient):
        def __init__(self, rows):
            super().__init__(rows)
            self.remaining_failures = 2

        def query_data(self, filter_query, **kwargs):
            if "session_id IN" in filter_query and self.remaining_failures:
                self.remaining_failures -= 1
                self.query_calls.append(
                    {
                        "filter_query": filter_query,
                        "partition": kwargs.get("partition"),
                        "checkout_latest": kwargs.get("checkout_latest"),
                        "columns": kwargs.get("columns"),
                    }
                )
                raise ValueError("Generic S3 error: error sending request")
            return super().query_data(filter_query, **kwargs)

    row = _row(id="row-1", session_id="session-1", _bucket=3)
    client = FlakyReadClient([row])
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(
        client,
        read_max_attempts=3,
        read_retry_base_delay_seconds=0,
    ).run_job(pipeline, row["job_id"], dry_run=True)

    assert summary.sessions_processed == 1
    assert summary.failed_rows == 0
    assert client.remaining_failures == 0
    assert len(client.query_calls) == 4


def test_exhausted_read_retry_preserves_prior_session_progress():
    class SecondSessionFailsClient(FakeGatewayClient):
        def query_data(self, filter_query, **kwargs):
            if "session_id IN ('session-2')" in filter_query:
                self.query_calls.append(
                    {
                        "filter_query": filter_query,
                        "partition": kwargs.get("partition"),
                        "checkout_latest": kwargs.get("checkout_latest"),
                        "columns": kwargs.get("columns"),
                    }
                )
                raise ValueError("Generic S3 error: request timeout")
            return super().query_data(filter_query, **kwargs)

    rows = [
        _row(id="row-1", session_id="session-1", _bucket=3),
        _row(id="row-2", session_id="session-2", _bucket=3),
    ]
    client = SecondSessionFailsClient(rows)
    pipeline = load_pipeline("landing_to_serving_pipeline")
    engine = ETLEngine(
        client,
        session_batch_size=1,
        read_max_attempts=3,
        read_retry_base_delay_seconds=0,
    )

    with pytest.raises(ETLRunFailed) as caught:
        engine.run_job(pipeline, rows[0]["job_id"], dry_run=True)

    summary = caught.value.summary
    assert summary.sessions_processed == 2
    assert summary.sessions_failed == 1
    assert summary.source_rows == 1
    assert summary.successful_rows == 1
    assert summary.failed_rows == 1
    assert summary.failures[0].session_id == "session-2"
    assert summary.failures[0].stage_name == "__session_load__"


def test_landing_sink_coalesces_equal_patches_across_sessions():
    rows = [
        _row(
            id=f"row-{index}",
            session_id=f"session-{index}",
            is_trainable=False,
            _bucket=3,
        )
        for index in range(3)
    ]
    client = FakeGatewayClient(rows)
    pipeline = PipelineDefinition(
        name="landing_batch",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(MarkTrainableStage(),),
    )

    summary = ETLEngine(
        client,
        session_batch_size=3,
        sink_batch_size=100,
    ).run_sessions(
        pipeline,
        [SessionKey(row["job_id"], row["session_id"]) for row in rows],
    )

    assert summary.landing_rows_updated == 3
    assert len(client.updated) == 1
    assert set(client.updated[0][0]) == {"row-0", "row-1", "row-2"}
    assert client.updated[0][1] == {"is_trainable": True}
    assert all(row["is_trainable"] is True for row in client.rows)


def test_serving_sink_upserts_records_from_multiple_sessions_in_one_batch():
    rows = [
        _row(id=f"row-{index}", session_id=f"session-{index}", _bucket=3)
        for index in range(3)
    ]
    client = FakeGatewayClient(rows)
    pipeline = load_pipeline("landing_to_serving_pipeline")

    summary = ETLEngine(
        client,
        session_batch_size=3,
        sink_batch_size=100,
    ).run_sessions(
        pipeline,
        [SessionKey(row["job_id"], row["session_id"]) for row in rows],
    )

    assert summary.serving_rows_upserted == 3
    assert client.serving_batches == [("row-0", "row-1", "row-2")]


def test_serving_sink_batch_is_bounded_and_preserves_partial_progress():
    class FailSecondServingBatchClient(FakeGatewayClient):
        def __init__(self, rows):
            super().__init__(rows)
            self.calls = 0

        def upsert_serving_batch(self, records):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("simulated second serving batch failure")
            super().upsert_serving_batch(records)

    rows = [
        _row(id=f"row-{index}", session_id=f"session-{index}", _bucket=3)
        for index in range(3)
    ]
    client = FailSecondServingBatchClient(rows)
    pipeline = load_pipeline("landing_to_serving_pipeline")
    engine = ETLEngine(
        client,
        session_batch_size=3,
        sink_batch_size=2,
    )

    with pytest.raises(ETLRunFailed) as caught:
        engine.run_sessions(
            pipeline,
            [SessionKey(row["job_id"], row["session_id"]) for row in rows],
        )

    summary = caught.value.summary
    assert client.calls == 2
    assert set(client.serving) == {"row-0", "row-1"}
    assert summary.serving_rows_upserted == 2
    assert summary.successful_rows == 2
    assert summary.failed_rows == 1
    assert summary.failures[0].record_id == "row-2"
    assert summary.failures[0].stage_name == "__serving_sink__"
