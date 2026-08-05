import re
from types import SimpleNamespace

from wt_sdk.etl import (
    ETLEngine,
    ETLRunFailed,
    ETLStage,
    InMemoryCheckpointStore,
    PipelineDefinition,
    PipelineMode,
    SessionKey,
)

from test_etl_pipeline import NormalizeClaudeMessagesStage, _row
from wt_sdk.etl.registry import build_serving_publish_pipeline


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
        self.next_update_time = 8_000

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
        del kwargs
        rows = list(self.rows)
        if isinstance(partition, int):
            rows = [row for row in rows if row["_bucket"] == partition]

        for field in ("job_id", "session_id", "id"):
            equal = re.search(rf"\b{field} = '((?:''|[^'])*)'", filter_query)
            if equal:
                expected = equal.group(1).replace("''", "'")
                rows = [row for row in rows if str(row.get(field)) == expected]

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
        record_id = re.search(r"\bid = '([^']+)'", filter_query).group(1)
        row = next(row for row in self.rows if row["id"] == record_id)
        row.update(updates)
        row["source_updated_at"] = self.next_update_time
        self.updated.append((record_id, dict(updates)))
        return {"updated": True}

    def upsert_serving_batch(self, records):
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

    def transform(self, record, context):
        del record, context
        return {"is_trainable": True}


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
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())
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


def test_manual_range_does_not_create_or_advance_checkpoint():
    client = FakeGatewayClient([_row(source_updated_at=1_000, _bucket=3)])
    store = InMemoryCheckpointStore()
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())
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
    pipeline = build_serving_publish_pipeline()

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
    pipeline = build_serving_publish_pipeline()

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
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())
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
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())

    summary = ETLEngine(client).run_sessions(
        pipeline,
        [SessionKey(rows[0]["job_id"], "session-1")],
    )

    assert summary.sessions_processed == 1
    assert set(client.serving) == {"wanted"}
