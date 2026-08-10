"""Real test-table coverage for the built-in landing-to-serving ETL stages.

The test uses a unique HASH key and always deletes and verifies its rows in
both ``landing_test`` and ``serving_test`` before the client is closed.
"""

import json
import time
import uuid

from dldb.utils import stable_hash

import wt_sdk._time as sdk_time
from wt_sdk import LandingRecord, WTGatewayClient
from wt_sdk.core.schemas import LANDING_PARTITIONS
from wt_sdk.etl import (
    BuildChosenTraceStage,
    DldbCheckpointStore,
    DeriveJobTagsStage,
    ETLEngine,
    PipelineDefinition,
    PipelineMode,
    SessionKey,
    TEST_CHECKPOINT_TABLE,
    load_pipeline,
    resolve_etl_state_db_uri,
)
from wt_sdk.etl.tests.integration.helpers import (
    LANDING_TEST_TABLE,
    SERVING_TEST_TABLE,
    TEST_TABLE_CONFIG,
    cleanup_test_trajectory,
)


def test_landing_to_serving_builds_trace_search_text_and_job_tags():
    suffix = uuid.uuid4().hex
    job_id = (
        "integration-dataset#integration-harness#integration-model#etl-stage-test"
        f"#20260805#codex#{suffix}"
    )
    session_id = f"etl-session-{suffix}"
    created_at = int(time.time())
    records = []
    expected_traces: dict[str, list[dict[str, str]]] = {}

    for step_id in range(2):
        record_id = f"etl-stage-{suffix}-{step_id}"
        messages = [
            {
                "role": "user",
                "content": f"integration question {step_id}",
            }
        ]
        response = {
            "role": "assistant",
            "content": f"integration answer {step_id}",
        }
        expected_traces[record_id] = [*messages, response]
        messages_json = json.dumps(messages)
        response_json = json.dumps(response)
        meta_json = json.dumps({"source": "etl-integration-test"})
        records.append(
            LandingRecord(
                dataset_type="ETL_INTEGRATION_TEST",
                id=record_id,
                session_id=session_id,
                created_at=created_at + step_id,
                step_id=step_id,
                is_terminal=step_id == 1,
                env_id=f"etl-env-{suffix}",
                job_id=job_id,
                is_truncated=False,
                messages=messages_json,
                response=response_json,
                agent_model="opencode-integration-test",
                env_name="etl-integration-test",
                is_session_completed=step_id == 1,
                is_trainable=True,
                meta_json=meta_json,
            )
        )

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.profile == "test"
        assert client.config.tables.landing_table == LANDING_TEST_TABLE
        assert client.config.tables.serving_table == SERVING_TEST_TABLE
        try:
            client.ingest_landing_batch(records)
            summary = ETLEngine(client).run_sessions(
                load_pipeline("landing_to_serving_pipeline"),
                [SessionKey(job_id=job_id, session_id=session_id)],
            )

            assert summary.failed_rows == 0
            assert summary.source_rows == 2
            assert summary.selected_rows == 2
            assert summary.successful_rows == 2
            assert summary.serving_rows_upserted == 2

            serving_rows = client.query_data(
                filter_query=f"job_id = '{job_id}'",
                partition=job_id,
                order_by="step_id",
                table=SERVING_TEST_TABLE,
                checkout_latest=True,
            )
            assert len(serving_rows) == 2
            for row in serving_rows:
                assert json.loads(row["chosen_trace"]) == expected_traces[row["id"]]
                question = f"integration question {row['step_id']}"
                answer = f"integration answer {row['step_id']}"
                assert question in row["search_text"]
                assert answer in row["search_text"]
                assert row["search_text"].count(question) == 1
                assert row["search_text"].count(answer) == 1
                assert "opencode-integration-test" in row["search_text"]
                assert "etl-integration-test" in row["search_text"]
                assert "ETL_INTEGRATION_TEST" in row["search_text"]
                assert row["tags"] == [
                    "integration-dataset",
                    "integration-harness",
                    "integration-model",
                    "etl-stage-test",
                ]
                assert all(tag in row["search_text"] for tag in row["tags"])
                assert row["source_updated_at"] is not None
                assert row["serving_updated_at"] is not None
        finally:
            cleanup_test_trajectory(client, job_id)


def _unused_test_job_and_bucket(
    client: WTGatewayClient,
    suffix: str,
) -> tuple[str, int]:
    existing = set(client.list_table_partitions(table=LANDING_TEST_TABLE))
    for candidate_number in range(LANDING_PARTITIONS * 2):
        job_id = (
            "integration-incremental#wt-etl#opencode#change-discovery"
            f"#20260805#codex#{suffix}-{candidate_number}"
        )
        bucket = stable_hash(job_id) % LANDING_PARTITIONS
        if bucket not in existing:
            return job_id, bucket
    raise AssertionError("could not find an unused landing_test HASH bucket")


def _incremental_record(
    *,
    record_id: str,
    job_id: str,
    session_id: str,
    step_id: int,
    source_updated_at: int,
    answer: str,
    is_session_completed: bool,
    is_trainable: bool,
) -> LandingRecord:
    return LandingRecord(
        dataset_type="ETL_INCREMENTAL_INTEGRATION_TEST",
        id=record_id,
        session_id=session_id,
        created_at=source_updated_at // 1000,
        source_updated_at=source_updated_at,
        step_id=step_id,
        is_terminal=False,
        env_id=f"env-{session_id}",
        job_id=job_id,
        is_truncated=False,
        messages=json.dumps([{"role": "user", "content": f"question-{step_id}"}]),
        response=json.dumps({"role": "assistant", "content": answer}),
        agent_model="opencode-integration-test",
        env_name="etl-incremental-integration-test",
        is_session_completed=is_session_completed,
        is_trainable=is_trainable,
        meta_json=json.dumps({"source": "etl-incremental-integration-test"}),
    )


def test_serving_incremental_rediscovers_enriched_rows_and_new_rows():
    suffix = uuid.uuid4().hex
    session_id = f"incremental-session-{suffix}"
    pipeline = PipelineDefinition(
        name=f"integration_incremental_{suffix}",
        version="1",
        mode=PipelineMode.SERVING,
        stages=(
            BuildChosenTraceStage(),
            DeriveJobTagsStage(),
        ),
    )
    checkpoint_store = DldbCheckpointStore(
        resolve_etl_state_db_uri(),
        table_name=TEST_CHECKPOINT_TABLE,
    )
    checkpoint_store.verify_ready()
    job_id = ""
    bucket = -1
    cleanup_errors: list[str] = []

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        try:
            job_id, bucket = _unused_test_job_and_bucket(client, suffix)
            initial_timestamp = sdk_time.now_ms() - 10_000
            client.ingest_landing_batch(
                [
                    _incremental_record(
                        record_id=f"incremental-old-{suffix}-0",
                        job_id=job_id,
                        session_id=session_id,
                        step_id=0,
                        source_updated_at=initial_timestamp,
                        answer="old-answer-0",
                        is_session_completed=False,
                        is_trainable=False,
                    ),
                    _incremental_record(
                        record_id=f"incremental-old-{suffix}-1",
                        job_id=job_id,
                        session_id=session_id,
                        step_id=1,
                        source_updated_at=initial_timestamp,
                        answer="old-answer-1",
                        is_session_completed=False,
                        is_trainable=False,
                    ),
                    _incremental_record(
                        record_id=f"incremental-old-{suffix}-2",
                        job_id=job_id,
                        session_id=session_id,
                        step_id=2,
                        source_updated_at=initial_timestamp,
                        answer="old-answer-2",
                        is_session_completed=True,
                        is_trainable=False,
                    ),
                ]
            )
            old_ids = {f"incremental-old-{suffix}-{index}" for index in range(3)}
            new_ids = {f"incremental-new-{suffix}-{index}" for index in range(3, 6)}

            engine = ETLEngine(client, checkpoint_store=checkpoint_store)
            first_cutoff = initial_timestamp + 1_000
            first = engine.run_incremental(
                pipeline,
                start_from_ms=initial_timestamp,
                run_started_at_ms=first_cutoff,
                run_id=f"first-{suffix}",
                buckets=[bucket],
                page_size=2,
            )
            assert first.discovery_rows == 3
            assert first.source_rows == 3
            assert first.selected_rows == 0
            assert first.serving_rows_upserted == 0

            enrichment_pipeline = load_pipeline("landing_enrichment_pipeline")
            enrichment = engine.run_sessions(
                enrichment_pipeline,
                [SessionKey(job_id=job_id, session_id=session_id)],
            )
            assert enrichment.source_rows == 3
            assert enrichment.landing_rows_updated == 3
            assert enrichment.dirty_sessions == {
                SessionKey(job_id=job_id, session_id=session_id)
            }

            new_timestamp = sdk_time.now_ms()
            client.ingest_landing_batch(
                [
                    _incremental_record(
                        record_id=f"incremental-new-{suffix}-{step_id}",
                        job_id=job_id,
                        session_id=session_id,
                        step_id=step_id,
                        source_updated_at=new_timestamp,
                        answer=f"new-answer-{step_id}",
                        is_session_completed=False,
                        is_trainable=True,
                    )
                    for step_id in range(3, 6)
                ]
            )

            landing_after_change = client.query_data(
                filter_query=f"job_id = '{job_id}'",
                partition=job_id,
                table=LANDING_TEST_TABLE,
                checkout_latest=True,
            )
            landing_by_id = {row["id"]: row for row in landing_after_change}
            assert set(landing_by_id) == old_ids | new_ids
            for record_id in old_ids:
                assert landing_by_id[record_id]["is_trainable"] is True
                assert landing_by_id[record_id]["source_updated_at"] > first_cutoff
            for record_id in new_ids:
                assert landing_by_id[record_id]["is_trainable"] is True
                assert landing_by_id[record_id]["source_updated_at"] == new_timestamp
            second_cutoff = max(
                int(row["source_updated_at"]) for row in landing_after_change
            ) + 1_000

            second = engine.run_incremental(
                pipeline,
                run_started_at_ms=second_cutoff,
                run_id=f"second-{suffix}",
                buckets=[bucket],
                page_size=2,
            )
            # All three old rows moved past the serving watermark when the
            # landing pipeline refreshed source_updated_at, and all three new
            # rows are in the same incremental window.
            assert second.discovery_rows == 6
            assert second.source_rows == 6
            assert second.selected_rows == 6
            assert second.serving_rows_upserted == 6

            second_serving = client.query_data(
                filter_query=f"job_id = '{job_id}'",
                partition=job_id,
                table=SERVING_TEST_TABLE,
                checkout_latest=True,
            )
            assert {row["id"] for row in second_serving} == old_ids | new_ids
            second_by_id = {row["id"]: row for row in second_serving}
            for step_id in range(6):
                prefix = "old" if step_id < 3 else "new"
                record_id = f"incremental-{prefix}-{suffix}-{step_id}"
                assert json.loads(second_by_id[record_id]["chosen_trace"])[-1][
                    "content"
                ] == f"{prefix}-answer-{step_id}"
                assert second_by_id[record_id]["source_updated_at"] == landing_by_id[
                    record_id
                ]["source_updated_at"]
                assert second_by_id[record_id]["tags"] == [
                    "integration-incremental",
                    "wt-etl",
                    "opencode",
                    "change-discovery",
                ]

            checkpoint = checkpoint_store.load(
                pipeline_name=pipeline.name,
                pipeline_version=pipeline.version,
                source_table=LANDING_TEST_TABLE,
                target_table=SERVING_TEST_TABLE,
                bucket=bucket,
            )
            assert checkpoint is not None
            assert checkpoint.committed_until_ms == second_cutoff
            assert checkpoint.last_run_id == f"second-{suffix}"
            assert checkpoint.status == "IDLE"
        finally:
            if job_id:
                try:
                    cleanup_test_trajectory(client, job_id)
                except Exception as exc:
                    cleanup_errors.append(f"trajectory tables: {exc}")
            if bucket >= 0:
                try:
                    checkpoint_store.delete(
                        pipeline_name=pipeline.name,
                        pipeline_version=pipeline.version,
                        source_table=LANDING_TEST_TABLE,
                        target_table=SERVING_TEST_TABLE,
                        bucket=bucket,
                    )
                    assert checkpoint_store.load(
                        pipeline_name=pipeline.name,
                        pipeline_version=pipeline.version,
                        source_table=LANDING_TEST_TABLE,
                        target_table=SERVING_TEST_TABLE,
                        bucket=bucket,
                    ) is None
                except Exception as exc:
                    cleanup_errors.append(f"checkpoint table: {exc}")
            try:
                checkpoint_store.close()
            except Exception as exc:
                cleanup_errors.append(f"checkpoint close: {exc}")

    if cleanup_errors:
        raise AssertionError("Incremental integration cleanup failed: " + "; ".join(cleanup_errors))
