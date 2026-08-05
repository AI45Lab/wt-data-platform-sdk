"""Real test-table coverage for the built-in landing-to-serving ETL stages.

The test uses a unique HASH key and always deletes and verifies its rows in
both ``landing_test`` and ``serving_test`` before the client is closed.
"""

import json
import time
import uuid

from dldb.utils import stable_hash

import wt_sdk._time as sdk_time
from wt_sdk import GatewayConfig, LandingRecord, TableConfig, WTGatewayClient
from wt_sdk.core.schemas import LANDING_PARTITIONS
from wt_sdk.etl import (
    DldbCheckpointStore,
    ETLEngine,
    SessionKey,
    TEST_CHECKPOINT_TABLE,
    build_serving_publish_pipeline,
    load_pipeline,
    resolve_etl_state_db_uri,
)


LANDING_TEST_TABLE = "landing_test"
SERVING_TEST_TABLE = "serving_test"
TEST_TABLE_CONFIG = GatewayConfig(
    tables=TableConfig(
        profile="test",
        landing_table=LANDING_TEST_TABLE,
        serving_table=SERVING_TEST_TABLE,
    )
)


def _cleanup_and_verify(client: WTGatewayClient, job_id: str) -> None:
    """Attempt both deletes and fail unless both test tables are empty."""

    filter_query = f"job_id = '{job_id}'"
    errors: list[str] = []
    for table_name, delete in (
        (LANDING_TEST_TABLE, client.delete_landing),
        (SERVING_TEST_TABLE, client.delete_serving),
    ):
        try:
            delete(filter_query)
        except Exception as exc:
            errors.append(f"{table_name} delete failed: {exc}")

    time.sleep(1)
    for table_name in (LANDING_TEST_TABLE, SERVING_TEST_TABLE):
        try:
            remaining = client.query_data(
                filter_query=filter_query,
                partition=job_id,
                table=table_name,
                checkout_latest=True,
            )
        except Exception as exc:
            errors.append(f"{table_name} cleanup verification failed: {exc}")
        else:
            if remaining:
                errors.append(
                    f"{table_name} cleanup left {len(remaining)} row(s): "
                    f"{[row.get('id') for row in remaining]}"
                )

    if errors:
        raise AssertionError("Integration cleanup failed: " + "; ".join(errors))


def test_landing_to_serving_builds_chosen_trace_and_job_tags():
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
                messages=json.dumps(messages),
                response=json.dumps(response),
                agent_model="opencode-integration-test",
                env_name="etl-integration-test",
                is_session_completed=step_id == 1,
                is_trainable=True,
                meta_json=json.dumps({"source": "etl-integration-test"}),
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
                assert row["tags"] == [
                    "integration-dataset",
                    "integration-harness",
                    "integration-model",
                    "etl-stage-test",
                ]
                assert row["source_updated_at"] is not None
                assert row["serving_updated_at"] is not None
        finally:
            _cleanup_and_verify(client, job_id)


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
        is_session_completed=False,
        is_trainable=True,
        meta_json=json.dumps({"source": "etl-incremental-integration-test"}),
    )


def test_incremental_discovers_new_rows_and_updates_previously_scanned_rows():
    suffix = uuid.uuid4().hex
    session_id = f"incremental-session-{suffix}"
    pipeline = build_serving_publish_pipeline(
        name=f"integration_incremental_{suffix}",
        version="1",
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
            updated_id = f"incremental-updated-{suffix}"
            stable_id = f"incremental-stable-{suffix}"
            new_id = f"incremental-new-{suffix}"
            client.ingest_landing_batch(
                [
                    _incremental_record(
                        record_id=updated_id,
                        job_id=job_id,
                        session_id=session_id,
                        step_id=0,
                        source_updated_at=initial_timestamp,
                        answer="answer-before-update",
                    ),
                    _incremental_record(
                        record_id=stable_id,
                        job_id=job_id,
                        session_id=session_id,
                        step_id=1,
                        source_updated_at=initial_timestamp,
                        answer="stable-answer",
                    ),
                ]
            )

            engine = ETLEngine(client, checkpoint_store=checkpoint_store)
            first_cutoff = initial_timestamp + 1_000
            first = engine.run_incremental(
                pipeline,
                settle_delay_ms=0,
                start_from_ms=initial_timestamp,
                run_started_at_ms=first_cutoff,
                run_id=f"first-{suffix}",
                buckets=[bucket],
            )
            assert first.discovery_rows == 2
            assert first.serving_rows_upserted == 2

            first_serving = client.query_data(
                filter_query=f"job_id = '{job_id}'",
                partition=job_id,
                table=SERVING_TEST_TABLE,
                checkout_latest=True,
            )
            assert {row["id"] for row in first_serving} == {updated_id, stable_id}
            first_by_id = {row["id"]: row for row in first_serving}
            assert json.loads(first_by_id[updated_id]["chosen_trace"])[-1]["content"] == (
                "answer-before-update"
            )

            client.update_landing(
                (
                    f"job_id = '{job_id}' AND session_id = '{session_id}' "
                    f"AND id = '{updated_id}'"
                ),
                {
                    "response": json.dumps(
                        {"role": "assistant", "content": "answer-after-update"}
                    )
                },
                partition=job_id,
            )
            new_record = _incremental_record(
                record_id=new_id,
                job_id=job_id,
                session_id=session_id,
                step_id=2,
                source_updated_at=sdk_time.now_ms(),
                answer="new-answer",
            )
            client.ingest_landing(new_record)

            landing_after_change = client.query_data(
                filter_query=f"job_id = '{job_id}'",
                partition=job_id,
                table=LANDING_TEST_TABLE,
                checkout_latest=True,
            )
            landing_by_id = {row["id"]: row for row in landing_after_change}
            assert landing_by_id[updated_id]["source_updated_at"] > initial_timestamp
            assert landing_by_id[stable_id]["source_updated_at"] == initial_timestamp
            second_cutoff = max(
                int(row["source_updated_at"]) for row in landing_after_change
            ) + 1_000

            second = engine.run_incremental(
                pipeline,
                settle_delay_ms=0,
                run_started_at_ms=second_cutoff,
                run_id=f"second-{suffix}",
                buckets=[bucket],
            )
            assert second.discovery_rows == 2
            # One changed/new row causes the complete session to be reloaded;
            # serving upsert keeps all three business IDs unique.
            assert second.source_rows == 3
            assert second.serving_rows_upserted == 3

            second_serving = client.query_data(
                filter_query=f"job_id = '{job_id}'",
                partition=job_id,
                table=SERVING_TEST_TABLE,
                checkout_latest=True,
            )
            assert {row["id"] for row in second_serving} == {
                updated_id,
                stable_id,
                new_id,
            }
            second_by_id = {row["id"]: row for row in second_serving}
            assert json.loads(second_by_id[updated_id]["chosen_trace"])[-1]["content"] == (
                "answer-after-update"
            )
            assert json.loads(second_by_id[new_id]["chosen_trace"])[-1]["content"] == (
                "new-answer"
            )
            assert second_by_id[updated_id]["source_updated_at"] == landing_by_id[
                updated_id
            ]["source_updated_at"]

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
                    _cleanup_and_verify(client, job_id)
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
