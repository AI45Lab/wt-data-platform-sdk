"""Canonical landing-enrichment integration coverage on test tables."""

import json
import time
import uuid

from dldb.utils import stable_hash

from wt_sdk import LandingRecord, WTGatewayClient
from wt_sdk.core.schemas import LANDING_PARTITIONS, SERVING_PARTITIONS
from wt_sdk.etl import (
    ETLEngine,
    SessionKey,
    StageContext,
    UpdateIsTrainableStage,
    load_pipeline,
)
from wt_sdk.etl.tests.integration.helpers import (
    LANDING_TEST_TABLE,
    SERVING_TEST_TABLE,
    TEST_TABLE_CONFIG,
    cleanup_test_trajectory,
)


def _test_records(
    job_id: str,
    session_id: str,
    suffix: str,
    source_updated_at: int,
) -> list[LandingRecord]:
    first_message = {"role": "user", "content": "integration task"}
    first_response = {"role": "assistant", "content": "integration answer"}
    follow_up = {"role": "user", "content": "integration follow-up"}
    common = {
        "dataset_type": "ETL_TRAINABILITY_INTEGRATION_TEST",
        "session_id": session_id,
        "env_id": f"trainability-env-{suffix}",
        "job_id": job_id,
        "is_truncated": False,
        "agent_model": "trainability-integration-model",
        "env_name": "trainability-integration-test",
        "is_trainable": False,
        "meta_json": json.dumps({"source": "trainability-integration-test"}),
    }
    created_at = source_updated_at // 1000
    return [
        LandingRecord(
            **common,
            id=f"trainability-non-matching-{suffix}",
            created_at=created_at,
            source_updated_at=source_updated_at,
            step_id=0,
            is_terminal=False,
            messages=json.dumps([first_message]),
            response=json.dumps(first_response),
            is_session_completed=False,
        ),
        LandingRecord(
            **common,
            id=f"trainability-matching-{suffix}",
            created_at=created_at + 1,
            source_updated_at=source_updated_at,
            step_id=1,
            is_terminal=True,
            messages=json.dumps([first_message, first_response, follow_up]),
            response=json.dumps(
                {"role": "assistant", "content": "integration final answer"}
            ),
            is_session_completed=True,
        ),
    ]


def _query_test_rows(
    client: WTGatewayClient,
    table: str,
    job_id: str,
) -> list[dict[str, object]]:
    partition_count = (
        LANDING_PARTITIONS if table == LANDING_TEST_TABLE else SERVING_PARTITIONS
    )
    bucket = stable_hash(job_id) % partition_count
    if bucket not in set(client.list_table_partitions(table=table)):
        return []

    escaped_job_id = job_id.replace("'", "''")
    return client.query_data(
        filter_query=f"job_id = '{escaped_job_id}'",
        partition=job_id,
        table=table,
        checkout_latest=True,
        exclude_none=False,
    )


def _row_timestamps(rows: list[dict[str, object]]) -> dict[object, object]:
    return {row["id"]: row["source_updated_at"] for row in rows}


def _shuffled_step_records(
    job_id: str,
    session_id: str,
    suffix: str,
    source_updated_at: int,
) -> list[LandingRecord]:
    messages = [
        {"role": "user", "content": "shuffled step task"},
        {"role": "assistant", "content": "first response"},
        {"role": "user", "content": "follow-up"},
        {"role": "assistant", "content": "final response"},
    ]
    common = {
        "dataset_type": "ETL_SHUFFLED_STEP_INTEGRATION_TEST",
        "session_id": session_id,
        "env_id": f"shuffled-step-env-{suffix}",
        "job_id": job_id,
        "is_truncated": False,
        "agent_model": "shuffled-step-integration-model",
        "env_name": "shuffled-step-integration-test",
        "is_trainable": False,
        "meta_json": json.dumps(
            {
                "source": "shuffled-step-integration-test",
                "env_state": json.dumps({"status_code": 200}),
            }
        ),
    }
    created_at = source_updated_at // 1000
    records_by_step = {
        step_id: LandingRecord(
            **common,
            id=f"shuffled-step-{step_id}-{suffix}",
            created_at=created_at + index,
            source_updated_at=source_updated_at,
            step_id=step_id,
            is_terminal=step_id == 40,
            messages=json.dumps(messages[:index]),
            is_session_completed=step_id == 40,
            reward=0.875 if step_id == 40 else None,
        )
        for index, step_id in enumerate((10, 20, 30, 40), start=1)
    }
    return [records_by_step[step_id] for step_id in (30, 10, 40, 20)]


def _stage_context(job_id: str, session_id: str) -> StageContext:
    return StageContext(
        pipeline_name="landing_enrichment_pipeline",
        pipeline_version="1",
        session_key=SessionKey(job_id, session_id),
    )


def test_trainability_stage_inside_canonical_landing_pipeline():
    suffix = f"{uuid.uuid4().hex}_mock_xqer"
    job_id = (
        "landing-enrichment#integration#model#trainability"
        f"#20260806#developer#{suffix}"
    )
    session_id = f"landing-enrichment-session-{suffix}"
    initial_timestamp = int(time.time() * 1000) - 10_000

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.profile == "test"
        assert client.config.tables.landing_table == LANDING_TEST_TABLE
        assert client.config.tables.serving_table == SERVING_TEST_TABLE
        try:
            client.ingest_landing_batch(
                _test_records(job_id, session_id, suffix, initial_timestamp)
            )
            before = _query_test_rows(client, LANDING_TEST_TABLE, job_id)
            assert len(before) == 2
            before_by_id = {row["id"]: row for row in before}
            matching_id = f"trainability-matching-{suffix}"
            non_matching_id = f"trainability-non-matching-{suffix}"

            first = ETLEngine(client).run_sessions(
                load_pipeline("landing_enrichment_pipeline"),
                [SessionKey(job_id, session_id)],
            )
            assert first.failed_rows == 0
            assert first.source_rows == 2
            assert first.landing_rows_updated == 1

            after_first = _query_test_rows(client, LANDING_TEST_TABLE, job_id)
            after_first_by_id = {row["id"]: row for row in after_first}
            assert after_first_by_id[matching_id]["is_trainable"] is True
            assert (
                after_first_by_id[matching_id]["source_updated_at"]
                > before_by_id[matching_id]["source_updated_at"]
            )
            assert after_first_by_id[non_matching_id] == before_by_id[non_matching_id]
            assert _query_test_rows(client, SERVING_TEST_TABLE, job_id) == []

            print(
                "\nCanonical landing enrichment first run: "
                f"failed_rows={first.failed_rows}, "
                f"landing_rows_updated={first.landing_rows_updated}, "
                f"matching_is_trainable="
                f"{after_first_by_id[matching_id]['is_trainable']}, "
                f"matching_source_updated_at="
                f"{before_by_id[matching_id]['source_updated_at']}->"
                f"{after_first_by_id[matching_id]['source_updated_at']}, "
                "non_matching_unchanged=True, serving_rows=0"
            )

            second = ETLEngine(client).run_sessions(
                load_pipeline("landing_enrichment_pipeline"),
                [SessionKey(job_id, session_id)],
            )
            assert second.failed_rows == 0
            assert second.landing_rows_updated == 0

            after_second = _query_test_rows(client, LANDING_TEST_TABLE, job_id)
            assert _row_timestamps(after_second) == _row_timestamps(after_first)
            assert _query_test_rows(client, SERVING_TEST_TABLE, job_id) == []

            print(
                "Canonical landing enrichment second run: "
                f"failed_rows={second.failed_rows}, "
                f"landing_rows_updated={second.landing_rows_updated}, "
                "source_updated_at_unchanged=True, serving_rows=0"
            )
        finally:
            cleanup_test_trajectory(client, job_id)
            assert _query_test_rows(client, LANDING_TEST_TABLE, job_id) == []
            assert _query_test_rows(client, SERVING_TEST_TABLE, job_id) == []
            print(
                "Canonical landing enrichment cleanup: "
                "landing_rows=0, serving_rows=0"
            )


def test_trainability_stage_handles_shuffled_step_ids_in_landing_test():
    suffix = f"{uuid.uuid4().hex}_shuffled_steps"
    job_id = f"landing-enrichment#integration#shuffled-steps#{suffix}"
    session_id = f"shuffled-step-session-{suffix}"
    initial_timestamp = int(time.time() * 1000) - 10_000

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.profile == "test"
        assert client.config.tables.landing_table == LANDING_TEST_TABLE
        assert client.config.tables.serving_table == SERVING_TEST_TABLE
        try:
            records = _shuffled_step_records(
                job_id,
                session_id,
                suffix,
                initial_timestamp,
            )
            assert [record.step_id for record in records] == [30, 10, 40, 20]
            client.ingest_landing_batch(records)

            before = _query_test_rows(client, LANDING_TEST_TABLE, job_id)
            assert len(before) == 4
            before_by_step = {int(row["step_id"]): row for row in before}
            assert sum(
                row.get("is_session_completed") is True for row in before
            ) == 1

            shuffled_session = tuple(
                before_by_step[step_id] for step_id in (30, 40, 10, 20)
            )
            assert int(shuffled_session[-1]["step_id"]) == 20
            direct_patches = UpdateIsTrainableStage().transform_session(
                shuffled_session,
                _stage_context(job_id, session_id),
            )
            assert direct_patches == {
                str(before_by_step[30]["id"]): {"is_trainable": False},
                str(before_by_step[40]["id"]): {
                    "is_trainable": True,
                    "reward": 0.875,
                },
                str(before_by_step[10]["id"]): {"is_trainable": False},
                str(before_by_step[20]["id"]): {"is_trainable": False},
            }

            first = ETLEngine(client).run_sessions(
                load_pipeline("landing_enrichment_pipeline"),
                [SessionKey(job_id, session_id)],
            )
            assert first.failed_rows == 0
            assert first.source_rows == 4
            assert first.landing_rows_updated == 1

            after_first = _query_test_rows(client, LANDING_TEST_TABLE, job_id)
            after_first_by_step = {
                int(row["step_id"]): row for row in after_first
            }
            assert after_first_by_step[40]["is_trainable"] is True
            assert after_first_by_step[40]["reward"] == 0.875
            assert (
                after_first_by_step[40]["source_updated_at"]
                > before_by_step[40]["source_updated_at"]
            )
            for step_id in (10, 20, 30):
                assert after_first_by_step[step_id] == before_by_step[step_id]
            assert _query_test_rows(client, SERVING_TEST_TABLE, job_id) == []

            second = ETLEngine(client).run_sessions(
                load_pipeline("landing_enrichment_pipeline"),
                [SessionKey(job_id, session_id)],
            )
            assert second.failed_rows == 0
            assert second.landing_rows_updated == 0
            after_second = _query_test_rows(client, LANDING_TEST_TABLE, job_id)
            assert _row_timestamps(after_second) == _row_timestamps(after_first)

            print(
                "\nShuffled-step trainability result: "
                "ingest_order=30,10,40,20, "
                "stage_input_order=30,40,10,20, "
                "completed_step=40, trainable_steps=40, reward=0.875, "
                f"first_updated={first.landing_rows_updated}, "
                f"second_updated={second.landing_rows_updated}"
            )
        finally:
            cleanup_test_trajectory(client, job_id)
            assert _query_test_rows(client, LANDING_TEST_TABLE, job_id) == []
            assert _query_test_rows(client, SERVING_TEST_TABLE, job_id) == []
            print(
                "Shuffled-step cleanup verified: landing_rows=0, serving_rows=0"
            )
