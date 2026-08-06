"""Canonical landing-enrichment integration coverage on test tables."""

import json
import time
import uuid

from dldb.utils import stable_hash

from wt_sdk import LandingRecord, WTGatewayClient
from wt_sdk.core.schemas import LANDING_PARTITIONS, SERVING_PARTITIONS
from wt_sdk.etl import ETLEngine, SessionKey, load_pipeline
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
        order_by="step_id",
        table=table,
        checkout_latest=True,
        exclude_none=False,
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

            second = ETLEngine(client).run_sessions(
                load_pipeline("landing_enrichment_pipeline"),
                [SessionKey(job_id, session_id)],
            )
            assert second.failed_rows == 0
            assert second.landing_rows_updated == 0

            after_second = _query_test_rows(client, LANDING_TEST_TABLE, job_id)
            assert {
                row["id"]: row["source_updated_at"] for row in after_second
            } == {
                row["id"]: row["source_updated_at"] for row in after_first
            }
            assert _query_test_rows(client, SERVING_TEST_TABLE, job_id) == []
        finally:
            cleanup_test_trajectory(client, job_id)
