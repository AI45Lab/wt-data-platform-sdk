"""Canonical enrichment coverage using reusable contributor-owned fixtures."""

import os
import subprocess
import sys
from pathlib import Path

from dldb.utils import stable_hash
import pytest

from wt_sdk import WTGatewayClient
from wt_sdk.core.schemas import SERVING_PARTITIONS
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
)


FIXTURE_JOB_ID = "gateway_mock_for_xquer"
FIXTURE_SESSION_IDS = (
    "65826628-cf3b-4918-846b-a0b5b642370d_mock_for_xquer",
    "47b3b4f2-5d6b-4ed3-8706-e54f43c0f7fa_mock_for_xquer",
    "1cd612ca-1dbc-46aa-9623-40b14525ace4_mock_for_xquer",
    "79c750ad-b827-4e10-9c31-e76e74c43f43_mock_for_xquer",
    "7eea120b-f9fe-4560-bd69-253a3e5d32fb_mock_for_xquer",
    "80e15221-e117-4fc9-9e35-c0f81c3a8160_mock_for_xquer",
    "mock_xq_case_01",
    "mock_xq_case_02",
    "mock_xq_case_03",
    "mock_xq_case_04",
    "mock_xq_case_05",
)

pytestmark = pytest.mark.skipif(
    os.getenv("WT_SDK_RUN_XQUER_FIXTURES") != "1",
    reason="set WT_SDK_RUN_XQUER_FIXTURES=1 to query fixed xquer fixtures",
)


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _query_fixture_rows(client: WTGatewayClient) -> list[dict[str, object]]:
    session_filter = " OR ".join(
        f"session_id = '{_sql_quote(session_id)}'"
        for session_id in FIXTURE_SESSION_IDS
    )
    return client.query_data(
        filter_query=(
            f"job_id = '{_sql_quote(FIXTURE_JOB_ID)}' AND ({session_filter})"
        ),
        columns=[
            "id",
            "job_id",
            "session_id",
            "step_id",
            "source_updated_at",
            "messages",
            "is_session_completed",
            "is_trainable",
            "meta_json",
            "reward",
        ],
        partition=FIXTURE_JOB_ID,
        order_by="step_id",
        table=LANDING_TEST_TABLE,
        checkout_latest=True,
        exclude_none=False,
        deserialize_json=False,
    )


def _query_serving_rows(client: WTGatewayClient) -> list[dict[str, object]]:
    bucket = stable_hash(FIXTURE_JOB_ID) % SERVING_PARTITIONS
    if bucket not in set(client.list_table_partitions(table=SERVING_TEST_TABLE)):
        return []
    return client.query_data(
        filter_query=f"job_id = '{_sql_quote(FIXTURE_JOB_ID)}'",
        partition=FIXTURE_JOB_ID,
        table=SERVING_TEST_TABLE,
        checkout_latest=True,
        exclude_none=False,
    )


def _rollback_fixture_trainability() -> subprocess.CompletedProcess[str]:
    script = Path(__file__).parents[4] / "scripts" / "ops" / "update_table_rows.py"
    return subprocess.run(
        [
            sys.executable,
            str(script),
            "--profile",
            "test",
            "--table",
            "landing",
            "--query",
            f"job_id = '{FIXTURE_JOB_ID}'",
            "--updates",
            '{"is_trainable": false}',
            "--yes",
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _context(session_id: str) -> StageContext:
    return StageContext(
        pipeline_name="landing_enrichment_pipeline",
        pipeline_version="1",
        session_key=SessionKey(FIXTURE_JOB_ID, session_id),
    )


def _format_steps(step_ids: tuple[int, ...]) -> str:
    return ", ".join(str(step_id) for step_id in step_ids) or "-"


def _render_results(results: list[tuple[object, ...]]) -> str:
    headers = (
        "session_id",
        "rows",
        "completed",
        "current_true",
        "stage_true",
        "would_change",
    )
    table = [headers, *results]
    widths = [
        max(len(str(row[column])) for row in table)
        for column in range(len(headers))
    ]
    separator = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    rendered = [separator]
    for index, row in enumerate(table):
        rendered.append(
            "| "
            + " | ".join(
                str(value).ljust(widths[column])
                for column, value in enumerate(row)
            )
            + " |"
        )
        if index == 0:
            rendered.append(separator)
    rendered.append(separator)
    return "\n".join(rendered)


def test_xquer_fixtures_through_canonical_pipeline_are_repeatable():
    stage = UpdateIsTrainableStage()
    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.profile == "test"
        assert client.config.tables.landing_table == LANDING_TEST_TABLE
        assert client.config.tables.serving_table == SERVING_TEST_TABLE
        try:
            before = _query_fixture_rows(client)
            serving_before = _query_serving_rows(client)
            before_by_id = {str(row["id"]): row for row in before}
            rows_by_session = {
                session_id: sorted(
                    (row for row in before if row.get("session_id") == session_id),
                    key=lambda row: row["step_id"],
                )
                for session_id in FIXTURE_SESSION_IDS
            }
            assert all(rows_by_session.values()), (
                "one or more fixture sessions are empty"
            )
            assert all(row.get("is_trainable") is False for row in before), (
                "fixture precondition failed; run update_table_rows.py to reset "
                "is_trainable=false"
            )

            expected_trainable_ids: set[str] = set()
            results = []
            for session_id, rows in rows_by_session.items():
                patches = stage.transform_session(tuple(rows), _context(session_id))
                trainable_ids = {
                    record_id
                    for record_id, patch in patches.items()
                    if patch["is_trainable"] is True
                }
                expected_trainable_ids.update(trainable_ids)
                results.append(
                    (
                        session_id,
                        len(rows),
                        _format_steps(
                            tuple(
                                int(row["step_id"])
                                for row in rows
                                if row.get("is_session_completed") is True
                            )
                        ),
                        "-",
                        _format_steps(
                            tuple(
                                int(row["step_id"])
                                for row in rows
                                if str(row["id"]) in trainable_ids
                            )
                        ),
                        _format_steps(
                            tuple(
                                int(row["step_id"])
                                for row in rows
                                if str(row["id"]) in trainable_ids
                            )
                        ),
                    )
                )

            assert expected_trainable_ids
            assert set(before_by_id).difference(expected_trainable_ids)

            session_keys = [
                SessionKey(FIXTURE_JOB_ID, session_id)
                for session_id in FIXTURE_SESSION_IDS
            ]
            first = ETLEngine(client).run_sessions(
                load_pipeline("landing_enrichment_pipeline"), session_keys
            )
            assert first.failed_rows == 0
            assert first.landing_rows_updated == len(expected_trainable_ids)

            after_first = _query_fixture_rows(client)
            after_first_by_id = {str(row["id"]): row for row in after_first}
            assert set(after_first_by_id) == set(before_by_id)
            for record_id, original in before_by_id.items():
                current = after_first_by_id[record_id]
                if record_id in expected_trainable_ids:
                    assert current["is_trainable"] is True
                    assert current["source_updated_at"] > original["source_updated_at"]
                else:
                    assert current == original
            assert _query_serving_rows(client) == serving_before

            second = ETLEngine(client).run_sessions(
                load_pipeline("landing_enrichment_pipeline"), session_keys
            )
            assert second.failed_rows == 0
            assert second.landing_rows_updated == 0
            after_second = _query_fixture_rows(client)
            assert {
                row["id"]: row["source_updated_at"] for row in after_second
            } == {
                row["id"]: row["source_updated_at"] for row in after_first
            }
            assert _query_serving_rows(client) == serving_before

            print("\nXquer trainability results\n" + _render_results(results))
            print(
                "First run: "
                f"failed_rows={first.failed_rows}, "
                f"landing_rows_updated={first.landing_rows_updated}, "
                "non_matching_unchanged=True, serving_unchanged=True"
            )
            print(
                "Second run: "
                f"failed_rows={second.failed_rows}, "
                f"landing_rows_updated={second.landing_rows_updated}, "
                "source_updated_at_unchanged=True, serving_unchanged=True"
            )
        finally:
            rollback = _rollback_fixture_trainability()
            print("Fixture rollback via update_table_rows.py:\n" + rollback.stdout)
            if rollback.stderr:
                print("Fixture rollback stderr:\n" + rollback.stderr)
            assert rollback.returncode == 0
            restored = _query_fixture_rows(client)
            assert restored
            assert all(row.get("is_trainable") is False for row in restored)
            print(f"Fixture rollback verified: {len(restored)} rows is_trainable=False")
