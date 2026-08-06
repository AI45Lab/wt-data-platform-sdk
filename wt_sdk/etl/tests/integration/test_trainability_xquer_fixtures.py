"""Read-only characterization of contributor-owned trainability fixtures."""

import copy
import os

import pytest

from wt_sdk import WTGatewayClient
from wt_sdk.etl import SessionKey, StageContext, UpdateIsTrainableStage
from wt_sdk.etl.tests.integration.helpers import (
    LANDING_TEST_TABLE,
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
        ],
        partition=FIXTURE_JOB_ID,
        order_by="step_id",
        table=LANDING_TEST_TABLE,
        checkout_latest=True,
        exclude_none=False,
        deserialize_json=False,
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


def test_xquer_fixture_results_are_read_only_and_repeatable():
    stage = UpdateIsTrainableStage()
    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.profile == "test"
        assert client.config.tables.landing_table == LANDING_TEST_TABLE
        queried_rows = _query_fixture_rows(client)
        database_snapshot = copy.deepcopy(queried_rows)

        rows_by_session = {
            session_id: sorted(
                (
                    row
                    for row in queried_rows
                    if row.get("session_id") == session_id
                ),
                key=lambda row: row["step_id"],
            )
            for session_id in FIXTURE_SESSION_IDS
        }
        results = []
        errors = []
        for session_id, rows in rows_by_session.items():
            if not rows:
                errors.append(f"{session_id}: no landing rows found")
                continue

            session = tuple(rows)
            session_snapshot = copy.deepcopy(session)
            first = stage.transform_session(session, _context(session_id))
            second = stage.transform_session(session, _context(session_id))
            assert first == second
            assert session == session_snapshot
            assert set(first) == {str(row["id"]) for row in session}

            completed_steps = tuple(
                int(row["step_id"])
                for row in session
                if row.get("is_session_completed") is True
            )
            current_true_steps = tuple(
                int(row["step_id"])
                for row in session
                if row.get("is_trainable") is True
            )
            stage_true_steps = tuple(
                int(row["step_id"])
                for row in session
                if first[str(row["id"])]["is_trainable"] is True
            )
            changed_steps = tuple(
                int(row["step_id"])
                for row in session
                if row.get("is_trainable")
                != first[str(row["id"])]["is_trainable"]
            )
            final_step = int(session[-1]["step_id"])
            if completed_steps != (final_step,):
                errors.append(
                    f"{session_id}: completion marker {completed_steps!r} is not "
                    f"only on final step {final_step}"
                )
            if final_step not in stage_true_steps:
                errors.append(
                    f"{session_id}: final step {final_step} was not marked trainable"
                )
            results.append(
                (
                    session_id,
                    len(session),
                    _format_steps(completed_steps),
                    _format_steps(current_true_steps),
                    _format_steps(stage_true_steps),
                    _format_steps(changed_steps),
                )
            )

        after = _query_fixture_rows(client)

    print("\nRead-only xquer trainability results\n" + _render_results(results))
    assert {row["id"]: row for row in after} == {
        row["id"]: row for row in database_snapshot
    }
    assert not errors, "\n".join(errors)
