"""Read-only trainability coverage for copied cybergym sessions."""

from __future__ import annotations

import copy
import json
import os
import sys
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path

import pytest

from wt_sdk import WTGatewayClient
from wt_sdk.etl import SessionKey, StageContext, UpdateIsTrainableStage
from wt_sdk.etl.tests.integration.helpers import (
    LANDING_TEST_TABLE,
    TEST_TABLE_CONFIG,
)


FIXTURE_JOB_ID = "cybegymopencode_k3l3"
SESSION_SAMPLE_SIZE = 300
PROJECT_ROOT = Path(__file__).resolve().parents[4]
MULTI_TRAINABLE_OUTPUT = (
    PROJECT_ROOT / "cybergym_multi_trainable_sessions.json"
)

pytestmark = pytest.mark.skipif(
    os.getenv("WT_SDK_RUN_CYBERGYM_FIXTURE") != "1",
    reason=(
        "set WT_SDK_RUN_CYBERGYM_FIXTURE=1 to query the fixed cybergym fixture"
    ),
)


def _sql_quote(value: str) -> str:
    return value.replace("'", "''")


def _query_completed_session_ids(client: WTGatewayClient) -> list[str]:
    rows = client.query_data(
        filter_query=(
            f"job_id = '{_sql_quote(FIXTURE_JOB_ID)}' "
            "AND is_session_completed = true"
        ),
        columns=["session_id"],
        partition=FIXTURE_JOB_ID,
        table=LANDING_TEST_TABLE,
        checkout_latest=True,
        exclude_none=False,
    )
    return sorted({str(row["session_id"]) for row in rows})


def _query_fixture_rows(
    client: WTGatewayClient,
) -> list[dict[str, object]]:
    return client.query_data(
        filter_query=f"job_id = '{_sql_quote(FIXTURE_JOB_ID)}'",
        columns=[
            "id",
            "job_id",
            "session_id",
            "step_id",
            "messages",
            "is_session_completed",
            "is_trainable",
            "meta_json",
            "reward",
        ],
        partition=FIXTURE_JOB_ID,
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


def _decode_object(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _status_code(row: Mapping[str, object]) -> object | None:
    metadata = _decode_object(row.get("meta_json"))
    if metadata is None:
        return None

    candidates = [metadata]
    for key in ("env_state", "telemetry"):
        nested = _decode_object(metadata.get(key))
        if nested is not None:
            candidates.append(nested)
    for candidate in candidates:
        if "status_code" in candidate:
            return candidate["status_code"]
    return None


def _is_non_200_status(value: object | None) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return value != 200
    if isinstance(value, str):
        return value.strip() != "200"
    return True


def _render_results(results: list[tuple[object, ...]]) -> str:
    headers = (
        "session_id",
        "rows",
        "last_step",
        "stored_true",
        "stage_true",
        "final_reward",
        "non_200_steps",
        "repeatable",
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


def _show_progress(completed: int, total: int) -> None:
    width = 40
    filled = width * completed // total
    bar = "#" * filled + "-" * (width - filled)
    end = "\n" if completed == total else ""
    print(
        f"\rRunning trainability stage [{bar}] {completed}/{total}",
        end=end,
        file=sys.stdout,
        flush=True,
    )


def _write_multi_trainable_sessions(
    sessions: list[dict[str, object]],
) -> None:
    payload = {
        "job_id": FIXTURE_JOB_ID,
        "sessions_processed": SESSION_SAMPLE_SIZE,
        "multi_trainable_session_count": len(sessions),
        "session_ids": [session["session_id"] for session in sessions],
        "sessions": sessions,
    }
    MULTI_TRAINABLE_OUTPUT.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


def test_cybergym_fixture_reports_stage_results_for_200_sessions():
    """Run the stage read-only and report its result for 200 sessions."""

    stage = UpdateIsTrainableStage()
    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.profile == "test"
        assert client.config.tables.landing_table == LANDING_TEST_TABLE
        completed_session_ids = _query_completed_session_ids(client)
        selected_session_ids = completed_session_ids[:SESSION_SAMPLE_SIZE]
        assert len(selected_session_ids) == SESSION_SAMPLE_SIZE, (
            f"expected at least {SESSION_SAMPLE_SIZE} completed sessions, "
            f"found {len(completed_session_ids)}"
        )
        rows = _query_fixture_rows(client)

    assert rows, f"no landing_test rows found for job_id={FIXTURE_JOB_ID!r}"
    rows_by_session: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        assert row.get("job_id") == FIXTURE_JOB_ID
        rows_by_session[str(row["session_id"])].append(row)

    assert set(selected_session_ids).issubset(rows_by_session)

    results: list[tuple[object, ...]] = []
    total_rows = 0
    stored_trainable_count = 0
    stage_trainable_count = 0
    final_step_trainable_count = 0
    sessions_with_multiple_trainable = 0
    sessions_without_trainable = 0
    multi_trainable_sessions: list[dict[str, object]] = []
    _show_progress(0, SESSION_SAMPLE_SIZE)
    for completed, session_id in enumerate(selected_session_ids, start=1):
        unordered_rows = rows_by_session[session_id]
        session = tuple(
            sorted(unordered_rows, key=lambda row: int(row["step_id"]))
        )
        assert session[-1].get("is_session_completed") is True, (
            f"session {session_id!r} is not completed on its final row"
        )

        snapshot = copy.deepcopy(session)
        first = stage.transform_session(session, _context(session_id))
        second = stage.transform_session(session, _context(session_id))
        assert first == second, (
            f"session {session_id!r} changed on the second stage execution"
        )
        assert session == snapshot, f"stage mutated session {session_id!r}"
        assert set(first) == {str(row["id"]) for row in session}

        last_step = int(session[-1]["step_id"])
        trainable_ids = {
            record_id
            for record_id, patch in first.items()
            if patch["is_trainable"] is True
        }
        trainable_steps = [
            int(row["step_id"])
            for row in session
            if str(row["id"]) in trainable_ids
        ]
        stored_trainable_steps = [
            int(row["step_id"])
            for row in session
            if row.get("is_trainable") is True
        ]
        final_reward = session[-1].get("reward")
        assert all(
            first[record_id].get("reward") == final_reward
            for record_id in trainable_ids
        )
        total_rows += len(session)
        stored_trainable_count += len(stored_trainable_steps)
        stage_trainable_count += len(trainable_ids)
        final_step_trainable_count += int(
            str(session[-1]["id"]) in trainable_ids
        )
        sessions_with_multiple_trainable += int(len(trainable_ids) > 1)
        sessions_without_trainable += int(not trainable_ids)
        if len(trainable_ids) > 1:
            multi_trainable_sessions.append(
                {
                    "session_id": session_id,
                    "row_count": len(session),
                    "last_step": last_step,
                    "stage_trainable_steps": trainable_steps,
                    "rows": [
                        {
                            **row,
                            "stage_is_trainable": first[str(row["id"])][
                                "is_trainable"
                            ],
                        }
                        for row in session
                    ],
                }
            )

        non_200_steps = tuple(
            int(row["step_id"])
            for row in session
            if _is_non_200_status(_status_code(row))
        )
        assert all(
            first[str(row["id"])]["is_trainable"] is False
            for row in session
            if int(row["step_id"]) in non_200_steps
        )

        results.append(
            (
                session_id,
                len(session),
                last_step,
                ", ".join(str(step) for step in stored_trainable_steps) or "-",
                ", ".join(str(step) for step in trainable_steps) or "-",
                final_reward,
                ", ".join(str(step) for step in non_200_steps) or "-",
                True,
            )
        )
        _show_progress(completed, SESSION_SAMPLE_SIZE)

    _write_multi_trainable_sessions(multi_trainable_sessions)
    print("\nCybergym trainability results\n" + _render_results(results))
    print(
        "\nCybergym trainability summary\n"
        f"sessions_processed={SESSION_SAMPLE_SIZE}\n"
        f"rows_processed={total_rows}\n"
        f"stored_is_trainable_true={stored_trainable_count}\n"
        f"stage_is_trainable_true={stage_trainable_count}\n"
        f"stage_true_on_session_last_step={final_step_trainable_count}\n"
        f"sessions_with_multiple_stage_true="
        f"{sessions_with_multiple_trainable}\n"
        f"sessions_without_stage_true={sessions_without_trainable}\n"
        f"multi_trainable_session_ids="
        f"{[item['session_id'] for item in multi_trainable_sessions]}\n"
        f"multi_trainable_sessions_json={MULTI_TRAINABLE_OUTPUT}"
    )
