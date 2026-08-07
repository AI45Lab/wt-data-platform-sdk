"""Unit coverage for landing trainability session selection."""

import json

import pytest

from wt_sdk.etl import SessionKey, StageContext, UpdateIsTrainableStage


def _row(
    record_id: str,
    step_id: int,
    messages: list[dict[str, str]],
    *,
    completed: bool,
    status_code: object | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {"source": "trainability-unit-test"}
    if status_code is not None:
        metadata["env_state"] = json.dumps({"status_code": status_code})
    return {
        "id": record_id,
        "step_id": step_id,
        "messages": json.dumps(messages),
        "is_session_completed": completed,
        "is_trainable": False,
        "meta_json": json.dumps(metadata),
    }


def _context() -> StageContext:
    return StageContext(
        pipeline_name="landing_enrichment_pipeline",
        pipeline_version="1",
        session_key=SessionKey("job-1", "session-1"),
    )


def test_completed_200_session_marks_only_the_append_only_chain_tail():
    first = {"role": "user", "content": "question"}
    response = {"role": "assistant", "content": "answer"}
    session = (
        _row("row-1", 1, [first], completed=False, status_code=200),
        _row("row-2", 2, [first, response], completed=True, status_code=200),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "row-1": {"is_trainable": False},
        "row-2": {"is_trainable": True},
    }


@pytest.mark.parametrize("status_code", [400, 429, 500, 502, 503])
def test_any_non_200_status_skips_the_entire_session(status_code: int):
    first = {"role": "user", "content": "question"}
    response = {"role": "assistant", "content": "answer"}
    session = (
        _row("row-1", 1, [first], completed=False, status_code=status_code),
        _row("row-2", 2, [first, response], completed=True, status_code=200),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {}


def test_string_200_status_is_accepted():
    message = {"role": "user", "content": "question"}
    session = (_row("row-1", 1, [message], completed=True, status_code="200"),)

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "row-1": {"is_trainable": True},
    }


def test_session_without_status_code_preserves_legacy_behavior():
    message = {"role": "user", "content": "question"}
    session = (_row("row-1", 1, [message], completed=True),)

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "row-1": {"is_trainable": True},
    }
