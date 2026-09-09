"""Unit tests for the landing trainability stage."""

import json
from copy import deepcopy

import pytest

from wt_sdk.etl import (
    SessionKey,
    StageContext,
    StageTransformError,
    UpdateIsTrainableStage,
)


def _row(
    record_id: object,
    step_id: object,
    *,
    completed: object = False,
    reward: object = None,
    is_trainable: bool = False,
    messages: object = "[]",
    meta_json: object = "{}",
) -> dict[str, object]:
    return {
        "id": record_id,
        "step_id": step_id,
        "messages": messages,
        "is_session_completed": completed,
        "is_trainable": is_trainable,
        "meta_json": meta_json,
        "reward": reward,
    }


def _context() -> StageContext:
    return StageContext(
        pipeline_name="landing_enrichment_pipeline",
        pipeline_version="4",
        session_key=SessionKey("job-1", "session-1"),
        stage_name="update_is_trainable",
    )


@pytest.fixture(autouse=True)
def _enable_trainability_downgrade_label(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAINABILITY_DOWNGRADE_LABEL", "true")


def test_stage_declares_its_pipeline_contract():
    stage = UpdateIsTrainableStage()

    assert stage.name == "update_is_trainable"
    assert stage.version == "4"
    assert stage.required_fields == (
        "id",
        "step_id",
        "messages",
        "is_session_completed",
        "meta_json",
        "reward",
    )
    assert stage.output_fields == ("is_trainable", "reward")
    assert stage.dependencies == ()
    assert stage.job_discovery_filter == "is_session_completed = true"


def test_session_without_completion_marker_is_skipped():
    session = (
        _row("row-1", 1, is_trainable=True),
        _row("row-2", 2, completed=None),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {}


@pytest.mark.parametrize("final_reward", [0.0, 0.75])
def test_completed_session_marks_only_max_step_and_copies_reward(final_reward: float):
    session = (
        _row("step-30", 30, is_trainable=True, reward=0.25),
        _row("step-10", 10),
        _row("step-40", 40, completed=True, reward=final_reward),
        _row("step-20", 20),
    )
    original = deepcopy(session)

    patches = UpdateIsTrainableStage().transform_session(session, _context())

    assert patches == {
        "step-30": {"is_trainable": False},
        "step-10": {"is_trainable": False},
        "step-40": {"is_trainable": True, "reward": final_reward},
        "step-20": {"is_trainable": False},
    }
    assert session == original


def test_single_record_completed_session_is_trainable():
    session = (_row("only-row", 7, completed=True, reward=1.0),)

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "only-row": {"is_trainable": True, "reward": 1.0}
    }


def test_null_final_reward_is_not_written_to_trainable_row():
    session = (
        _row("first", 1, reward=0.25),
        _row("last", 2, completed=True, reward=None),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "first": {"is_trainable": False},
        "last": {"is_trainable": True},
    }


def test_completion_before_max_step_warns_and_uses_completed_row_reward():
    session = (
        _row("completed", 4, completed=True, reward=0.625),
        _row("tail", 5, reward=0.125),
    )
    context = _context()

    assert UpdateIsTrainableStage().transform_session(session, context) == {
        "completed": {"is_trainable": False},
        "tail": {"is_trainable": True, "reward": 0.625},
    }
    assert len(context.emitted_warnings) == 1
    warning = context.emitted_warnings[0]
    assert warning.job_id == "job-1"
    assert warning.session_id == "session-1"
    assert warning.stage_name == "update_is_trainable"
    assert warning.warning_type == "CompletionMarkerBeforeMaxStep"
    assert warning.message == (
        "is_session_completed is not set on the maximum step_id record; "
        "completed_record_id='completed', completed_step_id=4, max_step_id=5; "
        "continuing trainability processing"
    )


def test_downgrade_filters_non_200_before_selecting_max_step():
    session = (
        _row(
            "first",
            1,
            messages="malformed JSON",
            meta_json='{"env_state": "{\\"status_code\\": 200}"}',
        ),
        _row(
            "last",
            2,
            completed=True,
            reward=0.5,
            messages=None,
            meta_json='{"env_state": "{\\"status_code\\": 500}"}',
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "first": {"is_trainable": True, "reward": 0.5},
        "last": {"is_trainable": False},
    }


def test_downgrade_marks_every_row_false_when_all_rows_are_non_200():
    session = (
        _row("first", 1, meta_json='{"status_code": 500}'),
        _row(
            "last",
            2,
            completed=True,
            reward=0.5,
            meta_json='{"telemetry": "{\\"status_code\\": 503}"}',
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "first": {"is_trainable": False},
        "last": {"is_trainable": False},
    }


@pytest.mark.parametrize("value", [None, "", "0", "false", "no", "off"])
def test_disabled_trainability_downgrade_label_uses_original_chain_selection(
    monkeypatch: pytest.MonkeyPatch,
    value: str | None,
):
    if value is None:
        monkeypatch.delenv("TRAINABILITY_DOWNGRADE_LABEL", raising=False)
    else:
        monkeypatch.setenv("TRAINABILITY_DOWNGRADE_LABEL", value)

    main_start = {"role": "user", "content": "main task"}
    main_response = {"role": "assistant", "content": "main response"}
    side_start = {"role": "user", "content": "side task"}
    side_response = {"role": "assistant", "content": "side response"}
    session = (
        _row("main-1", 1, messages=json.dumps([main_start])),
        _row("side-1", 2, messages=json.dumps([side_start])),
        _row(
            "main-2",
            3,
            messages=json.dumps([main_start, main_response]),
        ),
        _row(
            "side-2",
            4,
            completed=True,
            reward=0.75,
            messages=json.dumps([side_start, side_response]),
        ),
        _row(
            "error-5",
            5,
            messages="malformed JSON",
            meta_json='{"status_code": 500}',
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "main-1": {"is_trainable": False},
        "side-1": {"is_trainable": False},
        "main-2": {"is_trainable": True, "reward": 0.75},
        "side-2": {"is_trainable": True, "reward": 0.75},
        "error-5": {"is_trainable": False},
    }


def test_empty_session_raises_stage_error():
    with pytest.raises(StageTransformError, match="session must contain at least one row"):
        UpdateIsTrainableStage().transform_session((), _context())


def test_multiple_completion_markers_raise_stage_error_for_second_marker():
    session = (
        _row("first", 1, completed=True),
        _row("second", 2, completed=True),
    )

    with pytest.raises(
        StageTransformError,
        match=r"There is exactly one `is_session_completed`\.",
    ) as error:
        UpdateIsTrainableStage().transform_session(session, _context())

    assert error.value.record_id == "second"


@pytest.mark.parametrize("completed", [0, 1, "true", [], {}])
def test_non_boolean_completion_marker_raises_stage_error(completed: object):
    session = (_row("bad-completion", 1, completed=completed),)

    with pytest.raises(
        StageTransformError,
        match="is_session_completed must be bool or null",
    ) as error:
        UpdateIsTrainableStage().transform_session(session, _context())

    assert error.value.record_id == "bad-completion"


@pytest.mark.parametrize("step_id", [True, False, None, 1.5, "1"])
def test_invalid_step_id_raises_stage_error(step_id: object):
    session = (_row("bad-step", step_id, completed=True),)

    with pytest.raises(
        StageTransformError,
        match=r"record 'bad-step' has invalid step_id:",
    ) as error:
        UpdateIsTrainableStage().transform_session(session, _context())

    assert error.value.record_id == "bad-step"


@pytest.mark.parametrize("record_id", [None, 7, "", "   "])
def test_invalid_record_id_raises_stage_error(record_id: object):
    session = (_row(record_id, 1, completed=True),)

    with pytest.raises(StageTransformError, match="record has invalid id:") as error:
        UpdateIsTrainableStage().transform_session(session, _context())

    assert error.value.record_id is None
