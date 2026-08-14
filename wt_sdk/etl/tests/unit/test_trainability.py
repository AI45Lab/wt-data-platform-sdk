"""Unit coverage for landing trainability session selection."""

import json

import pytest

from wt_sdk.etl import (
    SessionKey,
    StageContext,
    StageTransformError,
    UpdateIsTrainableStage,
)


def _row(
    record_id: str,
    step_id: int,
    messages: list[dict[str, str]],
    *,
    completed: bool,
    status_code: object | None = None,
    reward: float | None = None,
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
        "reward": reward,
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


@pytest.mark.parametrize("subagent_step", [1, 2, 3, 4, 5, 20, 100])
def test_single_record_side_chain_is_not_trainable_at_any_step(
    subagent_step: int,
):
    startup = {"role": "system", "content": "startup side task"}
    root_start = {"role": "user", "content": "main task"}
    root_response = {"role": "assistant", "content": "main response"}
    session = (
        _row(
            "startup",
            subagent_step,
            [startup],
            completed=False,
            status_code=200,
        ),
        _row(
            "root-1",
            subagent_step + 1,
            [root_start],
            completed=False,
            status_code=200,
        ),
        _row(
            "root-2",
            subagent_step + 2,
            [root_start, root_response],
            completed=True,
            status_code=200,
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "startup": {"is_trainable": False},
        "root-1": {"is_trainable": False},
        "root-2": {"is_trainable": True},
    }


def test_single_record_side_chain_after_step_four_is_not_trainable():
    side = {"role": "system", "content": "independent side task"}
    root_start = {"role": "user", "content": "main task"}
    root_response = {"role": "assistant", "content": "main response"}
    session = (
        _row("side", 5, [side], completed=False, status_code=200),
        _row("root-1", 6, [root_start], completed=False, status_code=200),
        _row(
            "root-2",
            7,
            [root_start, root_response],
            completed=True,
            status_code=200,
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "side": {"is_trainable": False},
        "root-1": {"is_trainable": False},
        "root-2": {"is_trainable": True},
    }


def test_single_record_root_session_at_step_one_remains_trainable():
    message = {"role": "user", "content": "main task"}
    session = (_row("root", 1, [message], completed=True, status_code=200),)

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "root": {"is_trainable": True},
    }


def test_completion_marker_before_max_step_raises_stage_error():
    first = {"role": "user", "content": "question"}
    response = {"role": "assistant", "content": "answer"}
    session = (
        _row("row-1", 1, [first], completed=True, status_code=200),
        _row("row-2", 2, [first, response], completed=False, status_code=200),
    )

    with pytest.raises(
        StageTransformError,
        match="is_session_completed must be set on the maximum step_id record",
    ) as exc:
        UpdateIsTrainableStage().transform_session(session, _context())

    assert exc.value.record_id == "row-1"


def test_multiple_completion_markers_raise_stage_error():
    first = {"role": "user", "content": "question"}
    response = {"role": "assistant", "content": "answer"}
    session = (
        _row("row-1", 1, [first], completed=True, status_code=200),
        _row("row-2", 2, [first, response], completed=True, status_code=200),
    )

    with pytest.raises(
        StageTransformError,
        match="There is exactly one `is_session_completed`",
    ):
        UpdateIsTrainableStage().transform_session(session, _context())


def test_completion_marker_on_max_step_accepts_unordered_session():
    first = {"role": "user", "content": "question"}
    response = {"role": "assistant", "content": "answer"}
    session = (
        _row(
            "row-2",
            2,
            [first, response],
            completed=True,
            status_code=200,
            reward=0.75,
        ),
        _row("row-1", 1, [first], completed=False, status_code=200),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "row-2": {"is_trainable": True, "reward": 0.75},
        "row-1": {"is_trainable": False},
    }


def test_early_chain_with_multiple_records_remains_trainable():
    first = {"role": "user", "content": "main task"}
    response = {"role": "assistant", "content": "main response"}
    side = {"role": "user", "content": "independent task"}
    session = (
        _row("root-1", 1, [first], completed=False, status_code=200),
        _row("side", 2, [side], completed=False, status_code=200),
        _row(
            "root-2",
            3,
            [first, response],
            completed=True,
            status_code=200,
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "root-1": {"is_trainable": False},
        "side": {"is_trainable": False},
        "root-2": {"is_trainable": True},
    }


@pytest.mark.parametrize("final_reward", [0.0, 0.75])
def test_trainable_chain_tails_receive_final_session_reward(
    final_reward: float,
):
    root_start = {"role": "user", "content": "main task"}
    root_response = {"role": "assistant", "content": "main response"}
    side_start = {"role": "user", "content": "side task"}
    side_response = {"role": "assistant", "content": "side response"}
    session = (
        _row(
            "root-1",
            1,
            [root_start],
            completed=False,
            status_code=200,
            reward=0.1,
        ),
        _row(
            "side-1",
            2,
            [side_start],
            completed=False,
            status_code=200,
        ),
        _row(
            "root-2",
            3,
            [root_start, root_response],
            completed=False,
            status_code=200,
        ),
        _row(
            "side-2",
            4,
            [side_start, side_response],
            completed=True,
            status_code=200,
            reward=final_reward,
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "root-1": {"is_trainable": False},
        "side-1": {"is_trainable": False},
        "root-2": {"is_trainable": True, "reward": final_reward},
        "side-2": {"is_trainable": True, "reward": final_reward},
    }


def test_later_strict_prefix_retries_longer_chain_and_drops_singleton_subagent():
    system = {"role": "system", "content": "instructions"}
    user = {"role": "user", "content": "task"}
    assistant = {"role": "assistant", "content": "attempt"}
    tool_call = {"role": "tool", "content": "first result"}
    second_assistant = {"role": "assistant", "content": "retry"}
    second_tool_call = {"role": "tool", "content": "second result"}
    retry_response = {"role": "assistant", "content": "success"}
    title_messages = [
        {"role": "system", "content": "generate title"},
        {"role": "user", "content": "title input"},
        {"role": "assistant", "content": "title"},
    ]
    short_prefix = [system, user]
    abandoned_attempt = [
        *short_prefix,
        assistant,
        tool_call,
        second_assistant,
        second_tool_call,
    ]
    successful_retry = [*short_prefix, retry_response]
    session = (
        _row(
            "abandoned",
            1,
            abandoned_attempt,
            completed=False,
            status_code=200,
        ),
        _row(
            "retry-reset",
            2,
            short_prefix,
            completed=False,
            status_code=200,
        ),
        _row(
            "title-generator",
            3,
            title_messages,
            completed=False,
            status_code=200,
        ),
        _row(
            "successful",
            4,
            successful_retry,
            completed=True,
            status_code=200,
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "abandoned": {"is_trainable": False},
        "retry-reset": {"is_trainable": False},
        "title-generator": {"is_trainable": False},
        "successful": {"is_trainable": True},
    }


def test_repeated_identical_single_record_subagents_are_not_merged():
    root_start = {"role": "user", "content": "main task"}
    root_response = {"role": "assistant", "content": "main response"}
    title_messages = [
        {"role": "system", "content": "generate title"},
        {"role": "user", "content": "title input"},
        {"role": "assistant", "content": "title"},
    ]
    session = (
        _row("root-1", 1, [root_start], completed=False, status_code=200),
        _row(
            "title-1",
            2,
            title_messages,
            completed=False,
            status_code=200,
        ),
        _row(
            "title-2",
            20,
            title_messages,
            completed=False,
            status_code=200,
        ),
        _row(
            "root-2",
            21,
            [root_start, root_response],
            completed=True,
            status_code=200,
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "root-1": {"is_trainable": False},
        "title-1": {"is_trainable": False},
        "title-2": {"is_trainable": False},
        "root-2": {"is_trainable": True},
    }


@pytest.mark.parametrize("status_code", [400, 429, 500, 502, 503])
def test_non_200_row_is_excluded_while_valid_rows_are_processed(status_code: int):
    first = {"role": "user", "content": "question"}
    response = {"role": "assistant", "content": "answer"}
    session = (
        _row("row-1", 1, [first], completed=False, status_code=status_code),
        _row("row-2", 2, [first, response], completed=True, status_code=200),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "row-1": {"is_trainable": False},
        "row-2": {"is_trainable": True},
    }


def test_non_200_row_between_valid_chain_records_does_not_break_the_chain():
    first = {"role": "user", "content": "question"}
    failed_attempt = {"role": "assistant", "content": "failed attempt"}
    successful_response = {"role": "assistant", "content": "answer"}
    session = (
        _row("row-1", 1, [first], completed=False, status_code=200),
        _row(
            "row-2",
            2,
            [first, failed_attempt],
            completed=False,
            status_code=502,
        ),
        _row(
            "row-3",
            3,
            [first, successful_response],
            completed=True,
            status_code=200,
        ),
    )

    assert UpdateIsTrainableStage().transform_session(session, _context()) == {
        "row-1": {"is_trainable": False},
        "row-2": {"is_trainable": False},
        "row-3": {"is_trainable": True},
    }


def test_non_200_row_messages_are_not_parsed():
    valid = _row(
        "valid",
        2,
        [{"role": "user", "content": "question"}],
        completed=True,
        status_code=200,
    )
    failed = _row(
        "failed",
        1,
        [{"role": "user", "content": "ignored"}],
        completed=False,
        status_code=502,
    )
    failed["messages"] = "malformed JSON"

    assert UpdateIsTrainableStage().transform_session(
        (failed, valid), _context()
    ) == {
        "failed": {"is_trainable": False},
        "valid": {"is_trainable": True},
    }


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
