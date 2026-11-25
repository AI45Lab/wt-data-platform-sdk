import json

import pytest

from wt_sdk.trajectory_stats.schemas import (
    CLASSIFICATION_STATUS_INVALID_JOB_ID,
    CLASSIFICATION_STATUS_VALID,
    METRIC_STATUS_INCOMPLETE,
    METRIC_STATUS_MISSING,
    METRIC_STATUS_VALID,
)
from wt_sdk.trajectory_stats.session_metrics import compute_session_metrics


JOB_ID = "cybergym#claudecode#glm-5.3#挖掘#level1-part2"
SESSION_ID = "session-1"
UPDATED_AT_MS = 1_700_000_000_000


def _payload(value):
    return value if isinstance(value, str) else json.dumps(value)


def landing_row(
    step_id,
    *,
    meta=None,
    response=None,
    messages=None,
    job_id=JOB_ID,
    session_id=SESSION_ID,
    sub_job_id=None,
):
    if messages is None:
        messages = [{"role": "user", "content": "request"}]
    return {
        "id": f"row-{step_id}",
        "job_id": job_id,
        "session_id": session_id,
        "sub_job_id": sub_job_id,
        "step_id": step_id,
        "meta_json": None if meta is None else _payload(meta),
        "response": None if response is None else _payload(response),
        "messages": _payload(messages),
    }


def compute(rows, *, job_id=JOB_ID, session_id=SESSION_ID):
    return compute_session_metrics(
        job_id,
        session_id,
        rows,
        updated_at_ms=UPDATED_AT_MS,
    )


def assert_metric(row, metric, value, status, reason=None):
    assert row[metric] == value
    assert row[f"{metric}_status"] == status
    assert row[f"{metric}_reason"] == reason


def test_claudecode_full_session_uses_all_requests_and_new_response_tools():
    rows = [
        landing_row(
            1,
            meta={"prompt_tokens": 10, "completion_tokens": 5},
            response={"message": {"role": "assistant", "tool_calls": [{}, {}]}},
        ),
        landing_row(
            2,
            meta={"prompt_tokens": 20, "completion_tokens": 7},
            response={
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "name": "bash"},
                    {"type": "tool_result", "content": "done"},
                ],
            },
        ),
        landing_row(
            31,
            response={
                "usage": {"input_tokens": 3, "output_tokens": 2},
                "message": {"role": "assistant", "content": []},
            },
        ),
    ]

    result = compute(rows)

    assert result["classification_status"] == CLASSIFICATION_STATUS_VALID
    assert result["dataset"] == "cybergym"
    assert result["harness"] == "claudecode"
    assert result["level"] == "level1-part2"
    assert_metric(result, "total_tokens", 47, METRIC_STATUS_VALID)
    assert_metric(result, "total_steps", 31, METRIC_STATUS_VALID)
    assert_metric(result, "tool_calls", 3, METRIC_STATUS_VALID)
    assert result["updated_at_ms"] == UPDATED_AT_MS


def test_codex_counts_call_events_but_not_outputs_or_reasoning():
    result = compute(
        [
            landing_row(
                1,
                meta={"prompt_tokens": 50, "completion_tokens": 8},
                response=[
                    {"type": "reasoning"},
                    {"type": "function_call", "name": "bash"},
                    {"type": "function_call_output", "output": "done"},
                    {"type": "web_search_call"},
                    {"type": "message", "content": [{"type": "output_text"}]},
                ],
            )
        ]
    )

    assert_metric(result, "total_tokens", 58, METRIC_STATUS_VALID)
    assert_metric(result, "tool_calls", 2, METRIC_STATUS_VALID)


def test_opencode_openai_usage_fallback_and_tool_calls():
    result = compute(
        [
            landing_row(
                1,
                response={
                    "usage": {
                        "prompt_tokens": 6914,
                        "completion_tokens": 98,
                        "reasoning_tokens": 17,
                        "total_tokens": 7012,
                    },
                    "message": {"role": "assistant", "tool_calls": [{}]},
                },
            )
        ]
    )

    assert_metric(result, "total_tokens", 7012, METRIC_STATUS_VALID)
    assert_metric(result, "tool_calls", 1, METRIC_STATUS_VALID)


def test_meta_response_usage_is_fallback_not_an_additional_request():
    result = compute(
        [
            landing_row(
                1,
                meta={"response": {"usage": {"input_tokens": 7, "output_tokens": 8}}},
                response={"role": "assistant", "content": []},
            )
        ]
    )

    assert_metric(result, "total_tokens", 15, METRIC_STATUS_VALID)


def test_cache_read_tokens_are_not_added_to_requested_total():
    result = compute(
        [
            landing_row(
                1,
                response={
                    "usage": {
                        "input_tokens": 100,
                        "cache_read_input_tokens": 24_000,
                        "cache_creation_input_tokens": 500,
                        "output_tokens": 5,
                    },
                    "content": [],
                },
            )
        ]
    )

    assert_metric(result, "total_tokens", 105, METRIC_STATUS_VALID)


def test_token_missing_in_one_request_marks_only_token_incomplete():
    rows = [
        landing_row(
            1,
            meta={"prompt_tokens": 10, "completion_tokens": 2},
            response={"role": "assistant", "content": []},
        ),
        landing_row(2, response={"role": "assistant", "content": []}),
    ]

    result = compute(rows)

    assert_metric(
        result,
        "total_tokens",
        None,
        METRIC_STATUS_INCOMPLETE,
        "TOKEN_USAGE_INCOMPLETE",
    )
    assert_metric(result, "total_steps", 2, METRIC_STATUS_VALID)
    assert_metric(result, "tool_calls", 0, METRIC_STATUS_VALID)


def test_all_token_usage_missing_is_unknown_not_zero():
    result = compute(
        [landing_row(1, response={"role": "assistant", "content": []})]
    )

    assert_metric(
        result,
        "total_tokens",
        None,
        METRIC_STATUS_MISSING,
        "TOKEN_USAGE_MISSING",
    )
    assert_metric(result, "tool_calls", 0, METRIC_STATUS_VALID)


def test_empty_message_placeholders_are_excluded_from_all_metrics():
    rows = [
        landing_row(
            0,
            messages=[],
            meta={"prompt_tokens": 999, "completion_tokens": 999},
            response={"message": {"tool_calls": [{}, {}]}},
        ),
        landing_row(
            31,
            meta={"prompt_tokens": 4, "completion_tokens": 3},
            response={"role": "assistant", "content": []},
        ),
    ]

    result = compute(rows)

    assert_metric(result, "total_tokens", 7, METRIC_STATUS_VALID)
    assert_metric(result, "total_steps", 31, METRIC_STATUS_VALID)
    assert_metric(result, "tool_calls", 0, METRIC_STATUS_VALID)


def test_only_placeholders_produce_three_independent_missing_metrics():
    result = compute([landing_row(0, messages=[], response={"content": []})])

    for metric in ("total_tokens", "total_steps", "tool_calls"):
        assert_metric(
            result,
            metric,
            None,
            METRIC_STATUS_MISSING,
            "NO_REAL_LANDING_REQUESTS",
        )


def test_malformed_response_does_not_invalidate_meta_token_or_step():
    result = compute(
        [landing_row(4, meta={"prompt_tokens": 8, "completion_tokens": 2}, response="{bad")]
    )

    assert_metric(result, "total_tokens", 10, METRIC_STATUS_VALID)
    assert_metric(result, "total_steps", 4, METRIC_STATUS_VALID)
    assert_metric(
        result,
        "tool_calls",
        None,
        METRIC_STATUS_MISSING,
        "TOOL_CALLS_MISSING",
    )


def test_normalized_tool_calls_and_tool_use_are_not_double_counted():
    result = compute(
        [
            landing_row(
                1,
                meta={"prompt_tokens": 1, "completion_tokens": 1},
                response={
                    "role": "assistant",
                    "tool_calls": [{}, {}],
                    "content": [{"type": "tool_use"}, {"type": "tool_use"}],
                },
            )
        ]
    )

    assert_metric(result, "tool_calls", 2, METRIC_STATUS_VALID)


def test_message_history_tool_calls_are_not_counted_again():
    result = compute(
        [
            landing_row(
                2,
                meta={"prompt_tokens": 1, "completion_tokens": 1},
                messages=[
                    {"role": "assistant", "tool_calls": [{}, {}, {}]},
                    {"role": "user", "content": "next"},
                ],
                response={"role": "assistant", "content": []},
            )
        ]
    )

    assert_metric(result, "tool_calls", 0, METRIC_STATUS_VALID)


def test_multiple_sub_jobs_are_combined_under_parent_job_session():
    rows = [
        landing_row(
            1,
            sub_job_id="child-a",
            meta={"prompt_tokens": 3, "completion_tokens": 2},
            response={"message": {"tool_calls": [{}]}},
        ),
        landing_row(
            5,
            sub_job_id="child-b",
            meta={"prompt_tokens": 7, "completion_tokens": 4},
            response={"message": {"tool_calls": [{}, {}]}},
        ),
    ]

    result = compute(rows)

    assert result["job_id"] == JOB_ID
    assert result["session_id"] == SESSION_ID
    assert "sub_job_id" not in result
    assert_metric(result, "total_tokens", 16, METRIC_STATUS_VALID)
    assert_metric(result, "total_steps", 5, METRIC_STATUS_VALID)
    assert_metric(result, "tool_calls", 3, METRIC_STATUS_VALID)


def test_zero_step_on_a_real_request_is_not_mechanically_incremented():
    result = compute(
        [
            landing_row(
                0,
                meta={"prompt_tokens": 1, "completion_tokens": 0},
                response={"content": []},
            )
        ]
    )

    assert_metric(result, "total_steps", 0, METRIC_STATUS_VALID)


def test_no_valid_step_id_is_missing_not_zero():
    result = compute(
        [
            landing_row(
                None,
                meta={"prompt_tokens": 1, "completion_tokens": 0},
                response={"content": []},
            )
        ]
    )

    assert_metric(
        result,
        "total_steps",
        None,
        METRIC_STATUS_MISSING,
        "STEP_ID_MISSING",
    )


def test_invalid_job_id_is_kept_for_audit_but_not_normal_classification():
    result = compute(
        [
            landing_row(
                1,
                job_id="invalid-job",
                meta={"prompt_tokens": 1, "completion_tokens": 2},
                response={"content": []},
            )
        ],
        job_id="invalid-job",
    )

    assert result["classification_status"] == CLASSIFICATION_STATUS_INVALID_JOB_ID
    assert result["dataset"] is None
    assert result["level"] == ""
    assert_metric(result, "total_tokens", 3, METRIC_STATUS_VALID)


def test_mismatched_landing_identity_is_rejected():
    with pytest.raises(ValueError, match="different job_id"):
        compute([landing_row(1, job_id="other#job#id#parts")])

    with pytest.raises(ValueError, match="different session_id"):
        compute([landing_row(1, session_id="other-session")])
