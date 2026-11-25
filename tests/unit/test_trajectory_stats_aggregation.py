import pytest

from wt_sdk.trajectory_stats.aggregation import aggregate_category_metrics
from wt_sdk.trajectory_stats.schemas import (
    CLASSIFICATION_STATUS_INVALID_JOB_ID,
    CLASSIFICATION_STATUS_VALID,
    METRIC_STATUS_INCOMPLETE,
    METRIC_STATUS_MISSING,
    METRIC_STATUS_VALID,
)


UPDATED_AT_MS = 1_700_000_000_000


def session_row(
    job_id,
    session_id,
    *,
    tokens=None,
    token_status=METRIC_STATUS_VALID,
    steps=None,
    step_status=METRIC_STATUS_VALID,
    tools=None,
    tool_status=METRIC_STATUS_VALID,
    dataset="cybergym",
    harness="claudecode",
    model="glm-5.3",
    task_type="挖掘",
    level="level1",
    classification_status=CLASSIFICATION_STATUS_VALID,
):
    return {
        "job_id": job_id,
        "session_id": session_id,
        "classification_status": classification_status,
        "dataset": dataset,
        "harness": harness,
        "model": model,
        "task_type": task_type,
        "level": level,
        "total_tokens": tokens,
        "total_tokens_status": token_status,
        "total_steps": steps,
        "total_steps_status": step_status,
        "tool_calls": tools,
        "tool_calls_status": tool_status,
    }


def test_even_count_median_mean_min_max_and_metric_independent_coverage():
    rows = [
        session_row("job-a", "s1", tokens=1, steps=1, tools=0),
        session_row("job-a", "s2", tokens=2, steps=3, tools=2),
        session_row("job-a", "s3", tokens=3, steps=5, tools=4),
        session_row(
            "job-a",
            "s4",
            tokens=None,
            token_status=METRIC_STATUS_INCOMPLETE,
            steps=7,
            tools=6,
        ),
    ]

    result = aggregate_category_metrics(rows, updated_at_ms=UPDATED_AT_MS)

    assert len(result) == 1
    category = result[0]
    assert category["delivered_session_count"] == 4
    assert category["total_tokens_mean"] == 2.0
    assert category["total_tokens_median"] == 2.0
    assert category["total_tokens_max"] == 3
    assert category["total_tokens_min"] == 1
    assert category["total_tokens_valid_session_count"] == 3
    assert category["total_tokens_missing_session_count"] == 1
    assert category["total_tokens_coverage"] == 0.75
    assert category["total_steps_mean"] == 4.0
    assert category["total_steps_median"] == 4.0
    assert category["total_steps_max"] == 7
    assert category["total_steps_min"] == 1
    assert category["total_steps_coverage"] == 1.0
    assert category["tool_calls_mean"] == 3.0
    assert category["tool_calls_median"] == 3.0
    assert category["tool_calls_max"] == 6
    assert category["tool_calls_min"] == 0
    assert category["updated_at_ms"] == UPDATED_AT_MS


def test_metric_with_no_valid_values_has_null_statistics_and_zero_coverage():
    result = aggregate_category_metrics(
        [
            session_row(
                "job-a",
                "s1",
                tokens=None,
                token_status=METRIC_STATUS_MISSING,
                steps=2,
                tools=0,
            )
        ],
        updated_at_ms=UPDATED_AT_MS,
    )[0]

    for statistic in ("mean", "median", "max", "min"):
        assert result[f"total_tokens_{statistic}"] is None
    assert result["total_tokens_valid_session_count"] == 0
    assert result["total_tokens_missing_session_count"] == 1
    assert result["total_tokens_coverage"] == 0.0
    assert result["total_steps_max"] == 2
    assert result["tool_calls_min"] == 0


def test_duplicate_session_is_counted_once_and_latest_row_wins():
    rows = [
        session_row("job-a", "shared-session", tokens=100),
        session_row("job-a", "shared-session", tokens=10),
        session_row("job-a", "other-session", tokens=20),
    ]

    result = aggregate_category_metrics(rows, updated_at_ms=UPDATED_AT_MS)[0]

    assert result["delivered_session_count"] == 2
    assert result["total_tokens_mean"] == 15.0
    assert result["total_tokens_max"] == 20
    assert result["total_tokens_min"] == 10


def test_same_session_id_under_different_jobs_remains_distinct():
    result = aggregate_category_metrics(
        [
            session_row("job-a", "same-session", tokens=10),
            session_row("job-b", "same-session", tokens=30),
        ],
        updated_at_ms=UPDATED_AT_MS,
    )[0]

    assert result["delivered_session_count"] == 2
    assert result["total_tokens_mean"] == 20.0


def test_invalid_classification_is_excluded_and_categories_are_sorted():
    rows = [
        session_row("job-z", "s1", dataset="zeta", level=""),
        session_row("job-a", "s2", dataset="alpha", level=""),
        session_row(
            "invalid",
            "s3",
            classification_status=CLASSIFICATION_STATUS_INVALID_JOB_ID,
            dataset=None,
            harness=None,
            model=None,
            task_type=None,
            level="",
        ),
    ]

    result = aggregate_category_metrics(rows, updated_at_ms=UPDATED_AT_MS)

    assert [row["dataset"] for row in result] == ["alpha", "zeta"]
    assert sum(row["delivered_session_count"] for row in result) == 2


@pytest.mark.parametrize("missing_key", ["job_id", "session_id"])
def test_missing_session_identity_is_rejected(missing_key):
    row = session_row("job-a", "s1")
    row[missing_key] = None

    with pytest.raises(ValueError, match=missing_key):
        aggregate_category_metrics([row], updated_at_ms=UPDATED_AT_MS)
