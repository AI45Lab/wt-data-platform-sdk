"""Offline checks of sampling, patch exports, and non-semantic explanations."""

import json
from copy import deepcopy

import pytest

from wt_sdk.etl.tools.trainability_sample import process_session, sample_job, write_report


def _row(step, messages, *, completed=False, status=200, session_id="session-1"):
    return {
        "id": f"{session_id}-{step}",
        "job_id": "job'1",
        "session_id": session_id,
        "step_id": step,
        "messages": json.dumps(messages),
        "meta_json": json.dumps({"env_state": json.dumps({"status_code": status})}),
        "is_session_completed": completed,
        "is_trainable": True,
        "reward": 0.75 if completed else None,
        "response": '{"content":"preserve all source columns"}',
    }


def test_chain_explanations_cover_reset_duplicates_normalization_and_system_changes(monkeypatch):
    monkeypatch.setenv("TRAINABILITY_DOWNGRADE_LABEL", "false")
    system = {"role": "system", "content": "system-a"}
    user = {"role": "user", "content": "request"}
    normalized_user = {"role": "user", "content": [{"type": "text", "text": "request"}]}
    assistant = {"role": "assistant", "content": "response"}
    rows = [
        _row(1, [system, user]),
        _row(2, [system, normalized_user, assistant]),
        _row(3, [system, user]),
        _row(4, [system, user]),
        _row(5, [system, user, assistant]),
        _row(6, [{"role": "system", "content": "system-b"}, user]),
        _row(7, None, completed=True, status=500),
    ]
    rows[-1]["messages"] = "malformed JSON"
    snapshot = deepcopy(rows)
    result = process_session(rows, "job'1", "session-1")
    assert result["trainable_step_ids"] == [3, 5]
    evidence = {row["step_id"]: row["trainability_diagnostics"] for row in result["rows"]}
    assert evidence[2]["relation"] == "strict_prefix_extension"
    assert evidence[2]["matched_record_id"] == "session-1-1"
    assert evidence[2]["reason_code"] == "superseded_in_chain"
    assert evidence[3]["relation"] == "strict_prefix_reset"
    assert evidence[3]["matched_record_id"] == "session-1-2"
    assert evidence[3]["reason_code"] == "selected_chain_tail"
    assert evidence[4]["identical_active_record_ids"] == ["session-1-3"]
    assert evidence[5]["chain_root_record_id"] == "session-1-4"
    assert evidence[6]["reason_code"] == "singleton_side_chain"
    assert evidence[6]["previous_eligible_record_comparison"]["system_messages_equal"] is False
    assert evidence[7]["reason_code"] == "non_200_status"
    assert result["rows"][2]["reward"] == 0.75
    assert evidence[3]["stored_reward"] is None
    assert evidence[3]["reward_source_record_id"] == "session-1-7"
    assert all(row["response"] == snapshot[0]["response"] for row in result["rows"])
    assert rows == snapshot
    assert process_session(rows, "job'1", "session-1") == result


@pytest.mark.parametrize("downgrade", ["true", "false"])
def test_all_error_rows_are_exported_false_without_parsing_messages(monkeypatch, downgrade):
    monkeypatch.setenv("TRAINABILITY_DOWNGRADE_LABEL", downgrade)
    row = _row(1, None, completed=True, status=500)
    row["messages"] = "malformed JSON"
    result = process_session([row], "job'1", "session-1")
    assert result["trainable_step_ids"] == []
    assert result["rows"][0]["is_trainable"] is False
    assert result["rows"][0]["trainability_diagnostics"]["stored_is_trainable"] is True


def test_downgrade_exports_filtered_max_step_and_completion_warning(monkeypatch):
    monkeypatch.setenv("TRAINABILITY_DOWNGRADE_LABEL", "true")
    rows = [_row(1, None, completed=True), _row(2, None), _row(3, None, status=503)]
    result = process_session(rows, "job'1", "session-1")
    assert result["trainable_step_ids"] == [2]
    assert result["warnings"][0]["warning_type"] == "CompletionMarkerBeforeMaxStep"
    evidence = result["rows"][1]["trainability_diagnostics"]
    assert evidence["reason_code"] == "selected_max_eligible_step"


class _ReadOnlyClient:
    def __init__(self, *, bad_session=False):
        self.queries = []
        self.bad_session = bad_session

    def query_data(self, **kwargs):
        self.queries.append(kwargs)
        if kwargs.get("columns") == ["session_id"]:
            return [{"session_id": value} for value in ("session-2", "session-1", "session-2")]
        session_id = (
            "session-1" if "session_id = 'session-1'" in kwargs["filter_query"] else "session-2"
        )
        rows = [
            _row(1, [], session_id=session_id),
            _row(2, ["extension"], completed=True, session_id=session_id),
        ]
        if self.bad_session and session_id == "session-1":
            rows[-1]["messages"] = "malformed JSON"
        return rows


def test_sample_queries_only_selected_complete_sessions_and_exports_json(monkeypatch, tmp_path):
    monkeypatch.delenv("TRAINABILITY_DOWNGRADE_LABEL", raising=False)
    client = _ReadOnlyClient()
    report = sample_job(client, job_id="job'1", session_count=1, table="v2_landing_test", seed=5)
    assert len(client.queries) == 2
    assert all("job_id = 'job''1'" in query["filter_query"] for query in client.queries)
    assert all(query["partition"] == "job'1" for query in client.queries)
    assert "columns" not in client.queries[1]
    assert "limit" not in client.queries[1]
    assert report["sessions_processed"] == 1
    assert report["rows_processed"] == 2
    assert report["trainable_row_count"] == 1
    assert report["sampling"]["available_sessions"] == 2
    repeated = sample_job(
        _ReadOnlyClient(), job_id="job'1", session_count=1, table="v2_landing_test", seed=5
    )
    assert repeated == report
    output = tmp_path / "report.json"
    write_report(report, output)
    assert json.loads(output.read_text()) == report


def test_sampling_reports_session_errors_and_continues(monkeypatch):
    monkeypatch.setenv("TRAINABILITY_DOWNGRADE_LABEL", "false")
    report = sample_job(
        _ReadOnlyClient(bad_session=True), job_id="job'1", session_count=2, table="v2_landing_test"
    )
    assert report["sessions_processed"] == 1
    assert report["sessions_failed"] == 1
    failed = next(session for session in report["sessions"] if session["status"] == "failed")
    assert failed["error_type"] == "StageTransformError"
    assert failed["record_id"] == "session-1-2"
    assert failed["rows"][-1]["messages"] == "malformed JSON"


@pytest.mark.parametrize("count", [0, -1, 3])
def test_invalid_or_unavailable_sample_size_fails(count):
    with pytest.raises(ValueError):
        sample_job(_ReadOnlyClient(), job_id="job'1", session_count=count, table="v2_landing_test")
