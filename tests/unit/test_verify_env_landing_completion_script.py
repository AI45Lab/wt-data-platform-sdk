from scripts.inspect.verify_env_landing_completion import compare_job_rows


def test_compare_job_rows_passes_when_env_and_landing_are_aligned():
    report = compare_job_rows(
        "job-a",
        [
            {"env_id": "session-1", "finished": True},
            {"env_id": "session-2", "finished": True},
        ],
        [
            {"session_id": "session-1", "is_session_completed": False},
            {"session_id": "session-1", "is_session_completed": True},
            {"session_id": "session-2", "is_session_completed": True},
        ],
    )

    assert report["status"] == "PASS"
    assert report["env_rows"] == 2
    assert report["landing_rows"] == 3
    assert report["landing_sessions"] == 2
    assert report["issues"] == []


def test_compare_job_rows_reports_finished_and_session_mismatches():
    report = compare_job_rows(
        "job-b",
        [
            {"env_id": "session-1", "finished": True},
            {"env_id": "session-1", "finished": False},
            {"env_id": "session-2", "finished": True},
        ],
        [
            {"session_id": "session-1", "is_session_completed": True},
            {"session_id": "session-1", "is_session_completed": True},
            {"session_id": "session-3", "is_session_completed": False},
            {"session_id": "", "is_session_completed": True},
        ],
    )

    assert report["status"] == "FAIL"
    assert report["all_env_finished"] is False
    assert report["duplicate_env_ids"] == ["session-1"]
    assert report["env_ids_missing_in_landing"] == ["session-2"]
    assert report["landing_sessions_missing_in_env"] == ["session-3"]
    assert report["sessions_with_multiple_completion_markers"] == ["session-1"]
    assert report["sessions_without_completion_marker"] == ["session-3"]
    assert "empty_landing_session_id" in report["issues"]


def test_compare_job_rows_accepts_arrow_like_boolean_values():
    class BoolLike:
        def __eq__(self, other):
            return other is True

    report = compare_job_rows(
        "job-c",
        [{"env_id": "session-1", "finished": BoolLike()}],
        [{"session_id": "session-1", "is_session_completed": BoolLike()}],
    )

    assert report["status"] == "PASS"
