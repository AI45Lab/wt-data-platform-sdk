import json
import sys

import pytest

import scripts.etl.run as run_module
from test_etl_pipeline import NormalizeClaudeMessagesStage
from wt_sdk.etl import (
    ETLRunFailed,
    PipelineMode,
    RecordFailure,
    RunSummary,
    SessionKey,
    build_serving_publish_pipeline,
)


def test_builtin_serving_factory_reference_is_directly_loadable():
    pipeline = run_module._load_pipeline(
        "wt_sdk.etl.pipelines:build_serving_pipeline"
    )

    assert pipeline.name == "serving_publish"
    assert [stage.name for stage in pipeline.ordered_stages] == [
        "build_chosen_trace",
        "derive_job_tags",
    ]


def test_parser_accepts_repeated_job_and_session_ids():
    args = run_module.build_parser().parse_args(
        [
            "--pipeline-factory",
            "wt_sdk.etl.pipelines:build_serving_pipeline",
            "--job-id",
            "job-1",
            "--session-id",
            "session-1",
            "--session-id",
            "session-2",
        ]
    )

    assert args.job_id == ["job-1"]
    assert args.session_id == ["session-1", "session-2"]


@pytest.mark.parametrize(
    ("flag", "expect_details"),
    [
        ("--validate-only", False),
        ("--list-stages", True),
    ],
)
def test_stage_introspection_does_not_create_database_client(
    monkeypatch,
    capsys,
    flag,
    expect_details,
):
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())
    monkeypatch.setattr(run_module, "_load_pipeline", lambda reference: pipeline)
    monkeypatch.setattr(
        run_module,
        "WTGatewayClient",
        lambda *args, **kwargs: pytest.fail("introspection must not create a client"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["run.py", "--pipeline-factory", "example:factory", flag],
    )

    assert run_module.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["pipeline_count"] == 1
    assert payload["pipelines"][0]["execution_order"] == [
        "normalize_claude_messages",
        "build_chosen_trace",
        "derive_job_tags",
    ]
    assert ("stages" in payload["pipelines"][0]) is expect_details


def test_etl_execution_still_requires_explicit_profile(monkeypatch):
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())
    monkeypatch.setattr(run_module, "_load_pipeline", lambda reference: pipeline)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run.py", "--pipeline-factory", "example:factory", "--job-id", "job-1"],
    )

    with pytest.raises(SystemExit, match="--profile is required"):
        run_module.main()


def test_summary_payload_contains_audit_counts_and_failed_row_ids():
    summary = RunSummary(
        pipeline_name="serving_publish",
        pipeline_version="1",
        mode=PipelineMode.SERVING,
        discovery_rows=5,
        source_rows=4,
        selected_rows=3,
        successful_rows=2,
        failed_rows=1,
        serving_rows_upserted=2,
        dirty_sessions={SessionKey("job-1", "session-1")},
        failures=[
            RecordFailure(
                record_id="bad-row",
                job_id="job-1",
                session_id="session-1",
                stage_name="build_chosen_trace",
                error_type="StageTransformError",
                message="malformed response",
            )
        ],
    )

    payload = run_module._summary_payload(
        summary,
        pipeline_run_id="serving_publish__v1__run",
        started_at_ms=1_000,
        ended_at_ms=1_250,
    )

    assert payload["status"] == "FAILED"
    assert payload["failed_row_ids"] == ["bad-row"]
    assert payload["started_at_ms"] == 1_000
    assert payload["ended_at_ms"] == 1_250
    assert payload["duration_ms"] == 250
    assert payload["dirty_sessions"] == [
        {"job_id": "job-1", "session_id": "session-1"}
    ]
    assert payload["audit"] == {
        "discovery_rows_read": 5,
        "source_rows_read": 4,
        "rows_selected": 3,
        "rows_succeeded": 2,
        "rows_failed": 1,
        "landing_rows_updated": 0,
        "serving_rows_upserted": 2,
    }


def test_failed_pipeline_prints_report_and_returns_nonzero(
    monkeypatch,
    capsys,
    tmp_path,
):
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())
    summary = RunSummary(
        pipeline_name=pipeline.name,
        pipeline_version=pipeline.version,
        mode=pipeline.mode,
        source_rows=1,
        selected_rows=1,
        failed_rows=1,
        failures=[
            RecordFailure(
                record_id="bad-row",
                job_id="job-1",
                session_id="session-1",
                stage_name="build_chosen_trace",
                error_type="StageTransformError",
                message="bad JSON",
            )
        ],
    )

    class FakeClient:
        def __init__(self, config):
            self.config = config

        def close(self):
            return None

    class FakeEngine:
        def __init__(self, client, checkpoint_store=None):
            del client, checkpoint_store

        def run_sessions(self, pipeline, session_keys, dry_run=False):
            del pipeline, session_keys, dry_run
            raise ETLRunFailed(summary)

    monkeypatch.setattr(run_module, "_load_pipeline", lambda reference: pipeline)
    monkeypatch.setattr(run_module, "WTGatewayClient", FakeClient)
    monkeypatch.setattr(run_module, "ETLEngine", FakeEngine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run.py",
            "--pipeline-factory",
            "example:factory",
            "--profile",
            "test",
            "--job-id",
            "job-1",
            "--session-id",
            "session-1",
            "--report-dir",
            str(tmp_path),
        ],
    )

    assert run_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == "FAILED"
    assert payload[0]["failed_row_ids"] == ["bad-row"]
    report_path = tmp_path / f"{payload[0]['pipeline_run_id']}.json"
    assert report_path.exists()
    assert json.loads(report_path.read_text())["ended_at"] == payload[0]["ended_at"]
