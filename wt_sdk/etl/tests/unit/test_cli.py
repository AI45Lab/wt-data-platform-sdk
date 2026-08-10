import json
import sys

import pytest

import wt_sdk.etl.cli.run as run_module
from wt_sdk.etl import (
    ETLRunFailed,
    PipelineMode,
    RecordFailure,
    RunSummary,
    SessionKey,
)


def test_builtin_pipeline_short_name_is_directly_loadable():
    pipeline = run_module.load_pipeline("landing_to_serving_pipeline")

    assert pipeline.name == "landing_to_serving_pipeline"
    assert [stage.name for stage in pipeline.ordered_stages] == [
        "build_chosen_trace",
        "derive_job_tags",
        "build_search_text",
    ]


def test_parser_accepts_job_and_session_id_lists():
    args = run_module.build_parser().parse_args(
        [
            "--pipeline",
            "landing_to_serving_pipeline",
            "--job-id",
            "job-1",
            "--session-id",
            "session-1",
            "session-2",
        ]
    )

    assert args.job_id == ["job-1"]
    assert args.session_id == ["session-1", "session-2"]

    jobs = run_module.build_parser().parse_args(
        [
            "--pipeline",
            "landing_to_serving_pipeline",
            "--job-id",
            "job-1",
            "job-2",
        ]
    )
    assert jobs.job_id == ["job-1", "job-2"]


def test_parser_accepts_exact_session_pairs_from_multiple_jobs():
    args = run_module.build_parser().parse_args(
        [
            "--pipeline",
            "landing_to_serving_pipeline",
            "--session",
            "job-1",
            "session-1",
            "--session",
            "job-2",
            "session-2",
        ]
    )

    assert args.session == [["job-1", "session-1"], ["job-2", "session-2"]]


def test_parser_defaults_to_zero_settle_delay():
    args = run_module.build_parser().parse_args(
        ["--pipeline", "landing_to_serving_pipeline"]
    )

    assert args.settle_delay_seconds == 0


def test_list_pipelines_uses_short_module_names(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["run.py", "--list-pipelines"],
    )

    assert run_module.main() == 0
    assert json.loads(capsys.readouterr().out)["pipelines"] == [
        "landing_enrichment_pipeline",
        "landing_to_serving_pipeline",
    ]


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
    pipeline = run_module.load_pipeline("landing_to_serving_pipeline")
    monkeypatch.setattr(run_module, "load_pipeline", lambda name: pipeline)
    monkeypatch.setattr(
        run_module,
        "WTGatewayClient",
        lambda *args, **kwargs: pytest.fail("introspection must not create a client"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["run.py", "--pipeline", "example_pipeline", flag],
    )

    assert run_module.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["pipeline_count"] == 1
    assert payload["pipelines"][0]["execution_order"] == [
        "build_chosen_trace",
        "derive_job_tags",
        "build_search_text",
    ]
    assert ("stages" in payload["pipelines"][0]) is expect_details


def test_manual_etl_defaults_to_test_and_does_not_require_state_uri(
    monkeypatch,
    tmp_path,
):
    pipeline = run_module.load_pipeline("landing_to_serving_pipeline")
    monkeypatch.delenv("WT_SDK_PROFILE", raising=False)
    monkeypatch.delenv("WT_SDK_ETL_STATE_DB_URI", raising=False)

    class FakeClient:
        def __init__(self, config):
            assert config.tables.profile == "test"
            self.config = config

        def close(self):
            return None

    class FakeEngine:
        def __init__(self, client, checkpoint_store=None):
            assert checkpoint_store is None

        def run_jobs(self, pipeline, job_ids, dry_run=False):
            assert job_ids == ["job-1"]
            return RunSummary(
                pipeline_name=pipeline.name,
                pipeline_version=pipeline.version,
                mode=pipeline.mode,
            )

    monkeypatch.setattr(run_module, "load_pipeline", lambda name: pipeline)
    monkeypatch.setattr(run_module, "WTGatewayClient", FakeClient)
    monkeypatch.setattr(run_module, "ETLEngine", FakeEngine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run.py",
            "--pipeline",
            "example_pipeline",
            "--job-id",
            "job-1",
            "--dry-run",
            "--report-dir",
            str(tmp_path),
        ],
    )

    assert run_module.main() == 0


def test_incremental_uses_env_profile_for_test_checkpoint_table(
    monkeypatch,
    tmp_path,
):
    pipeline = run_module.load_pipeline("landing_to_serving_pipeline")
    captured = {}

    class FakeClient:
        def __init__(self, config):
            self.config = config

        def close(self):
            return None

    class FakeCheckpointStore:
        def __init__(self, db_uri, table_name):
            captured["db_uri"] = db_uri
            captured["table_name"] = table_name

        def verify_ready(self):
            return None

        def close(self):
            return None

    class FakeEngine:
        def __init__(self, client, checkpoint_store=None):
            assert client.config.tables.profile == "test"
            assert checkpoint_store is not None

        def run_incremental(self, pipeline, **kwargs):
            captured["settle_delay_ms"] = kwargs["settle_delay_ms"]
            captured["run_started_at_ms"] = kwargs["run_started_at_ms"]
            return RunSummary(
                pipeline_name=pipeline.name,
                pipeline_version=pipeline.version,
                mode=pipeline.mode,
            )

    monkeypatch.setenv("WT_SDK_PROFILE", "test")
    monkeypatch.setenv("WT_SDK_ETL_STATE_DB_URI", "s3://wind-tunnel-etl")
    monkeypatch.setattr(run_module.sdk_time, "now_ms", lambda: 12_000)
    monkeypatch.setattr(run_module, "load_pipeline", lambda name: pipeline)
    monkeypatch.setattr(run_module, "WTGatewayClient", FakeClient)
    monkeypatch.setattr(run_module, "DldbCheckpointStore", FakeCheckpointStore)
    monkeypatch.setattr(run_module, "ETLEngine", FakeEngine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run.py",
            "--pipeline",
            "example_pipeline",
            "--start-from",
            "0",
            "--dry-run",
            "--report-dir",
            str(tmp_path),
        ],
    )

    assert run_module.main() == 0
    assert captured == {
        "db_uri": "s3://wind-tunnel-etl",
        "table_name": "etl_checkpoints_test",
        "settle_delay_ms": 0,
        "run_started_at_ms": 12_000,
    }


@pytest.mark.parametrize(
    ("delay_args", "expected_end_ms"),
    [
        ([], 12_000),
        (["--settle-delay-seconds", "2"], 10_000),
    ],
)
def test_open_ended_manual_range_uses_fixed_start_cutoff_and_optional_delay(
    monkeypatch,
    tmp_path,
    delay_args,
    expected_end_ms,
):
    pipeline = run_module.load_pipeline("landing_to_serving_pipeline")
    captured = {}

    class FakeClient:
        def __init__(self, config):
            self.config = config

        def close(self):
            return None

    class FakeEngine:
        def __init__(self, client, checkpoint_store=None):
            assert checkpoint_store is None

        def run_range(self, pipeline, *, start_ms, end_ms, page_size, dry_run):
            captured.update(start_ms=start_ms, end_ms=end_ms)
            return RunSummary(
                pipeline_name=pipeline.name,
                pipeline_version=pipeline.version,
                mode=pipeline.mode,
            )

    monkeypatch.setattr(run_module.sdk_time, "now_ms", lambda: 12_000)
    monkeypatch.setattr(run_module, "load_pipeline", lambda name: pipeline)
    monkeypatch.setattr(run_module, "WTGatewayClient", FakeClient)
    monkeypatch.setattr(run_module, "ETLEngine", FakeEngine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run.py",
            "--pipeline",
            "example_pipeline",
            "--start-time",
            "1",
            "--dry-run",
            "--report-dir",
            str(tmp_path),
            *delay_args,
        ],
    )

    assert run_module.main() == 0
    assert captured == {"start_ms": 1_000, "end_ms": expected_end_ms}


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


def test_dirty_handoff_excludes_sessions_already_processed_by_serving():
    already_processed = SessionKey("job-1", "session-1")
    needs_handoff = SessionKey("job-2", "session-2")
    summary = RunSummary(
        pipeline_name="landing_to_serving_pipeline",
        pipeline_version="1",
        mode=PipelineMode.SERVING,
        successful_sessions={already_processed},
    )

    assert run_module._pending_dirty_sessions(
        {already_processed, needs_handoff},
        summary,
    ) == {needs_handoff}


def test_failed_pipeline_prints_report_and_returns_nonzero(
    monkeypatch,
    capsys,
    tmp_path,
):
    pipeline = run_module.load_pipeline("landing_to_serving_pipeline")
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

    monkeypatch.setenv("WT_SDK_PROFILE", "test")
    monkeypatch.setattr(run_module, "load_pipeline", lambda name: pipeline)
    monkeypatch.setattr(run_module, "WTGatewayClient", FakeClient)
    monkeypatch.setattr(run_module, "ETLEngine", FakeEngine)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run.py",
            "--pipeline",
            "example_pipeline",
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
