import json
import sys

import pytest

import scripts.etl.run as run_module
from test_etl_pipeline import NormalizeClaudeMessagesStage
from wt_sdk.etl import build_serving_publish_pipeline


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

