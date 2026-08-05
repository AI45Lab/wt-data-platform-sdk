import json

import pytest

from wt_sdk.etl import (
    BuildChosenTraceStage,
    DeriveJobTagsStage,
    ETLStage,
    PipelineConfigurationError,
    PipelineDefinition,
    PipelineMode,
    SessionValidationError,
    StageContext,
    StageTransformError,
    build_serving_publish_pipeline,
)


def _row(**updates):
    row = {
        "dataset_type": "trajectory",
        "dt": "2026-08-05",
        "id": "row-1",
        "session_id": "session-1",
        "created_at": 1_754_000_000,
        "source_updated_at": 1_754_000_000_000,
        "serving_updated_at": None,
        "step_id": 0,
        "is_terminal": False,
        "step_reward": None,
        "reward": None,
        "messages": json.dumps([{"role": "user", "content": "raw"}]),
        "response": json.dumps({"role": "assistant", "content": "answer"}),
        "chosen_trace": None,
        "rejected_trace": None,
        "ground_truth_answer": None,
        "reference_answer": None,
        "search_text": None,
        "agent_model": "claude-3-7-sonnet",
        "env_name": "test-env",
        "is_session_completed": True,
        "is_trainable": True,
        "meta_json": json.dumps({"provider_messages": []}),
        "tags": None,
        "env_id": "env-1",
        "job_id": "dataset#harness#model#task#20260805#owner#extra",
        "is_truncated": False,
        "blob_manifest": [],
    }
    row.update(updates)
    return row


class NormalizeClaudeMessagesStage(ETLStage):
    name = "normalize_claude_messages"
    version = "1"
    required_fields = ("agent_model", "meta_json")
    output_fields = ("messages",)

    def applies(self, record, context: StageContext) -> bool:
        del context
        return "claude" in str(record.get("agent_model") or "").lower()

    def transform(self, record, context: StageContext):
        del record, context
        return {
            "messages": json.dumps(
                [{"role": "user", "content": "normalized"}],
                separators=(",", ":"),
            )
        }


class SetTrainableStage(ETLStage):
    name = "set_trainable"
    output_fields = ("is_trainable",)

    def transform(self, record, context):
        del record, context
        return {"is_trainable": True}


def test_canonical_serving_pipeline_applies_claude_then_trace_then_tags():
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())

    assert [stage.name for stage in pipeline.ordered_stages] == [
        "normalize_claude_messages",
        "build_chosen_trace",
        "derive_job_tags",
    ]

    result = pipeline.process_session([_row()])

    assert result.selected_rows == 1
    assert len(result.serving_records) == 1
    serving = result.serving_records[0]
    assert json.loads(serving.messages) == [
        {"role": "user", "content": "normalized"}
    ]
    assert json.loads(serving.chosen_trace) == [
        {"role": "user", "content": "normalized"},
        {"role": "assistant", "content": "answer"},
    ]
    assert serving.tags == ["dataset", "harness", "model", "task"]
    assert serving.source_updated_at == 1_754_000_000_000
    assert serving.serving_updated_at is None


def test_public_validate_dag_returns_topological_order_without_pipeline_run():
    ordered = PipelineDefinition.validate_dag(
        (
            DeriveJobTagsStage(),
            BuildChosenTraceStage(),
            NormalizeClaudeMessagesStage(),
        )
    )

    assert [stage.name for stage in ordered] == [
        "normalize_claude_messages",
        "build_chosen_trace",
        "derive_job_tags",
    ]


def test_public_validate_dag_rejects_cycle():
    class FirstStage(ETLStage):
        name = "first"
        output_fields = ("search_text",)
        dependencies = ("second",)

        def transform(self, record, context):
            del record, context
            return {"search_text": "first"}

    class SecondStage(ETLStage):
        name = "second"
        output_fields = ("reference_answer",)
        dependencies = ("first",)

        def transform(self, record, context):
            del record, context
            return {"reference_answer": "second"}

    with pytest.raises(PipelineConfigurationError, match="cycle"):
        PipelineDefinition.validate_dag((FirstStage(), SecondStage()))


def test_describe_dag_returns_machine_readable_stage_inventory():
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())

    description = pipeline.describe_dag()

    assert description["pipeline_name"] == "serving_publish"
    assert description["execution_order"] == [
        "normalize_claude_messages",
        "build_chosen_trace",
        "derive_job_tags",
    ]
    assert description["edges"] == [
        {"from": "normalize_claude_messages", "to": "build_chosen_trace"},
        {"from": "build_chosen_trace", "to": "derive_job_tags"},
    ]
    assert description["stages"][1]["output_fields"] == ["chosen_trace"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"is_trainable": False},
        {"agent_model": "gpt-5"},
    ],
)
def test_canonical_serving_pipeline_requires_trainable_and_claude(overrides):
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())

    result = pipeline.process_session([_row(**overrides)])

    assert result.selected_rows == 0
    assert result.serving_records == ()


def test_job_tags_are_best_effort_and_invalid_name_becomes_null():
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())

    result = pipeline.process_session([_row(job_id="not-a-conventional-job")])

    assert result.serving_records[0].tags is None


def test_chosen_trace_rejects_malformed_response_json():
    pipeline = build_serving_publish_pipeline(NormalizeClaudeMessagesStage())

    with pytest.raises(StageTransformError, match="response contains malformed JSON"):
        pipeline.process_session([_row(response="not-json")])


def test_landing_pipeline_returns_only_actual_diff():
    pipeline = PipelineDefinition(
        name="landing_enrichment",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(SetTrainableStage(),),
    )

    unchanged = pipeline.process_session([_row(is_trainable=True)])
    changed = pipeline.process_session([_row(is_trainable=False)])

    assert unchanged.landing_patches == ()
    assert changed.landing_patches[0].updates == {"is_trainable": True}


def test_pipeline_rejects_conflicting_output_owners():
    class OtherTrainableStage(SetTrainableStage):
        name = "other_trainable"

    with pytest.raises(PipelineConfigurationError, match="owned by both"):
        PipelineDefinition(
            name="invalid",
            version="1",
            mode=PipelineMode.LANDING,
            stages=(SetTrainableStage(), OtherTrainableStage()),
        )


def test_pipeline_rejects_fields_outside_unified_schema():
    class UnknownOutputStage(ETLStage):
        name = "unknown_output"
        output_fields = ("not_a_real_column",)

        def transform(self, record, context):
            del record, context
            return {"not_a_real_column": "value"}

    with pytest.raises(PipelineConfigurationError, match="outside the unified schema"):
        PipelineDefinition(
            name="invalid",
            version="1",
            mode=PipelineMode.LANDING,
            stages=(UnknownOutputStage(),),
        )


def test_stage_predicate_must_return_bool():
    class InvalidPredicateStage(SetTrainableStage):
        def applies(self, record, context):
            del record, context
            return "yes"

    pipeline = PipelineDefinition(
        name="invalid_predicate",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(InvalidPredicateStage(),),
    )

    with pytest.raises(StageTransformError, match="must return bool"):
        pipeline.process_session([_row()])


def test_applicable_stage_requires_dependencies_to_have_run_for_record():
    pipeline = PipelineDefinition(
        name="dependency_runtime_check",
        version="1",
        mode=PipelineMode.SERVING,
        stages=(NormalizeClaudeMessagesStage(), BuildChosenTraceStage()),
    )

    with pytest.raises(StageTransformError, match="dependencies did not run"):
        pipeline.process_session([_row(agent_model="gpt-5")])


def test_session_scope_validation_rejects_duplicate_step_id():
    pipeline = PipelineDefinition(
        name="landing_enrichment",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(SetTrainableStage(),),
    )
    second = _row(id="row-2")

    with pytest.raises(SessionValidationError, match="duplicate step_id"):
        pipeline.process_session([_row(), second])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("step_id", 1.5),
        ("source_updated_at", None),
    ],
)
def test_session_scope_validation_rejects_invalid_cursor_fields(field, value):
    pipeline = PipelineDefinition(
        name="landing_enrichment",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(SetTrainableStage(),),
    )

    with pytest.raises(SessionValidationError, match=field):
        pipeline.process_session([_row(**{field: value})])
