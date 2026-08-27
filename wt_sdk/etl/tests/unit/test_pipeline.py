import json

import pytest

from wt_sdk.etl import (
    BuildChosenTraceStage,
    BuildSearchTextStage,
    DeriveJobTagsStage,
    ETLStage,
    FreeCotStage,
    PipelineConfigurationError,
    PipelineDefinition,
    PipelineInputScope,
    PipelineMode,
    SessionValidationError,
    StageTransformError,
    UpdateIsTrainableStage,
    load_pipeline,
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
        "agent_model": "opencode-model",
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


class SetTrainableStage(ETLStage):
    name = "set_trainable"
    output_fields = ("is_trainable",)

    def transform_session(self, session, context):
        del context
        return {
            record["id"]: {"is_trainable": True}
            for record in session
        }


class ProcessNonTrainableStage(ETLStage):
    name = "process_non_trainable"
    required_fields = ("id", "is_trainable")
    output_fields = ("search_text",)

    def transform_session(self, session, context):
        del context
        return {
            record["id"]: {"search_text": "non-trainable-stage-output"}
            for record in session
            if record.get("is_trainable") is False
        }


def test_canonical_serving_pipeline_builds_trace_and_tags_then_search_text():
    pipeline = load_pipeline("landing_to_serving_pipeline")

    assert [stage.name for stage in pipeline.ordered_stages] == [
        "build_chosen_trace",
        "derive_job_tags",
        "build_search_text",
    ]
    assert pipeline.version == "3"
    assert pipeline.input_scope is PipelineInputScope.MATCHED_ROWS

    result = pipeline.process_session([_row()])

    assert result.selected_rows == 1
    assert len(result.serving_records) == 1
    serving = result.serving_records[0]
    assert json.loads(serving.messages) == [{"role": "user", "content": "raw"}]
    assert json.loads(serving.chosen_trace) == [
        {"role": "user", "content": "raw"},
        {"role": "assistant", "content": "answer"},
    ]
    assert serving.search_text == "\n".join(
        [
            '[{"role":"user","content":"raw"},'
            '{"role":"assistant","content":"answer"}]',
            "opencode-model",
            json.dumps({"provider_messages": []}),
            "trajectory",
            "dataset",
            "harness",
            "model",
            "task",
        ]
    )
    assert serving.tags == ["dataset", "harness", "model", "task"]
    assert serving.source_updated_at == 1_754_000_000_000
    assert serving.serving_updated_at is None


def test_public_validate_dag_returns_topological_order_without_pipeline_run():
    ordered = PipelineDefinition.validate_dag(
        (BuildChosenTraceStage(), DeriveJobTagsStage())
    )

    assert [stage.name for stage in ordered] == [
        "build_chosen_trace",
        "derive_job_tags",
    ]


def test_independent_stage_keeps_factory_declaration_order():
    ordered = PipelineDefinition.validate_dag(
        (DeriveJobTagsStage(), BuildChosenTraceStage())
    )

    assert [stage.name for stage in ordered] == [
        "derive_job_tags",
        "build_chosen_trace",
    ]


def test_dependency_reorders_stages_even_when_declaration_is_reversed():
    class FirstStage(ETLStage):
        name = "first"
        output_fields = ("search_text",)

        def transform_session(self, session, context):
            del context
            return {record["id"]: {"search_text": "first"} for record in session}

    class SecondStage(ETLStage):
        name = "second"
        output_fields = ("reference_answer",)
        dependencies = ("first",)

        def transform_session(self, session, context):
            del context
            return {
                record["id"]: {"reference_answer": "second"}
                for record in session
            }

    ordered = PipelineDefinition.validate_dag((SecondStage(), FirstStage()))

    assert [stage.name for stage in ordered] == ["first", "second"]


def test_public_validate_dag_rejects_cycle():
    class FirstStage(ETLStage):
        name = "first"
        output_fields = ("search_text",)
        dependencies = ("second",)

        def transform_session(self, session, context):
            del session, context
            return {}

    class SecondStage(ETLStage):
        name = "second"
        output_fields = ("reference_answer",)
        dependencies = ("first",)

        def transform_session(self, session, context):
            del session, context
            return {}

    with pytest.raises(PipelineConfigurationError, match="cycle"):
        PipelineDefinition.validate_dag((FirstStage(), SecondStage()))


def test_describe_dag_returns_machine_readable_stage_inventory():
    pipeline = load_pipeline("landing_to_serving_pipeline")

    description = pipeline.describe_dag()

    assert description["pipeline_name"] == "landing_to_serving_pipeline"
    assert description["input_scope"] == "matched_rows"
    assert description["execution_order"] == [
        "build_chosen_trace",
        "derive_job_tags",
        "build_search_text",
    ]
    assert description["edges"] == [
        {"from": "build_chosen_trace", "to": "build_search_text"},
        {"from": "derive_job_tags", "to": "build_search_text"},
    ]
    assert description["stages"][0]["output_fields"] == ["chosen_trace"]
    assert description["stages"][1]["output_fields"] == ["tags"]
    assert description["stages"][2]["output_fields"] == ["search_text"]


def test_canonical_serving_pipeline_skips_session_when_no_stage_selects_rows():
    result = load_pipeline("landing_to_serving_pipeline").process_session(
        [_row(is_trainable=False)]
    )

    assert result.selected_rows == 0
    assert result.serving_records == ()


def test_matched_row_scope_is_rejected_for_landing_pipeline():
    with pytest.raises(PipelineConfigurationError, match="only for serving"):
        PipelineDefinition(
            name="invalid_landing_pipeline",
            version="1",
            mode=PipelineMode.LANDING,
            stages=(UpdateIsTrainableStage(),),
            input_scope=PipelineInputScope.MATCHED_ROWS,
        )


def test_matched_row_scope_requires_safe_pipeline_filter():
    with pytest.raises(PipelineConfigurationError, match="requires a safe"):
        PipelineDefinition(
            name="invalid_serving_pipeline",
            version="1",
            mode=PipelineMode.SERVING,
            stages=(ProcessNonTrainableStage(),),
            input_scope=PipelineInputScope.MATCHED_ROWS,
        )


def test_serving_pipeline_can_publish_non_trainable_row_from_independent_stage():
    pipeline = PipelineDefinition(
        name="mixed_trigger_serving_pipeline",
        version="1",
        mode=PipelineMode.SERVING,
        stages=(
            BuildChosenTraceStage(),
            DeriveJobTagsStage(),
            ProcessNonTrainableStage(),
        ),
    )

    result = pipeline.process_session([_row(is_trainable=False)])

    assert result.selected_rows == 1
    assert result.successful_rows == 1
    assert len(result.serving_records) == 1
    assert result.serving_records[0].search_text == "non-trainable-stage-output"
    assert result.serving_records[0].chosen_trace is None
    assert result.serving_records[0].tags is None


def test_landing_stage_controls_which_session_rows_it_selects():
    pipeline = PipelineDefinition(
        name="mixed_trigger_landing_pipeline",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(ProcessNonTrainableStage(),),
    )

    result = pipeline.process_session([_row(is_trainable=False)])

    assert result.selected_rows == 1
    assert result.successful_rows == 1
    assert result.landing_patches[0].updates == {
        "search_text": "non-trainable-stage-output"
    }


def test_builtin_factories_are_no_argument_cli_factories():
    serving = load_pipeline("landing_to_serving_pipeline")
    landing = load_pipeline("landing_enrichment_pipeline")

    assert [stage.name for stage in serving.ordered_stages] == [
        "build_chosen_trace",
        "derive_job_tags",
        "build_search_text",
    ]
    assert landing.mode is PipelineMode.LANDING
    assert [type(stage) for stage in landing.ordered_stages] == [
        UpdateIsTrainableStage,
        FreeCotStage,
    ]


def test_job_tags_are_best_effort_and_invalid_name_becomes_null():
    result = load_pipeline("landing_to_serving_pipeline").process_session(
        [_row(job_id="not-a-conventional-job")]
    )

    assert result.serving_records[0].tags is None


def test_chosen_trace_rejects_malformed_response_json_with_record_id():
    pipeline = load_pipeline("landing_to_serving_pipeline")

    with pytest.raises(StageTransformError, match="response contains malformed JSON") as exc:
        pipeline.process_session([_row(response="not-json")])

    assert exc.value.record_id == "row-1"


def test_collect_failure_discards_all_session_outputs():
    pipeline = load_pipeline("landing_to_serving_pipeline")
    rows = [
        _row(id="bad-row", step_id=0, response="not-json"),
        _row(id="good-row", step_id=1),
    ]

    result = pipeline.process_session(rows, collect_failures=True)

    assert result.source_rows == 2
    assert result.successful_rows == 0
    assert result.serving_records == ()
    assert len(result.failures) == 1
    assert result.failures[0].record_id == "bad-row"
    assert result.failures[0].stage_name == "build_chosen_trace"


def test_stage_warning_is_reported_without_interrupting_stage_or_downstream_logic():
    execution = []

    class WarnAndContinueStage(ETLStage):
        name = "warn_and_continue"
        output_fields = ("search_text",)

        def transform_session(self, session, context):
            context.warn(
                "response required fallback normalization",
                warning_type="FallbackNormalization",
            )
            execution.append("continued_after_warning")
            return {session[0]["id"]: {"search_text": "normalized"}}

    class DownstreamStage(ETLStage):
        name = "downstream"
        output_fields = ("reference_answer",)
        dependencies = ("warn_and_continue",)

        def transform_session(self, session, context):
            del context
            execution.append("downstream_executed")
            assert session[0]["search_text"] == "normalized"
            return {session[0]["id"]: {"reference_answer": "done"}}

    pipeline = PipelineDefinition(
        name="warning_pipeline",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(WarnAndContinueStage(), DownstreamStage()),
    )

    result = pipeline.process_session([_row()])

    assert execution == ["continued_after_warning", "downstream_executed"]
    assert result.successful_rows == 1
    assert result.failures == ()
    assert len(result.warnings) == 1
    warning = result.warnings[0]
    assert warning.job_id == _row()["job_id"]
    assert warning.session_id == "session-1"
    assert warning.stage_name == "warn_and_continue"
    assert warning.warning_type == "FallbackNormalization"
    assert warning.message == "response required fallback normalization"
    assert result.landing_patches[0].updates == {
        "reference_answer": "done",
        "search_text": "normalized",
    }


def test_stage_warning_is_retained_when_stage_later_fails():
    class WarnThenFailStage(ETLStage):
        name = "warn_then_fail"
        output_fields = ("search_text",)

        def transform_session(self, session, context):
            context.warn("suspicious source data")
            raise StageTransformError(
                "source data cannot be transformed",
                record_id=session[0]["id"],
            )

    pipeline = PipelineDefinition(
        name="warning_then_failure",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(WarnThenFailStage(),),
    )

    result = pipeline.process_session([_row()], collect_failures=True)

    assert result.successful_rows == 0
    assert result.landing_patches == ()
    assert len(result.warnings) == 1
    assert result.warnings[0].message == "suspicious source data"
    assert len(result.failures) == 1
    assert result.failures[0].stage_name == "warn_then_fail"


def test_landing_pipeline_returns_only_actual_final_diff():
    pipeline = PipelineDefinition(
        name="landing_enrichment",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(SetTrainableStage(),),
    )

    unchanged = pipeline.process_session([_row(is_trainable=True)])
    changed = pipeline.process_session([_row(is_trainable=False)])

    assert unchanged.selected_rows == 1
    assert unchanged.landing_patches == ()
    assert changed.landing_patches[0].updates == {"is_trainable": True}


def test_each_stage_sees_complete_session_after_previous_stage_barrier():
    class NormalizeAllMessagesStage(ETLStage):
        name = "normalize_all_messages"
        required_fields = ("id", "messages")
        output_fields = ("messages",)

        def transform_session(self, session, context):
            del context
            return {
                record["id"]: {
                    "messages": json.dumps(
                        [
                            *json.loads(record["messages"]),
                            {"role": "system", "content": "normalized"},
                        ]
                    )
                }
                for record in session
            }

    class AnalyzeNormalizedSessionStage(ETLStage):
        name = "analyze_normalized_session"
        required_fields = ("id", "step_id", "messages", "is_trainable")
        output_fields = ("is_trainable",)
        dependencies = ("normalize_all_messages",)

        def transform_session(self, session, context):
            del context
            assert all(
                json.loads(record["messages"])[-1]["content"] == "normalized"
                for record in session
            )
            tail_step = max(record["step_id"] for record in session)
            return {
                record["id"]: {"is_trainable": record["step_id"] == tail_step}
                for record in session
            }

    pipeline = PipelineDefinition(
        name="session_barrier",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(AnalyzeNormalizedSessionStage(), NormalizeAllMessagesStage()),
    )
    rows = [
        _row(id="row-1", step_id=0, is_trainable=True),
        _row(id="row-2", step_id=1, is_trainable=False),
    ]

    result = pipeline.process_session(rows)

    assert [stage.name for stage in pipeline.ordered_stages] == [
        "normalize_all_messages",
        "analyze_normalized_session",
    ]
    assert result.selected_rows == 2
    patches = {patch.record_id: patch.updates for patch in result.landing_patches}
    assert patches["row-1"]["is_trainable"] is False
    assert patches["row-2"]["is_trainable"] is True
    assert all(
        json.loads(patch["messages"])[-1]["content"] == "normalized"
        for patch in patches.values()
    )


def test_stage_receives_recursively_immutable_session():
    class MutatingStage(ETLStage):
        name = "mutating"
        output_fields = ("search_text",)

        def transform_session(self, session, context):
            del context
            session[0]["search_text"] = "illegal"
            return {}

    pipeline = PipelineDefinition(
        name="immutable_input",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(MutatingStage(),),
    )

    with pytest.raises(StageTransformError, match="failed for session"):
        pipeline.process_session([_row()])


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

        def transform_session(self, session, context):
            del session, context
            return {}

    with pytest.raises(PipelineConfigurationError, match="outside the unified schema"):
        PipelineDefinition(
            name="invalid",
            version="1",
            mode=PipelineMode.LANDING,
            stages=(UnknownOutputStage(),),
        )


@pytest.mark.parametrize(
    ("stage_result", "message"),
    [
        ([], "dict keyed by record ID"),
        ({"unknown": {"search_text": "x"}}, "unknown record ID"),
        ({"row-1": {}}, "non-empty dict patch"),
        ({"row-1": {"reference_answer": "x"}}, "undeclared fields"),
    ],
)
def test_session_patch_contract_is_validated(stage_result, message):
    class InvalidPatchStage(ETLStage):
        name = "invalid_patch"
        output_fields = ("search_text",)

        def transform_session(self, session, context):
            del session, context
            return stage_result

    pipeline = PipelineDefinition(
        name="invalid_patch",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(InvalidPatchStage(),),
    )

    with pytest.raises(StageTransformError, match=message):
        pipeline.process_session([_row()])


def test_session_scope_validation_warns_and_continues_for_duplicate_step_id():
    stage_executed = []

    class CaptureExecutionStage(ETLStage):
        name = "capture_execution"
        output_fields = ("is_trainable",)

        def transform_session(self, session, context):
            del context
            stage_executed.append(True)
            return {
                record["id"]: {"is_trainable": True}
                for record in session
            }

    pipeline = PipelineDefinition(
        name="landing_enrichment",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(CaptureExecutionStage(),),
    )

    result = pipeline.process_session([_row(id="row-2"), _row(id="row-1")])

    assert stage_executed == [True]
    assert result.failures == ()
    assert result.successful_rows == 2
    assert len(result.warnings) == 1
    warning = result.warnings[0]
    assert warning.job_id == _row()["job_id"]
    assert warning.session_id == "session-1"
    assert warning.stage_name == "__session_validation__"
    assert warning.warning_type == "DuplicateStepId"
    assert warning.message == "session contains duplicate step_id values: [0]"


@pytest.mark.parametrize(
    ("field", "value"),
    [("step_id", 1.5), ("source_updated_at", None)],
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
