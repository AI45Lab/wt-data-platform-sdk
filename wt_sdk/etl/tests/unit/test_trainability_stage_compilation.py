"""Guard the trainability stage's integration into the ETL package.

Merging stage changes into the ETL must never introduce syntax, import, or
pipeline-wiring errors. These tests exercise that boundary without any
gateway access: the stage source is compiled explicitly, the stage and
pipeline modules are imported, the landing pipeline is assembled with the
stage wired in, and the sampling tool's offline session path runs the stage
end to end.

The synthetic session keeps one append-only main chain of exactly
``MIN_SIDE_CHAIN_LENGTH`` records — the shortest length that survives the
short-side-chain rule once the session also contains a second chain — plus a
single-record side chain. The completion marker sits on the maximum step_id
record so no data-quality warnings are emitted.
"""

import importlib
import json
import pkgutil
from pathlib import Path

from wt_sdk.etl import UpdateIsTrainableStage, load_pipeline
from wt_sdk.etl.stages.trainability import MIN_SIDE_CHAIN_LENGTH
from wt_sdk.etl.tools.trainability_sample import process_session

_JOB_ID = "dataset#harness#model#task#20260911#owner#extra"
_SESSION_ID = "session-1"


def test_trainability_stage_source_compiles():
    stage_path = Path(__file__).parents[2] / "stages" / "trainability.py"
    compile(stage_path.read_text(encoding="utf-8"), str(stage_path), "exec")


def test_every_stage_and_pipeline_module_imports():
    for package_name in ("wt_sdk.etl.stages", "wt_sdk.etl.pipelines"):
        package = importlib.import_module(package_name)
        for module_info in pkgutil.iter_modules(
            package.__path__,
            prefix=f"{package_name}.",
        ):
            importlib.import_module(module_info.name)
    importlib.import_module("wt_sdk.etl.tools.trainability_sample")


def test_trainability_stage_wired_into_landing_pipeline():
    pipeline = load_pipeline("landing_enrichment_pipeline")

    trainability_stage = pipeline.ordered_stages[0]
    assert isinstance(trainability_stage, UpdateIsTrainableStage)
    assert trainability_stage.name == "update_is_trainable"
    assert trainability_stage.dependencies == ()
    assert pipeline.ordered_stages[1].dependencies == ("update_is_trainable",)


def test_trainability_stage_runs_inside_landing_pipeline():
    pipeline = load_pipeline("landing_enrichment_pipeline")

    result = pipeline.process_session(_completed_session())

    assert result.failures == ()
    assert result.successful_rows == MIN_SIDE_CHAIN_LENGTH + 1
    patches = {patch.record_id: patch.updates for patch in result.landing_patches}
    # Landing patches are the actual final diff only: the tail's stored reward
    # already equals the completion reward it is copied from, so the visible
    # change is the trainable flip alone.
    assert set(patches) == {"row-tail"}
    assert patches["row-tail"] == {"is_trainable": True}


def test_sample_tool_process_session_runs_stage_and_agrees_with_patches():
    report = process_session(_completed_session(), _JOB_ID, _SESSION_ID)

    assert report["status"] == "processed"
    assert report["warnings"] == []
    assert report["row_count"] == MIN_SIDE_CHAIN_LENGTH + 1
    assert report["eligible_row_count"] == MIN_SIDE_CHAIN_LENGTH + 1
    assert report["trainable_step_ids"] == [_tail_step_id()]
    assert report["is_trainable_true_count"] == 1
    reason_codes = {
        row["id"]: row["trainability_diagnostics"]["reason_code"]
        for row in report["rows"]
    }
    assert len(reason_codes) == MIN_SIDE_CHAIN_LENGTH + 1
    assert reason_codes["row-tail"] == "selected_chain_tail"
    assert reason_codes["row-side"] == "short_side_chain"
    assert all(
        code == "superseded_in_chain"
        for record_id, code in reason_codes.items()
        if record_id not in {"row-tail", "row-side"}
    )
    # The stage patch (before the pipeline's final-diff reduction) must carry
    # the completion record's non-null reward onto the trainable tail.
    tail_row = next(row for row in report["rows"] if row["id"] == "row-tail")
    assert tail_row["trainability_diagnostics"]["stage_patch"] == {
        "is_trainable": True,
        "reward": 0.7,
    }
    assert (
        tail_row["trainability_diagnostics"]["reward_source_record_id"] == "row-tail"
    )


def _row(row_id: str, step_id: int, messages: list, **updates: object) -> dict:
    row = {
        "dataset_type": "trajectory",
        "dt": "2026-09-11",
        "id": row_id,
        "session_id": _SESSION_ID,
        "created_at": 1_758_000_000,
        "source_updated_at": 1_758_000_000_000,
        "serving_updated_at": None,
        "step_id": step_id,
        "is_terminal": False,
        "step_reward": None,
        "reward": None,
        "messages": json.dumps(messages),
        "response": json.dumps({"role": "assistant", "content": "answer"}),
        "chosen_trace": None,
        "rejected_trace": None,
        "ground_truth_answer": None,
        "reference_answer": None,
        "search_text": None,
        "agent_model": "opencode-model",
        "env_name": "test-env",
        "is_session_completed": False,
        "is_trainable": False,
        "meta_json": json.dumps({"provider_messages": []}),
        "tags": None,
        "env_id": "env-1",
        "job_id": _JOB_ID,
        "is_truncated": False,
        "blob_manifest": [],
    }
    row.update(updates)
    return row


def _tail_step_id() -> int:
    """The step_id of the main chain's final record.

    Main-chain records occupy steps 0..9 and 11..(MIN_SIDE_CHAIN_LENGTH);
    the single side-chain record takes step 10 in between. The tail therefore
    carries the session's maximum step_id and the completion marker.
    """

    return MIN_SIDE_CHAIN_LENGTH


def _completed_session() -> list[dict]:
    user_turn = {"role": "user", "content": "question"}
    assistant_turn = {
        "role": "assistant",
        "content": [{"type": "text", "text": "answer"}],
    }
    side_step_id = 10
    rows: list[dict] = []
    turns: list[dict] = []
    for index in range(MIN_SIDE_CHAIN_LENGTH):
        turns.append(user_turn if index % 2 == 0 else assistant_turn)
        step_id = index + 1 if index >= side_step_id else index
        row_id = "row-tail" if index == MIN_SIDE_CHAIN_LENGTH - 1 else f"row-{index + 1}"
        rows.append(
            _row(
                row_id,
                step_id,
                list(turns),
                **(
                    {"is_session_completed": True, "reward": 0.7}
                    if index == MIN_SIDE_CHAIN_LENGTH - 1
                    else {}
                ),
            )
        )
        if index + 1 == side_step_id:
            rows.append(
                _row("row-side", side_step_id, [{"role": "system", "content": "chat"}])
            )
    return rows
