"""Hermetic coverage for serving search-text aggregation."""

import json

import pytest

from wt_sdk.etl import (
    BuildSearchTextStage,
    SessionKey,
    StageContext,
    StageTransformError,
)

from wt_sdk.etl.tests.unit.test_pipeline import _row


def _context():
    return StageContext(
        pipeline_name="landing_to_serving_pipeline",
        pipeline_version="2",
        session_key=SessionKey("job-1", "session-1"),
    )


def test_search_text_joins_configured_fields_in_stable_order():
    row = _row(
        chosen_trace='[{"content":"chosen needle"}]',
        rejected_trace='[{"content":"rejected needle"}]',
        agent_model="model needle",
        meta_json='{"source":"meta needle"}',
    )

    patches = BuildSearchTextStage().transform_session((row,), _context())

    assert patches == {
        "row-1": {
            "search_text": "\n".join(
                [
                    row["chosen_trace"],
                    row["rejected_trace"],
                    row["agent_model"],
                    row["meta_json"],
                ]
            )
        }
    }


def test_search_text_skips_null_and_blank_values_without_parsing_json():
    row = _row(
        chosen_trace="not-json but still searchable",
        rejected_trace=None,
        agent_model="model",
        meta_json=None,
    )

    patches = BuildSearchTextStage().transform_session((row,), _context())

    assert patches["row-1"]["search_text"] == (
        "not-json but still searchable\nmodel"
    )


def test_search_text_skips_non_trainable_rows():
    patches = BuildSearchTextStage().transform_session(
        (_row(is_trainable=False),),
        _context(),
    )

    assert patches == {}


def test_search_text_returns_null_when_no_source_contains_text():
    row = _row(
        chosen_trace=None,
        rejected_trace=None,
        agent_model=None,
        meta_json=None,
    )

    patches = BuildSearchTextStage().transform_session((row,), _context())

    assert patches == {"row-1": {"search_text": None}}


def test_search_text_rejects_non_string_source_with_record_id():
    row = _row(meta_json={"not": "an ETL JSON string"})

    with pytest.raises(StageTransformError, match="meta_json must be a string") as exc:
        BuildSearchTextStage().transform_session((row,), _context())

    assert exc.value.record_id == "row-1"


def test_search_text_is_deterministic_and_does_not_mutate_input():
    row = _row(meta_json=json.dumps({"source": "stable"}))
    original = dict(row)
    stage = BuildSearchTextStage()

    first = stage.transform_session((row,), _context())
    second = stage.transform_session((row,), _context())

    assert first == second
    assert row == original
