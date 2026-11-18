"""Regressions for terminal response markers conflicting with empty telemetry."""

import json
import unittest
from copy import deepcopy
from types import MappingProxyType

from wt_sdk.etl.pipelines.landing_enrichment_pipeline import build_pipeline
from wt_sdk.etl.stages.trainability import (
    _is_eligible_trainability_record,
    _is_incomplete_stream_request,
)
from wt_sdk.etl.tools.trainability_sample import process_session


def _stream_row(normalized_response, **metadata):
    return {
        "response": normalized_response,
        "meta_json": {
            "request": {"stream": True},
            "env_state": {"status_code": 200, "finish_reason": None},
            **metadata,
        },
    }


class TestStreamCompletion(unittest.TestCase):
    def test_response_marker_overrides_null_telemetry(self):
        for key in ("finish_reason", "finishReason", "stop_reason", "stopReason"):
            for encoded in (False, True):
                with self.subTest(key=key, encoded=encoded):
                    row = _stream_row({key: "tool_use"})
                    if encoded:
                        row = {key: json.dumps(value) for key, value in row.items()}
                    self.assertTrue(_is_eligible_trainability_record(row))

    def test_raw_anthropic_response_is_used_when_normalized_marker_is_absent(self):
        row = _stream_row(
            {"role": "assistant", "content": "answer"},
            response=json.dumps({"stop_reason": "end_turn"}),
        )
        self.assertTrue(_is_eligible_trainability_record(row))

    def test_missing_or_empty_terminal_marker_remains_incomplete(self):
        responses = [{}, None] + [
            {"finish_reason": value} for value in (None, "", "null", " NULL ", "  ")
        ]
        for response in responses:
            with self.subTest(response=response):
                self.assertTrue(_is_incomplete_stream_request(_stream_row(response)))

    def test_explicitly_incomplete_response_is_not_rescued_by_metadata(self):
        row = _stream_row(
            {"finish_reason": None},
            env_state={"status_code": 200, "finish_reason": "tool_use"},
            response={"stop_reason": "end_turn"},
        )
        self.assertTrue(_is_incomplete_stream_request(row))

    def test_terminal_chunk_overrides_earlier_empty_chunks(self):
        row = _stream_row({
            "chunks": [
                json.dumps({"choices": [{"finish_reason": None}]}),
                {"choices": [{"finish_reason": "stop"}]},
            ],
        })
        self.assertFalse(_is_incomplete_stream_request(row))
        row["response"] = MappingProxyType({
            "choices": (MappingProxyType({"finish_reason": "stop"}),),
        })
        self.assertFalse(_is_incomplete_stream_request(row))

    def test_legacy_telemetry_only_completion_is_supported(self):
        row = _stream_row(None, telemetry=json.dumps({"finishReason": "stop"}))
        self.assertFalse(_is_incomplete_stream_request(row))

    def test_non_streaming_request_does_not_require_a_terminal_marker(self):
        row = _stream_row(None, request={"stream": False})
        self.assertTrue(_is_eligible_trainability_record(row))

    def test_non_200_response_is_still_excluded_with_a_terminal_marker(self):
        for status in (502, 504):
            with self.subTest(status=status):
                row = _stream_row(
                    {"finish_reason": "tool_use"},
                    env_state={"status_code": status},
                )
                self.assertFalse(_is_eligible_trainability_record(row))

    def test_projected_landing_session_selects_step_21(self):
        # Minimal reproduction of the supplied 23-row session. Streaming 200s
        # have null telemetry, while non-streaming 200s are steps 16 and 20.
        error_steps = {15: 502, 17: 502, 18: 502, 19: 504, 22: 502, 24: 504}
        rows = []
        for step in [*range(1, 23), 24]:
            status = error_steps.get(step, 200)
            stream = step not in (16, 19, 20, 24)
            messages = (
                [{"role": "system", "content": "Generate a session title"}]
                if step == 1 else
                [{"role": "user", "content": str(i)} for i in range(step)]
            )
            rows.append({
                "id": f"row-{step}",
                "job_id": "job",
                "session_id": "session",
                "step_id": step,
                "env_id": "env",
                "source_updated_at": 1_800_000_000_000,
                "agent_model": "glm-5.3",
                "messages": json.dumps(messages),
                "response": json.dumps({
                    "finish_reason": "tool_use" if status == 200 else None,
                }),
                "meta_json": json.dumps({
                    "request": {"stream": stream},
                    "env_state": {
                        "status_code": status,
                        "finish_reason": "tool_use" if not stream and status == 200 else None,
                    },
                }),
                "reward": 0.0,
                "is_trainable": False,
                "is_session_completed": step == 22,
            })

        pipeline = build_pipeline()
        self.assertIn("response", pipeline.source_columns)
        projected = [{key: row[key] for key in pipeline.source_columns} for row in rows]
        original = deepcopy(projected)
        result = pipeline.process_session(projected)
        self.assertEqual(projected, original)
        self.assertEqual(result.failures, ())
        self.assertEqual(
            {patch.record_id: patch.updates for patch in result.landing_patches},
            {"row-21": {"is_trainable": True}},
        )
        self.assertEqual(
            [warning.warning_type for warning in result.warnings],
            ["CompletionMarkerBeforeMaxStep"],
        )
        report = process_session(projected, "job", "session", include_rows=False)
        self.assertEqual(report["eligible_row_count"], 17)
        self.assertEqual(report["trainable_step_ids"], [21])
        self.assertEqual(report["reason_counts"], {
            "short_side_chain": 1,
            "superseded_in_chain": 15,
            "non_200_status": 6,
            "selected_chain_tail": 1,
        })
        patches = {patch.record_id: patch.updates for patch in result.landing_patches}
        updated = [{**row, **patches.get(row["id"], {})} for row in projected]
        repeated = pipeline.process_session(updated)
        self.assertEqual(repeated.failures, ())
        self.assertEqual(repeated.landing_patches, ())
