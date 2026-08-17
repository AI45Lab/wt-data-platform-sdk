import json

import pytest

from wt_sdk.etl import FreeCotStage, StageTransformError
from wt_sdk.etl.stage import SessionKey, StageContext


class ReplayClient:
    def __init__(self, result: str = "decoded reasoning") -> None:
        self.result = result
        self.calls: list[tuple[str, str, int]] = []

    def extract(self, signature: str, model: str, max_tokens: int) -> str:
        self.calls.append((signature, model, max_tokens))
        return self.result


class FailingReplayClient:
    def extract(self, signature: str, model: str, max_tokens: int) -> str:
        del signature, model, max_tokens
        raise RuntimeError("service unavailable")


def _context() -> StageContext:
    return StageContext(
        pipeline_name="landing_enrichment_pipeline",
        pipeline_version="1",
        session_key=SessionKey("job-1", "session-1"),
    )


def _record(
    record_id: str,
    *,
    signature: str = "encrypted-signature",
    is_trainable: bool = True,
    is_session_completed: bool = False,
    meta_json: object = '{"source":"test"}',
) -> dict[str, object]:
    return {
        "id": record_id,
        "agent_model": "claude-opus-4-6-thinking",
        "is_trainable": is_trainable,
        "is_session_completed": is_session_completed,
        "messages": json.dumps(
            [{"role": "assistant", "encrypted_content": signature}]
        ),
        "meta_json": meta_json,
        "session_id": "session-1",
        "step_id": 0,
    }


def test_freecot_decodes_unique_signatures_only_for_trainable_completed_sessions():
    client = ReplayClient()
    stage = FreeCotStage(replay_client=client)
    session = (
        _record("trainable-1"),
        _record("trainable-2", is_session_completed=True),
        _record("not-trainable", is_trainable=False, is_session_completed=False),
    )

    patches = stage.transform_session(session, _context())

    assert len(client.calls) == 1
    assert set(patches) == {"trainable-1", "trainable-2"}
    for patch in patches.values():
        assert json.loads(patch["messages"])[0]["reasoning_content"] == "decoded reasoning"
        assert json.loads(patch["meta_json"]) == {"source": "test"}


def test_freecot_skips_incomplete_sessions_without_calling_replay_service():
    client = ReplayClient()

    patches = FreeCotStage(replay_client=client).transform_session(
        (_record("trainable"),), _context()
    )

    assert patches == {}
    assert client.calls == []


def test_freecot_extracts_reasoning_without_anthropic_wrapper_tokens():
    stage = FreeCotStage(
        replay_client=ReplayClient(
            "<antThinking>\noriginal reasoning chain\n</antThinking>\n"
            "<!--cot_tokens:1842-->"
        )
    )

    patches = stage.transform_session(
        (_record("trainable", is_session_completed=True),), _context()
    )

    message = json.loads(patches["trainable"]["messages"])[0]
    assert message["reasoning_content"] == "original reasoning chain"


def test_freecot_preserves_metadata_by_rejecting_invalid_json_object():
    stage = FreeCotStage(replay_client=ReplayClient())

    with pytest.raises(StageTransformError, match="meta_json must be a JSON object"):
        stage.transform_session(
            (_record("trainable", is_session_completed=True, meta_json="[]"),),
            _context(),
        )


def test_freecot_replay_failure_fails_the_session():
    stage = FreeCotStage(replay_client=FailingReplayClient())

    with pytest.raises(StageTransformError, match="FreeCoT replay failed") as exc:
        stage.transform_session(
            (_record("trainable", is_session_completed=True),), _context()
        )

    assert exc.value.record_id == "trainable"
