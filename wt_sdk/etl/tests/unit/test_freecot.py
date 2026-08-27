import json

import pytest

from wt_sdk.etl import FreeCotStage, StageTransformError
from wt_sdk.etl.stage import SessionKey, StageContext


class ReplayClient:
    def __init__(self, result: str = "decoded reasoning") -> None:
        self.result = result
        self.calls: list[tuple[object, str, int, str]] = []

    def extract(
        self,
        signature: str | list,
        model: str,
        max_tokens: int,
        *,
        family: str,
    ) -> str:
        self.calls.append((signature, model, max_tokens, family))
        return self.result


class FailingReplayClient:
    def extract(
        self,
        signature: str | list,
        model: str,
        max_tokens: int,
        *,
        family: str,
    ) -> str:
        del signature, model, max_tokens, family
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


def _gpt_record(
    record_id: str,
    *,
    signature: str = "gpt-encrypted-signature",
    is_trainable: bool = True,
    is_session_completed: bool = False,
    meta_json: object = '{"source":"test"}',
) -> dict[str, object]:
    return {
        "id": record_id,
        "agent_model": "gpt-5-high",
        "is_trainable": is_trainable,
        "is_session_completed": is_session_completed,
        "messages": json.dumps(
            [
                {
                    "type": "reasoning",
                    "encrypted_content": signature,
                    "id": "rs_test",
                    "summary": [],
                }
            ]
        ),
        "meta_json": meta_json,
        "session_id": "session-1",
        "step_id": 0,
    }


def test_freecot_backfills_decoded_signatures_into_previous_blocks():
    client = ReplayClient()
    stage = FreeCotStage(replay_client=client)
    session = (
        _record(
            "prev-block",
            is_trainable=False,
            is_session_completed=False,
            signature="shared-sig",
        ),
        _record("trainable", is_session_completed=True, signature="shared-sig"),
    )

    patches = stage.transform_session(session, _context())

    assert len(client.calls) == 1
    assert set(patches) == {"prev-block", "trainable"}
    for patch in patches.values():
        assert json.loads(patch["messages"])[0]["reasoning_content"] == "decoded reasoning"
        assert set(patch) == {"messages"}


def test_freecot_decodes_signatures_from_all_blocks_in_trainable_session():
    client = ReplayClient()
    stage = FreeCotStage(replay_client=client)
    session = (
        _record(
            "prev-block",
            is_trainable=False,
            is_session_completed=False,
            signature="only-in-prev",
        ),
        _record("trainable", is_session_completed=True, signature="trainable-sig"),
    )

    patches = stage.transform_session(session, _context())

    assert sorted(signature for signature, _, _, _ in client.calls) == [
        "only-in-prev",
        "trainable-sig",
    ]
    assert set(patches) == {"prev-block", "trainable"}
    for patch in patches.values():
        assert json.loads(patch["messages"])[0]["reasoning_content"] == "decoded reasoning"
        assert set(patch) == {"messages"}


def test_freecot_skips_sessions_without_trainable_records():
    client = ReplayClient()

    patches = FreeCotStage(replay_client=client).transform_session(
        (
            _record("not-trainable-1", is_trainable=False, is_session_completed=True),
            _record(
                "not-trainable-2",
                is_trainable=False,
                is_session_completed=True,
                signature="sig-2",
            ),
        ),
        _context(),
    )

    assert patches == {}
    assert client.calls == []


def test_freecot_skips_incomplete_sessions_without_calling_replay_service():
    client = ReplayClient()

    patches = FreeCotStage(replay_client=client).transform_session(
        (_record("trainable"),), _context()
    )

    assert patches == {}
    assert client.calls == []


def test_freecot_skips_messages_with_existing_reasoning_content():
    client = ReplayClient()
    already_decoded = _record("already-decoded", signature="already-decoded")
    already_decoded["messages"] = json.dumps(
        [
            {
                "role": "assistant",
                "encrypted_content": "already-decoded",
                "reasoning_content": "existing reasoning",
            }
        ]
    )

    patches = FreeCotStage(replay_client=client).transform_session(
        (
            already_decoded,
            _record("needs-decoding", signature="decode-me", is_session_completed=True),
        ),
        _context(),
    )

    assert client.calls == [("decode-me", "claude-opus-4-6", 128000, "claude")]
    assert set(patches) == {"needs-decoding"}


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


def test_freecot_does_not_read_or_patch_metadata():
    stage = FreeCotStage(replay_client=ReplayClient())

    patches = stage.transform_session(
        (_record("trainable", is_session_completed=True, meta_json="[]"),),
        _context(),
    )

    assert set(patches["trainable"]) == {"messages"}


def test_freecot_replay_failure_fails_the_session():
    stage = FreeCotStage(replay_client=FailingReplayClient())

    with pytest.raises(StageTransformError, match="FreeCoT replay failed") as exc:
        stage.transform_session(
            (_record("trainable", is_session_completed=True),), _context()
        )

    assert exc.value.record_id == "trainable"


def test_freecot_decodes_gpt_reasoning_items():
    client = ReplayClient()
    stage = FreeCotStage(replay_client=client)
    session = (
        _gpt_record(
            "prev-block",
            is_trainable=False,
            is_session_completed=False,
            signature="shared-gpt-sig",
        ),
        _gpt_record("trainable", is_session_completed=True, signature="shared-gpt-sig"),
    )

    patches = stage.transform_session(session, _context())

    assert len(client.calls) == 1
    signature_arg, model_arg, _, family_arg = client.calls[0]
    assert family_arg == "gpt"
    assert model_arg == "gpt-5-high"
    assert isinstance(signature_arg, list)
    assert signature_arg[0]["type"] == "reasoning"
    assert signature_arg[0]["encrypted_content"] == "shared-gpt-sig"
    assert set(patches) == {"prev-block", "trainable"}
    for patch in patches.values():
        message = json.loads(patch["messages"])[0]
        assert message["reasoning_content"] == "decoded reasoning"
        assert set(patch) == {"messages"}


def test_freecot_skips_unsupported_models():
    client = ReplayClient()
    record = _record("trainable", is_session_completed=True)
    record["agent_model"] = "llama-3-70b"

    patches = FreeCotStage(replay_client=client).transform_session(
        (record,), _context()
    )

    assert patches == {}
    assert client.calls == []


def test_freecot_handles_mixed_claude_and_gpt_session():
    client = ReplayClient()
    stage = FreeCotStage(replay_client=client)
    session = (
        _record("claude-block", is_session_completed=True, signature="claude-sig"),
        _gpt_record("gpt-block", is_session_completed=True, signature="gpt-sig"),
    )

    patches = stage.transform_session(session, _context())

    assert len(client.calls) == 2
    families = {family for _, _, _, family in client.calls}
    assert families == {"claude", "gpt"}
    assert set(patches) == {"claude-block", "gpt-block"}
    assert json.loads(patches["claude-block"]["messages"])[0]["reasoning_content"]
    assert json.loads(patches["gpt-block"]["messages"])[0]["reasoning_content"]
