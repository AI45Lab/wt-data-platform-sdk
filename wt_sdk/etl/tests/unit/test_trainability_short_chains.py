"""Short trajectories must be excluded even in single-chain sessions."""

import json

from wt_sdk.etl.stage import SessionKey, StageContext
import json

from wt_sdk.etl.stages.trainability import UpdateIsTrainableStage


def test_single_chain_length_threshold():
    context = StageContext("test", "1", SessionKey("job", "session"))
    for length in (1, 2, 3, 4):
        rows = tuple(
            {
                "id": str(index),
                "step_id": index,
                "messages": json.dumps(
                    [{"role": "user", "content": str(i)} for i in range(index + 1)]
                ),
                "is_session_completed": index == length - 1,
                "meta_json": {},
                "reward": 0.0,
            }
            for index in range(length)
        )
        patches = UpdateIsTrainableStage().transform_session(rows, context)
        assert {key for key, patch in patches.items() if patch["is_trainable"]} == (
            {str(length - 1)} if length >= 3 else set()
        )


def test_single_empty_evaluation_summary_is_excluded():
    row = {
        "id": "summary",
        "step_id": 0,
        "messages": "[]",
        "response": {"role": "assistant", "content": ""},
        "is_session_completed": True,
        "is_trainable": True,
        "meta_json": {"env_state": {"event_type": "evaluation_summary"}},
        "reward": 0.0,
    }
    context = StageContext("test", "1", SessionKey("job", "session"))
    assert UpdateIsTrainableStage().transform_session((row,), context) == {
        "summary": {"is_trainable": False}
    }
