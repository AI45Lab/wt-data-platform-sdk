import json

import pytest

from scripts.dev.upload_trainability_mock import (
    SOURCE_SESSION_IDS,
    _target_scope,
    _verify_persisted,
    build_records,
)


def _payload() -> dict[str, object]:
    rows = []
    for case_index, session_id in enumerate(SOURCE_SESSION_IDS, start=1):
        for step_id in (1, 2):
            rows.append(
                {
                    "dataset_type": "RL",
                    "id": f"source-{case_index}-{step_id}",
                    "session_id": session_id,
                    "created_at": 1_786_005_100 + case_index * 10 + step_id,
                    "source_updated_at": 123,
                    "step_id": step_id,
                    "messages": [{"role": "user", "content": str(step_id)}],
                    "response": {"role": "assistant", "content": str(step_id)},
                    "is_session_completed": step_id == 1,
                    "is_trainable": step_id == 2,
                    "meta_json": {"case": case_index},
                }
            )
    return {"total_rows": len(rows), "rows": rows}


def test_build_records_rewrites_identity_and_completion_marker():
    records, session_ids = build_records(_payload())

    assert session_ids == tuple(f"mock_xq_case_{index:02d}" for index in range(1, 6))
    assert len(records) == 10
    for session_id in session_ids:
        session = [record for record in records if record.session_id == session_id]
        assert [record.step_id for record in session] == [1, 2]
        assert [record.is_session_completed for record in session] == [False, True]
        assert [record.is_trainable for record in session] == [False, False]
        assert all(record.job_id == "gateway_mock_for_xquer" for record in session)
        assert all(record.env_id == session_id for record in session)
        assert all(record.source_updated_at != 123 for record in session)
        assert json.loads(session[0].messages)[0]["content"] == "1"
        assert json.loads(session[0].meta_json)["case"] in range(1, 6)


def test_build_records_rejects_missing_case():
    payload = _payload()
    payload["rows"] = [
        row for row in payload["rows"] if row["session_id"] != SOURCE_SESSION_IDS[-1]
    ]
    payload["total_rows"] = len(payload["rows"])

    with pytest.raises(ValueError, match="exactly the five expected"):
        build_records(payload)


def test_scope_quotes_values_and_verification_checks_final_row():
    assert _target_scope("job'x", ("session'a",)) == (
        "job_id = 'job''x' AND session_id IN ('session''a')"
    )
    records, session_ids = build_records(_payload())
    persisted = [
        {
            "id": record.id,
            "session_id": record.session_id,
            "step_id": record.step_id,
            "is_session_completed": record.is_session_completed,
        }
        for record in records
    ]
    _verify_persisted(persisted, records, session_ids)
    persisted[0]["is_session_completed"] = True

    with pytest.raises(RuntimeError, match="completed steps"):
        _verify_persisted(persisted, records, session_ids)
