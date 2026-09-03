import json
import time
import uuid

from wt_sdk import GatewayConfig, LandingRecord, TableConfig, WTGatewayClient


TEST_TABLE_CONFIG = GatewayConfig(tables=TableConfig(landing_table="v2_landing_test"))


def _message(text: str) -> dict:
    return {"role": "user", "content": text}


def _response(text: str) -> dict:
    return {"role": "assistant", "content": text}


def _make_gateway_record(job_id: str, session_id: str, step_id: int, trainable: bool) -> LandingRecord:
    now = int(time.time())
    return LandingRecord(
        dataset_type="GATEWAY_TEST",
        id=f"gateway_{now}_{uuid.uuid4().hex}_{step_id:03d}",
        session_id=session_id,
        created_at=now + step_id,
        step_id=step_id,
        is_terminal=False,
        env_id=session_id,
        job_id=job_id,
        is_truncated=False,
        step_reward=float(step_id),
        reward=float(step_id),
        messages=json.dumps([_message(f"step {step_id}")]),
        response=json.dumps(_response(f"response {step_id}")),
        agent_model="test-model",
        env_name="test-env",
        is_session_completed=False,
        is_trainable=trainable,
        meta_json=json.dumps(
            {
                "group_id": "group-a",
                "env_state": {"weight_version": "1.0.0"},
            }
        ),
    )


def test_gateway_query_and_update_landing_interfaces():
    job_id = f"gateway_job_{uuid.uuid4().hex}"
    session_id = f"gateway_session_{uuid.uuid4().hex}"
    records = [
        _make_gateway_record(job_id, session_id, 1, trainable=True),
        _make_gateway_record(job_id, session_id, 2, trainable=False),
        _make_gateway_record(job_id, session_id, 3, trainable=True),
    ]

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        try:
            client.ingest_landing_batch(records)
            time.sleep(1)

            full_traj = client.query_data(
                filter_query=f"job_id = '{job_id}' AND session_id = '{session_id}'",
                order_by="step_id",
                ascending=True,
                checkout_latest=True,
            )
            assert [record["step_id"] for record in full_traj] == [1, 2, 3]

            latest = client.query_data(
                filter_query=f"job_id = '{job_id}' AND session_id = '{session_id}'",
                order_by="step_id",
                ascending=False,
                limit=1,
                checkout_latest=True,
            )
            assert len(latest) == 1
            assert latest[0]["step_id"] == 3

            update_result = client.update_landing(
                filter_query=(
                    f"job_id = '{job_id}' AND session_id = '{session_id}' AND step_id = 2"
                ),
                updates={
                    "is_terminal": True,
                    "is_session_completed": True,
                    "is_trainable": True,
                    "meta_json": json.dumps(
                        {
                            "group_id": "group-a",
                            "env_state": {"weight_version": "1.0.1"},
                        }
                    ),
                },
            )
            assert update_result["updated"] is True
            assert update_result["partition"] is not None
            assert update_result["updated_fields"] == [
                "is_session_completed",
                "is_terminal",
                "is_trainable",
                "meta_json",
            ]
            assert "source_updated_at" in update_result["effective_updated_fields"]
            assert update_result["source_updated_at_touched"] is True

            updated = client.query_data(
                filter_query=(
                    f"job_id = '{job_id}' AND session_id = '{session_id}' AND step_id = 2"
                ),
                limit=1,
                checkout_latest=True,
            )
            assert len(updated) == 1
            assert updated[0]["is_terminal"] is True
            assert updated[0]["is_session_completed"] is True
            assert updated[0]["is_trainable"] is True
            assert updated[0]["source_updated_at"] >= records[1].source_updated_at
            assert json.loads(updated[0]["meta_json"])["env_state"]["weight_version"] == "1.0.1"

            trainable_rows = client.query_data(
                filter_query=f"job_id = '{job_id}' AND is_trainable = true",
                columns=["id", "session_id", "step_id", "is_trainable"],
                order_by="step_id",
                ascending=True,
                checkout_latest=True,
            )
            assert [row["step_id"] for row in trainable_rows] == [1, 2, 3]

        finally:
            client.delete_landing(f"job_id = '{job_id}' AND session_id = '{session_id}'")
