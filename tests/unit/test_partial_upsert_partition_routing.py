"""Partial upserts route per HASH bucket (dldb >= 1.1.11 fix).

Landing hashes sub_job_id and serving hashes job_id. Each dldb upsert call
covers exactly one physical bucket and only that bucket's rows.
"""

import wt_sdk.client as client_module
from dldb.utils import stable_hash
from wt_sdk import WTGatewayClient
from wt_sdk.config import GatewayConfig, TableConfig
from wt_sdk.models import LandingRecord, ServingRecord
from wt_sdk.sharding import landing_bucket_for, plan_shards


class RecordingSession:
    """Minimal dldb session double recording upsert routing decisions."""

    def __init__(self):
        self.upsert_calls = []

    def upsert(self, table_name, columns=None, datas=None, partition=None, **kwargs):
        self.upsert_calls.append(
            {
                "table_name": table_name,
                "columns": list(columns or []),
                "datas": datas,
                "partition": partition,
                "kwargs": kwargs,
            }
        )


def _client_with_recorder(monkeypatch):
    session = RecordingSession()
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: session)
    client = WTGatewayClient(
        GatewayConfig(
            tables=TableConfig(
                landing_table="landing_test",
                serving_table="serving_test",
            )
        )
    )
    return client, session


def _record(job_id, record_id, *, sub_job_id=None):
    kwargs = {}
    if sub_job_id is not None:
        kwargs["sub_job_id"] = sub_job_id
    return LandingRecord(
        dataset_type="RL",
        id=record_id,
        created_at=100,
        job_id=job_id,
        is_terminal=True,
        step_reward=0.5,
        **kwargs,
    )


def _serving_record(job_id, record_id):
    return ServingRecord(
        dataset_type="RL",
        id=record_id,
        created_at=100,
        job_id=job_id,
        sub_job_id=f"{job_id}#ignored-for-serving-routing",
        analysis='{"status":"complete"}',
    )


def _serving_jobs_in_distinct_buckets():
    first = "serving-route-job-0"
    first_bucket = stable_hash(first) % 128
    second = next(
        f"serving-route-job-{index}"
        for index in range(1, 130)
        if stable_hash(f"serving-route-job-{index}") % 128 != first_bucket
    )
    return first, second


def test_partial_landing_upsert_splits_by_hash_bucket(monkeypatch):
    client, session = _client_with_recorder(monkeypatch)
    job_id = "route-job"
    shard_a, shard_b = plan_shards(job_id, 2)
    expected = {landing_bucket_for(shard_a), landing_bucket_for(shard_b)}
    assert len(expected) == 2  # plan_shards guarantees distinct buckets

    client.upsert_landing_batch(
        [
            _record(job_id, "landing-1", sub_job_id=shard_a),
            _record(job_id, "landing-2", sub_job_id=shard_b),
        ],
        insert_missing=False,
    )

    assert len(session.upsert_calls) == 2
    routed = {}
    for call in session.upsert_calls:
        assert call["table_name"] == "landing_test"
        assert call["kwargs"] == {"insert_missing": False}
        assert isinstance(call["partition"], int)
        # One call covers exactly one bucket and its own rows only.
        sub_job_ids = list(call["datas"]["sub_job_id"])
        assert len(sub_job_ids) == 1
        assert landing_bucket_for(sub_job_ids[0]) == call["partition"]
        routed[call["partition"]] = sub_job_ids[0]
    assert set(routed) == expected
    assert set(routed.values()) == {shard_a, shard_b}


def test_partial_landing_upsert_same_bucket_is_one_call(monkeypatch):
    client, session = _client_with_recorder(monkeypatch)
    # sub_job_id never passed: the model backfills it to job_id, and the
    # partial frame still hash-routes.
    client.upsert_landing_batch(
        [
            _record("single-bucket-job", "landing-1"),
            _record("single-bucket-job", "landing-2"),
        ],
        insert_missing=False,
    )

    assert len(session.upsert_calls) == 1
    call = session.upsert_calls[0]
    assert call["partition"] == landing_bucket_for("single-bucket-job")
    assert call["kwargs"] == {"insert_missing": False}
    assert "sub_job_id" in call["datas"].columns
    assert len(call["datas"]) == 2


def test_full_landing_upsert_stays_unpartitioned(monkeypatch):
    client, session = _client_with_recorder(monkeypatch)
    job_id = "route-job"
    shard_a, shard_b = plan_shards(job_id, 2)

    client.upsert_landing_batch(
        [
            _record(job_id, "landing-1", sub_job_id=shard_a),
            _record(job_id, "landing-2", sub_job_id=shard_b),
        ],
    )

    # Legacy contract: one plain call, no partition=, no insert_missing flag.
    assert len(session.upsert_calls) == 1
    call = session.upsert_calls[0]
    assert call["partition"] is None
    assert call["kwargs"] == {}
    assert len(call["datas"]) == 2


def test_partial_serving_upsert_splits_by_job_id_hash_bucket(monkeypatch):
    client, session = _client_with_recorder(monkeypatch)
    fallback_keys = []

    def serving_metadata(table_name, fallback_key):
        fallback_keys.append(fallback_key)
        assert table_name == "serving_test"
        return {
            "partition_column": "job_id",
            "partition_type": "HASH",
            "partitions": 128,
        }

    monkeypatch.setattr(client, "_get_partition_metadata_for_table", serving_metadata)
    job_a, job_b = _serving_jobs_in_distinct_buckets()

    client.upsert_serving_batch(
        [
            _serving_record(job_a, "serving-1"),
            _serving_record(job_b, "serving-2"),
        ],
        insert_missing=False,
    )

    assert fallback_keys == ["job_id"]
    assert len(session.upsert_calls) == 2
    routed = {}
    for call in session.upsert_calls:
        assert call["table_name"] == "serving_test"
        assert call["kwargs"] == {"insert_missing": False}
        assert isinstance(call["partition"], int)
        assert "sub_job_id" not in call["datas"].columns
        job_ids = list(call["datas"]["job_id"])
        assert len(job_ids) == 1
        assert stable_hash(job_ids[0]) % 128 == call["partition"]
        routed[call["partition"]] = job_ids[0]
    assert set(routed.values()) == {job_a, job_b}


def test_full_serving_upsert_stays_unpartitioned(monkeypatch):
    client, session = _client_with_recorder(monkeypatch)
    job_a, job_b = _serving_jobs_in_distinct_buckets()

    client.upsert_serving_batch(
        [
            _serving_record(job_a, "serving-1"),
            _serving_record(job_b, "serving-2"),
        ],
    )

    assert len(session.upsert_calls) == 1
    call = session.upsert_calls[0]
    assert call["table_name"] == "serving_test"
    assert call["partition"] is None
    assert call["kwargs"] == {}
    assert len(call["datas"]) == 2


def test_partial_upsert_falls_back_when_metadata_not_hash(monkeypatch):
    client, session = _client_with_recorder(monkeypatch)
    monkeypatch.setattr(
        client,
        "_get_partition_metadata_for_table",
        lambda table_name, fallback_key: {
            "partition_column": "dt",
            "partition_type": "VALUE",
            "partitions": None,
        },
    )
    job_id = "route-job"
    shard_a, shard_b = plan_shards(job_id, 2)

    client.upsert_landing_batch(
        [
            _record(job_id, "landing-1", sub_job_id=shard_a),
            _record(job_id, "landing-2", sub_job_id=shard_b),
        ],
        insert_missing=False,
    )

    # Non-HASH table: single unpartitioned call keeps all rows.
    assert len(session.upsert_calls) == 1
    call = session.upsert_calls[0]
    assert call["partition"] is None
    assert call["kwargs"] == {"insert_missing": False}
    assert len(call["datas"]) == 2
