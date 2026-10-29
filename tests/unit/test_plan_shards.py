"""Unit tests for wt_sdk.sharding plan_shards / landing_bucket_for."""

import pytest
from dldb.utils import stable_hash

from wt_sdk import landing_bucket_for, plan_shards
from wt_sdk.core.schemas import LANDING_PARTITIONS
from wt_sdk.sharding import landing_bucket_for as module_landing_bucket_for


def test_plan_shards_returns_n_prefixed_distinct_shards():
    job_id = "dataset#harness#model#task#20261029#codex"
    shards = plan_shards(job_id, 8)

    assert len(shards) == 8
    assert len(set(shards)) == 8
    assert all(shard.startswith(job_id + "#") for shard in shards)


def test_plan_shards_buckets_are_pairwise_distinct():
    shards = plan_shards("some-job", 16)

    buckets = [stable_hash(shard) % LANDING_PARTITIONS for shard in shards]
    assert len(set(buckets)) == 16


def test_plan_shards_is_deterministic():
    assert plan_shards("job-a", 5) == plan_shards("job-a", 5)


def test_plan_shards_single_shard_is_first_candidate():
    assert plan_shards("job-b", 1) == ["job-b#shard0"]


def test_plan_shards_skips_colliding_candidates(monkeypatch):
    # Force the first candidate to collide with shard0, then diverge: the
    # greedy planner must skip shard1 and still fill the request.
    real_stable_hash = stable_hash

    def fake_stable_hash(value):
        if value == "job-c#shard1":
            return real_stable_hash("job-c#shard0")
        return real_stable_hash(value)

    monkeypatch.setattr("wt_sdk.sharding.stable_hash", fake_stable_hash)

    shards = plan_shards("job-c", 2)

    assert "job-c#shard1" not in shards
    assert shards[0] == "job-c#shard0"
    assert shards[1].startswith("job-c#shard")


def test_plan_shards_rejects_invalid_arguments():
    with pytest.raises(ValueError, match="non-empty"):
        plan_shards("", 2)
    with pytest.raises(ValueError, match="positive"):
        plan_shards("job-d", 0)
    with pytest.raises(ValueError, match="positive"):
        plan_shards("job-d", -3)
    with pytest.raises(ValueError, match="distinct-bucket"):
        plan_shards("job-d", LANDING_PARTITIONS + 1)


def test_plan_shards_respects_custom_partition_count():
    # With 2 buckets at most 2 distinct-bucket shards can be planned.
    shards = plan_shards("job-e", 2, partitions=2)
    buckets = {stable_hash(shard) % 2 for shard in shards}
    assert len(shards) == 2
    assert len(buckets) == 2
    with pytest.raises(ValueError, match="distinct-bucket"):
        plan_shards("job-e", 3, partitions=2)


def test_landing_bucket_for_matches_dldb_hash():
    sub_job_id = "job-f#shard0"
    assert landing_bucket_for(sub_job_id) == stable_hash(sub_job_id) % LANDING_PARTITIONS
    assert module_landing_bucket_for(sub_job_id, partitions=16) == stable_hash(sub_job_id) % 16
