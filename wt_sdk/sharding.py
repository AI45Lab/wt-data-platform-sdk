"""Write-shard planning for HASH(sub_job_id)-partitioned landing tables.

Landing routes every row by ``stable_hash(sub_job_id) % LANDING_PARTITIONS``.
Distinct sub_job_ids land on distinct buckets only with high probability, so
a job that simply numbers its shards ``{job_id}#shard0..n`` may collide.
``plan_shards`` checks the hash while picking shard ids, so the returned ids
are guaranteed to cover n different buckets.
"""
from dldb.utils import stable_hash

from wt_sdk.core.schemas import LANDING_PARTITIONS


def landing_bucket_for(sub_job_id: str, partitions: int = LANDING_PARTITIONS) -> int:
    """Return the landing HASH bucket a sub_job_id writes to (read-only helper)."""
    return stable_hash(sub_job_id) % partitions


def plan_shards(job_id: str, n: int, *, partitions: int = LANDING_PARTITIONS) -> list:
    """Plan n write shards for one job, guaranteed to hit n distinct buckets.

    Shard ids follow the ``{job_id}#{suffix}`` convention (see AGENTS.md job
    splitting). Candidates are numbered ``shard0, shard1, ...``; a candidate
    whose bucket is already taken is skipped, so the returned ids may have
    gaps but always map to pairwise-distinct buckets. Registering the returned
    shards in the env-config table remains the writer's responsibility, same
    as hand-picked shard ids.

    Deterministic: the same (job_id, n) always returns the same list.
    """
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")
    if not isinstance(n, int) or isinstance(n, bool) or n <= 0:
        raise ValueError("n must be a positive integer")
    if n > partitions:
        raise ValueError(
            f"cannot plan {n} distinct-bucket shards: landing has only {partitions} buckets"
        )

    shards = []
    taken_buckets = set()
    index = 0
    while len(shards) < n:
        shard = f"{job_id}#shard{index}"
        bucket = stable_hash(shard) % partitions
        if bucket not in taken_buckets:
            taken_buckets.add(bucket)
            shards.append(shard)
        index += 1
    return shards


__all__ = ["plan_shards", "landing_bucket_for"]
