from types import SimpleNamespace

import pytest

from wt_sdk.trajectory_stats.schemas import (
    CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS,
    CLASSIFICATION_STATUS_VALID,
)
from wt_sdk.trajectory_stats.service import (
    TrajectoryStatsService,
    TrajectoryStatsUpdate,
)
from wt_sdk.trajectory_stats.session_metrics import compute_session_metrics


JOB_ID = "cybergym#claudecode#glm-5.3#挖掘#level1"
UPDATED_AT_MS = 1_700_000_000_000


def landing_row(job_id, session_id, step_id, tokens, *, sub_job_id=None):
    return {
        "id": f"{session_id}-{step_id}",
        "job_id": job_id,
        "session_id": session_id,
        "sub_job_id": sub_job_id or job_id,
        "step_id": step_id,
        "meta_json": {"prompt_tokens": tokens, "completion_tokens": 0},
        "response": {"role": "assistant", "content": []},
        "messages": [{"role": "user", "content": "request"}],
    }


def stored_session(job_id, session_id, tokens):
    return compute_session_metrics(
        job_id,
        session_id,
        [landing_row(job_id, session_id, 1, tokens)],
        updated_at_ms=UPDATED_AT_MS,
    )


class FakeClient:
    def __init__(self, *, landing_rows=(), serving_rows=(), shards=None, profile="production"):
        self.config = SimpleNamespace(
            tables=SimpleNamespace(
                profile=profile,
                landing_table="wt_landing",
                serving_table="wt_serving",
            )
        )
        self.landing_rows = list(landing_rows)
        self.serving_rows = list(serving_rows)
        self.shards = shards or {}
        self.queries = []
        self.resolved_jobs = []

    def resolve_sub_jobs(self, job_id):
        self.resolved_jobs.append(job_id)
        return self.shards.get(job_id, [job_id])

    def query_data(self, *, filter_query, columns, checkout_latest, table):
        self.queries.append(
            {
                "filter_query": filter_query,
                "columns": columns,
                "checkout_latest": checkout_latest,
                "table": table,
            }
        )
        if table == "wt_serving":
            return list(self.serving_rows)
        return list(self.landing_rows)


class FakeStore:
    def __init__(self, *, session_rows=(), category_rows=()):
        self.session_rows = list(session_rows)
        self.category_rows = list(category_rows)
        self.session_writes = []
        self.category_writes = []
        self.category_deletes = []

    def list_session_rows(self):
        return list(self.session_rows)

    def list_category_rows(self):
        return list(self.category_rows)

    def upsert_session_rows(self, rows):
        materialized = list(rows)
        self.session_writes.append(materialized)
        return len(materialized)

    def upsert_category_rows(self, rows):
        materialized = list(rows)
        self.category_writes.append(materialized)
        return len(materialized)

    def delete_category_rows(self, keys):
        materialized = list(keys)
        self.category_deletes.append(materialized)
        return len(materialized)


def test_update_sessions_combines_split_shards_and_recomputes_whole_category():
    shards = [f"{JOB_ID}#a", f"{JOB_ID}#b"]
    client = FakeClient(
        landing_rows=[
            landing_row(JOB_ID, "s1", 1, 10, sub_job_id=shards[0]),
            landing_row(JOB_ID, "s2", 2, 20, sub_job_id=shards[1]),
        ],
        shards={JOB_ID: shards},
    )
    unrelated = stored_session(JOB_ID, "existing", 30)
    store = FakeStore(session_rows=[unrelated])
    service = TrajectoryStatsService(client, store)

    result = service.update_sessions(
        [(JOB_ID, "s2"), (JOB_ID, "s1"), (JOB_ID, "s1")],
        updated_at_ms=UPDATED_AT_MS,
    )

    assert result == TrajectoryStatsUpdate(
        sessions_updated=2,
        categories_upserted=1,
        categories_deleted=0,
    )
    assert client.resolved_jobs == [JOB_ID]
    assert len(client.queries) == 1
    query = client.queries[0]
    assert f"job_id = '{JOB_ID}'" in query["filter_query"]
    assert "session_id IN ('s1', 's2')" in query["filter_query"]
    assert "sub_job_id" not in query["filter_query"]
    assert query["checkout_latest"] is True
    assert [row["total_tokens"] for row in store.session_writes[0]] == [10, 20]
    assert len(store.category_writes) == 1
    category = store.category_writes[0][0]
    assert category["delivered_session_count"] == 3
    assert category["total_tokens_mean"] == 20.0
    assert category["total_tokens_median"] == 20.0
    assert category["total_steps_max"] == 2


def test_update_sessions_does_not_write_partial_results_when_shard_is_unknown():
    client = FakeClient(
        landing_rows=[landing_row(JOB_ID, "s1", 1, 10, sub_job_id="unknown")],
        shards={JOB_ID: [f"{JOB_ID}#a", f"{JOB_ID}#b"]},
    )
    store = FakeStore()
    service = TrajectoryStatsService(client, store)

    with pytest.raises(ValueError, match="unknown sub_job_id"):
        service.update_sessions([(JOB_ID, "s1")])

    assert store.session_writes == []
    assert store.category_writes == []


def test_update_sessions_propagates_landing_read_failure_before_writes():
    class FailingClient(FakeClient):
        def query_data(self, **kwargs):
            raise RuntimeError("landing unavailable")

    store = FakeStore()
    service = TrajectoryStatsService(FailingClient(), store)

    with pytest.raises(RuntimeError, match="landing unavailable"):
        service.update_sessions([(JOB_ID, "s1")])

    assert store.session_writes == []
    assert store.category_writes == []


def test_invalid_reclassification_removes_only_its_old_empty_category():
    old = stored_session(JOB_ID, "s1", 10)
    client = FakeClient(
        landing_rows=[landing_row("invalid-job", "s1", 1, 10)]
    )
    store = FakeStore(session_rows=[old])
    # Use an invalid parent identity for both old and new rows to exercise the
    # category-removal path without depending on a second job's category.
    old_invalid = dict(old, job_id="invalid-job")
    store.session_rows = [old_invalid]
    service = TrajectoryStatsService(client, store)

    result = service.update_sessions(
        [("invalid-job", "s1")],
        updated_at_ms=UPDATED_AT_MS,
    )

    assert result.categories_upserted == 0
    assert result.categories_deleted == 1
    assert store.category_writes == []
    assert store.category_deletes == [
        [("cybergym", "claudecode", "glm-5.3", "挖掘", "level1")]
    ]


def test_discovery_uses_serving_only_for_distinct_parent_session_identities():
    client = FakeClient(
        serving_rows=[
            {"job_id": JOB_ID, "session_id": "s2"},
            {"job_id": JOB_ID, "session_id": "s1"},
            {"job_id": JOB_ID, "session_id": "s1"},
            {"job_id": JOB_ID, "session_id": None},
        ]
    )
    service = TrajectoryStatsService(client, FakeStore())

    keys = service.discover_delivered_sessions([JOB_ID])

    assert keys == [(JOB_ID, "s1"), (JOB_ID, "s2")]
    assert client.resolved_jobs == []
    assert len(client.queries) == 1
    assert client.queries[0]["table"] == "wt_serving"
    assert client.queries[0]["columns"] == ["job_id", "session_id"]


def test_rebuild_all_categories_includes_retained_rows_and_deletes_stale():
    retained = stored_session(JOB_ID, "legacy", 10)
    stale = {
        "dataset": "old",
        "harness": "old",
        "model": "old",
        "task_type": "old",
        "level": "",
    }
    store = FakeStore(session_rows=[retained], category_rows=[stale])
    service = TrajectoryStatsService(FakeClient(), store)

    result = service.rebuild_all_categories(updated_at_ms=UPDATED_AT_MS)

    assert result == TrajectoryStatsUpdate(
        categories_upserted=1,
        categories_deleted=1,
    )
    category = store.category_writes[0][0]
    assert category["delivered_session_count"] == 1
    assert store.category_deletes == [[("old", "old", "old", "old", "")]]


def test_empty_update_and_nonproduction_writes_fail_closed():
    service = TrajectoryStatsService(FakeClient(), FakeStore())

    assert service.update_sessions([]) == TrajectoryStatsUpdate()

    with pytest.raises(ValueError, match="production profile"):
        TrajectoryStatsService(FakeClient(profile="test"), FakeStore())
