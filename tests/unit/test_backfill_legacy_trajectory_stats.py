import pandas as pd
import pytest

from scripts.trajectory_stats import backfill_legacy_trajectory_stats as script
from wt_sdk.trajectory_stats.schemas import CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS
from wt_sdk.trajectory_stats.session_metrics import compute_session_metrics


JOB_ID = "cybergym#claudecode#glm-5.3#挖掘#level1"
UPDATED_AT_MS = 1_700_000_000_000


def landing_row(job_id, session_id, tokens, step_id=1):
    return {
        "id": f"{session_id}-{step_id}",
        "job_id": job_id,
        "session_id": session_id,
        "step_id": step_id,
        "meta_json": {"prompt_tokens": tokens, "completion_tokens": 0},
        "response": {"role": "assistant", "content": []},
        "messages": [{"role": "user", "content": "request"}],
    }


class FakeStore:
    def __init__(self, *, session_rows=(), category_rows=()):
        self.sessions = {
            (row["job_id"], row["session_id"]): dict(row) for row in session_rows
        }
        self.categories = {
            tuple(row[column] for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS): dict(row)
            for row in category_rows
        }
        self.session_batches = []

    def list_session_rows(self):
        return list(self.sessions.values())

    def list_category_rows(self):
        return list(self.categories.values())

    def upsert_session_rows(self, rows):
        materialized = list(rows)
        self.session_batches.append(materialized)
        for row in materialized:
            self.sessions[(row["job_id"], row["session_id"])] = dict(row)
        return len(materialized)

    def upsert_category_rows(self, rows):
        materialized = list(rows)
        for row in materialized:
            key = tuple(row[column] for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS)
            self.categories[key] = dict(row)
        return len(materialized)

    def delete_category_rows(self, keys):
        materialized = list(keys)
        for key in materialized:
            self.categories.pop(key, None)
        return len(materialized)


def test_dry_run_and_unconfirmed_backfill_do_not_connect(monkeypatch, capsys):
    monkeypatch.setattr(
        script.dldb,
        "connect",
        lambda *args, **kwargs: pytest.fail("backfill preflight must not connect"),
    )

    assert script.run_backfill(dry_run=True) == 0
    assert script.run_backfill(confirm_production=True) == 2
    assert script.run_backfill(confirm_backfill=True) == 2

    output = capsys.readouterr().out
    assert script.LEGACY_SERVING_TABLE in output
    assert script.LEGACY_LANDING_TABLE in output
    assert "never dropped or truncated" in output


def test_legacy_backfill_is_idempotent_and_preserves_new_stack_sessions():
    retained = compute_session_metrics(
        JOB_ID,
        "new-stack",
        [landing_row(JOB_ID, "new-stack", 30)],
        updated_at_ms=100,
    )
    store = FakeStore(session_rows=[retained])
    serving_rows = [
        {"job_id": JOB_ID, "session_id": "legacy-2"},
        {"job_id": JOB_ID, "session_id": "legacy-1"},
        {"job_id": JOB_ID, "session_id": "legacy-1"},
    ]
    landing_rows = [
        landing_row(JOB_ID, "legacy-1", 10),
        landing_row(JOB_ID, "legacy-2", 20),
        landing_row(JOB_ID, "not-delivered", 999),
    ]

    first = script.backfill_legacy_rows(
        serving_rows,
        landing_rows,
        store,
        updated_at_ms=UPDATED_AT_MS,
        batch_size=1,
    )
    second = script.backfill_legacy_rows(
        serving_rows,
        landing_rows,
        store,
        updated_at_ms=UPDATED_AT_MS,
        batch_size=1,
    )

    assert first.sessions_updated == 2
    assert second.sessions_updated == 2
    assert first.categories_upserted == 1
    assert len(store.sessions) == 3
    assert set(store.sessions) == {
        (JOB_ID, "new-stack"),
        (JOB_ID, "legacy-1"),
        (JOB_ID, "legacy-2"),
    }
    category = next(iter(store.categories.values()))
    assert category["delivered_session_count"] == 3
    assert category["total_tokens_mean"] == 20.0
    assert category["total_tokens_median"] == 20.0
    assert category["updated_at_ms"] == UPDATED_AT_MS
    assert all(
        row["session_id"] != "not-delivered"
        for batch in store.session_batches
        for row in batch
    )


def test_invalid_legacy_job_is_retained_for_audit_but_not_aggregated():
    store = FakeStore()

    result = script.backfill_legacy_rows(
        [{"job_id": "invalid-job", "session_id": "s1"}],
        [landing_row("invalid-job", "s1", 3)],
        store,
        updated_at_ms=UPDATED_AT_MS,
    )

    assert result.sessions_updated == 1
    assert result.categories_upserted == 0
    row = store.sessions[("invalid-job", "s1")]
    assert row["classification_status"] == "INVALID_JOB_ID"
    assert row["total_tokens"] == 3
    assert store.categories == {}


def test_full_category_rebuild_deletes_stale_rows_without_touching_new_sessions():
    retained = compute_session_metrics(
        JOB_ID,
        "new-stack",
        [landing_row(JOB_ID, "new-stack", 5)],
        updated_at_ms=100,
    )
    stale = {
        "dataset": "old",
        "harness": "old",
        "model": "old",
        "task_type": "old",
        "level": "",
    }
    store = FakeStore(session_rows=[retained], category_rows=[stale])

    result = script.backfill_legacy_rows(
        [],
        [],
        store,
        updated_at_ms=UPDATED_AT_MS,
    )

    assert result.sessions_updated == 0
    assert result.categories_upserted == 1
    assert result.categories_deleted == 1
    assert (JOB_ID, "new-stack") in store.sessions
    assert ("old", "old", "old", "old", "") not in store.categories


def test_read_rows_selects_only_required_columns_and_normalizes_nulls():
    class LegacySession:
        def filter(self, table_name, **kwargs):
            assert table_name == script.LEGACY_LANDING_TABLE
            assert kwargs["columns"] == script.LEGACY_LANDING_COLUMNS
            assert kwargs["checkout_latest"] is True
            return pd.DataFrame(
                [{"id": "row-1", "job_id": JOB_ID, "session_id": pd.NA}]
            )

    rows = script._read_rows(
        LegacySession(),
        script.LEGACY_LANDING_TABLE,
        script.LEGACY_LANDING_COLUMNS,
    )

    assert rows == [{"id": "row-1", "job_id": JOB_ID, "session_id": None}]
