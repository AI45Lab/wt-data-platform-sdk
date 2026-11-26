import re

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


class PagedLegacySession:
    def __init__(self, rows, *, sort_rows=True):
        self.rows = list(rows)
        self.sort_rows = sort_rows
        self.calls = []

    def filter(
        self,
        table_name,
        *,
        query,
        columns,
        limit,
        partitions,
        order_by,
        ascending,
        checkout_latest,
    ):
        assert table_name == script.LEGACY_LANDING_TABLE
        self.calls.append(
            {
                "query": query,
                "columns": columns,
                "limit": limit,
                "partitions": partitions,
                "order_by": order_by,
                "ascending": ascending,
                "checkout_latest": checkout_latest,
            }
        )
        selected = list(self.rows)
        job_match = re.search(r"\bjob_id = '((?:''|[^'])*)'", query)
        assert job_match is not None
        expected_job = job_match.group(1).replace("''", "'")
        assert partitions == [
            script.stable_hash(expected_job) % script.LEGACY_LANDING_PARTITIONS
        ]
        selected = [row for row in selected if row["job_id"] == expected_job]
        session_match = re.search(r"\bsession_id IN \((.*?)\)", query)
        assert session_match is not None
        expected_sessions = {
            value.replace("''", "'")
            for value in re.findall(r"'((?:''|[^'])*)'", session_match.group(1))
        }
        selected = [row for row in selected if row["session_id"] in expected_sessions]
        cursor_match = re.search(r"\bid > '((?:''|[^'])*)'", query)
        if cursor_match:
            cursor = cursor_match.group(1).replace("''", "'")
            selected = [row for row in selected if row["id"] > cursor]
        id_page_match = re.search(r"\bid IN \((.*?)\)", query)
        if id_page_match:
            expected_ids = {
                value.replace("''", "'")
                for value in re.findall(r"'((?:''|[^'])*)'", id_page_match.group(1))
            }
            selected = [row for row in selected if row["id"] in expected_ids]
        assert order_by in (None, "id")
        assert ascending is True
        if self.sort_rows:
            selected.sort(key=lambda row: row["id"])
        page = selected[:limit]
        return pd.DataFrame(
            [{column: row.get(column) for column in columns} for row in page]
        )


def test_keyset_reverse_queries_match_batch_backfill_and_ignore_undelivered():
    serving_rows = [
        {"job_id": JOB_ID, "session_id": "legacy-1"},
        {"job_id": JOB_ID, "session_id": "legacy-2"},
        {"job_id": JOB_ID, "session_id": "legacy-1"},
    ]
    source_rows = [
        dict(landing_row(JOB_ID, "legacy-1", 5, step_id=2), id="c", _bucket=0),
        dict(landing_row(JOB_ID, "not-delivered", 999), id="d", _bucket=1),
        dict(landing_row(JOB_ID, "legacy-1", 10, step_id=1), id="a", _bucket=0),
        dict(landing_row(JOB_ID, "legacy-2", 20), id="b", _bucket=1),
    ]
    expected_store = FakeStore()
    expected = script.backfill_legacy_rows(
        serving_rows,
        source_rows,
        expected_store,
        updated_at_ms=UPDATED_AT_MS,
        batch_size=1,
    )
    session = PagedLegacySession(source_rows)
    streamed_store = FakeStore()

    streamed_count = script._backfill_delivered_sessions(
        session,
        script.LEGACY_LANDING_TABLE,
        script._delivered_session_keys(serving_rows),
        streamed_store,
        updated_at_ms=UPDATED_AT_MS,
        batch_size=1,
        session_query_batch_size=25,
        scan_batch_size=2,
    )
    category_update = script.rebuild_category_table(
        streamed_store,
        updated_at_ms=UPDATED_AT_MS,
    )

    assert streamed_count == expected.sessions_updated
    assert category_update.categories_upserted == expected.categories_upserted
    assert category_update.categories_deleted == expected.categories_deleted
    assert streamed_store.sessions == expected_store.sessions
    assert streamed_store.categories == expected_store.categories
    assert streamed_store.sessions[(JOB_ID, "legacy-1")]["total_tokens"] == 15
    assert streamed_store.sessions[(JOB_ID, "legacy-1")]["total_steps"] == 2
    assert [call["limit"] for call in session.calls] == [2, 2, 2, 2]
    assert [call["columns"] for call in session.calls[:2]] == [
        ["id"],
        script.LEGACY_LANDING_COLUMNS,
    ]
    assert "not-delivered" not in session.calls[0]["query"]
    assert "session_id IN ('legacy-1', 'legacy-2')" in session.calls[0]["query"]
    assert "id IN ('a', 'b')" in session.calls[1]["query"]
    assert "id > 'b'" in session.calls[2]["query"]
    assert [call["checkout_latest"] for call in session.calls] == [
        True,
        False,
        False,
        False,
    ]


def test_keyset_reverse_query_rejects_non_increasing_ids():
    rows = [
        dict(landing_row(JOB_ID, "legacy-1", 10), id="same", _bucket=0),
        dict(landing_row(JOB_ID, "legacy-1", 20), id="same", _bucket=0),
    ]
    session = PagedLegacySession(rows, sort_rows=False)

    with pytest.raises(ValueError, match="not strictly increasing"):
        list(
            script._iter_landing_rows(
                session,
                script.LEGACY_LANDING_TABLE,
                job_id=JOB_ID,
                session_ids=["legacy-1"],
                scan_batch_size=2,
                checkout_latest=True,
            )
        )


def test_backfill_does_not_scan_landing_without_delivered_sessions():
    def unexpected_scan():
        yield from pytest.fail("landing must not be scanned without serving sessions")

    result = script.backfill_legacy_rows(
        [],
        unexpected_scan(),
        FakeStore(),
        updated_at_ms=UPDATED_AT_MS,
    )

    assert result.sessions_updated == 0


def test_reverse_queries_are_grouped_by_parent_job_and_session_batch():
    session = PagedLegacySession([])
    store = FakeStore()
    delivered = [
        (JOB_ID, "s3"),
        ("invalid-job", "s1"),
        (JOB_ID, "s1"),
        (JOB_ID, "s2"),
    ]

    updated = script._backfill_delivered_sessions(
        session,
        script.LEGACY_LANDING_TABLE,
        delivered,
        store,
        updated_at_ms=UPDATED_AT_MS,
        batch_size=500,
        session_query_batch_size=2,
        scan_batch_size=2000,
    )

    assert updated == 4
    assert len(session.calls) == 3
    assert "session_id IN ('s1', 's2')" in session.calls[0]["query"]
    assert "session_id IN ('s3')" in session.calls[1]["query"]
    assert "job_id = 'invalid-job'" in session.calls[2]["query"]
    assert [call["checkout_latest"] for call in session.calls] == [True, False, False]
    assert store.sessions[(JOB_ID, "s1")]["total_tokens_status"] == "MISSING"
