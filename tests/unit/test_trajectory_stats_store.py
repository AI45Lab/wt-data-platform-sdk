from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest

from wt_sdk.config import (
    DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE,
    DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE,
)
from wt_sdk.trajectory_stats.schemas import (
    CATEGORY_TRAJECTORY_STATS_SCHEMA,
    SESSION_TRAJECTORY_METRICS_SCHEMA,
)
from wt_sdk.trajectory_stats.store import TrajectoryStatsStore


class FakeSession:
    def __init__(self):
        session_record = SimpleNamespace(
            partition_column="job_id",
            partition_type="HASH",
            partitions=128,
        )
        category_record = SimpleNamespace(
            partition_column="",
            partition_type="",
            partitions=-1,
        )
        self.schema_table = {
            DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE: session_record,
            DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE: category_record,
        }
        self.schemas = {
            DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE: SESSION_TRAJECTORY_METRICS_SCHEMA,
            DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE: CATEGORY_TRAJECTORY_STATS_SCHEMA,
        }
        self.upserts = []
        self.deletes = []
        self.filter_rows = []
        self.shutdown_called = False

    def table_exists(self, table_name):
        return table_name in self.schemas

    def get_schema(self, table_name):
        return self.schemas[table_name]

    def filter(self, table_name, *, query, limit, checkout_latest):
        assert table_name == DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE
        assert query == "job_id IS NOT NULL"
        assert limit is None
        assert checkout_latest is True
        return pd.DataFrame(self.filter_rows)

    def upsert(self, table_name, *, columns, datas):
        self.upserts.append((table_name, columns, datas))

    def delete(self, table_name, query):
        self.deletes.append((table_name, query))

    def shutdown(self):
        self.shutdown_called = True


def make_store(monkeypatch, session=None, db_uri="s3://stats"):
    fake = session or FakeSession()
    captured = {}

    def connect(uri, **kwargs):
        captured["uri"] = uri
        captured["kwargs"] = kwargs
        return fake

    monkeypatch.setattr("wt_sdk.trajectory_stats.store.dldb.connect", connect)
    monkeypatch.setattr(
        "wt_sdk.trajectory_stats.store.storage_options_for",
        lambda uri, s3: {"endpoint": "test"} if uri.startswith("s3://") else None,
    )
    return TrajectoryStatsStore(db_uri), fake, captured


def test_store_uses_one_serving_db_and_never_creates_tables(monkeypatch):
    store, session, captured = make_store(monkeypatch)

    store.verify_ready()

    assert captured["uri"] == "s3://stats"
    assert captured["kwargs"] == {"storage_options": {"endpoint": "test"}}
    assert store.session_table == DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE
    assert store.category_table == DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE
    assert not hasattr(session, "create_table")


def test_session_and_category_upserts_use_their_composite_keys(monkeypatch):
    store, session, _ = make_store(monkeypatch)
    session_row = {
        "job_id": "job-a",
        "session_id": "s1",
        "classification_status": "INVALID_JOB_ID",
        "dataset": None,
        "harness": None,
        "model": None,
        "task_type": None,
        "level": "",
        "total_tokens": None,
        "total_tokens_status": "MISSING",
        "total_tokens_reason": "TOKEN_USAGE_MISSING",
        "total_steps": 1,
        "total_steps_status": "VALID",
        "total_steps_reason": None,
        "tool_calls": 0,
        "tool_calls_status": "VALID",
        "tool_calls_reason": None,
        "updated_at_ms": 10,
    }

    assert store.upsert_session_rows([session_row]) == 1
    assert store.upsert_session_rows([]) == 0

    table_name, keys, frame = session.upserts[0]
    assert table_name == DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE
    assert keys == ["job_id", "session_id"]
    assert (
        pa.Table.from_pandas(
            frame,
            schema=SESSION_TRAJECTORY_METRICS_SCHEMA,
            preserve_index=False,
        ).schema
        == SESSION_TRAJECTORY_METRICS_SCHEMA
    )
    assert frame.iloc[0]["tool_calls"] == 0


def test_delete_categories_escapes_exact_keys_without_full_table_delete(monkeypatch):
    store, session, _ = make_store(monkeypatch)

    deleted = store.delete_category_rows(
        [("cybergym", "claudecode", "model'o", "挖掘", "level1")]
    )

    assert deleted == 1
    table_name, query = session.deletes[0]
    assert table_name == DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE
    assert "model = 'model''o'" in query
    assert query.startswith("(dataset = 'cybergym'")
    assert "1 = 1" not in query


def test_list_session_rows_normalizes_pandas_nulls(monkeypatch):
    store, session, _ = make_store(monkeypatch)
    session.filter_rows = [
        {
            "job_id": "job-a",
            "session_id": "s1",
            "total_tokens": pd.NA,
            "total_steps": 2,
        }
    ]

    assert store.list_session_rows() == [
        {
            "job_id": "job-a",
            "session_id": "s1",
            "total_tokens": None,
            "total_steps": 2,
        }
    ]


def test_missing_or_mismatched_table_fails_closed(monkeypatch):
    store, session, _ = make_store(monkeypatch)
    del session.schemas[DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE]

    with pytest.raises(ValueError, match="does not exist"):
        store.verify_ready()

    session.schemas[DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE] = (
        CATEGORY_TRAJECTORY_STATS_SCHEMA
    )
    session.schemas[DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE] = (
        CATEGORY_TRAJECTORY_STATS_SCHEMA
    )
    with pytest.raises(ValueError, match="schema does not match"):
        store.verify_ready()


def test_close_shuts_down_shared_connection(monkeypatch):
    store, session, _ = make_store(monkeypatch)

    store.close()

    assert session.shutdown_called is True
