from types import SimpleNamespace

import pyarrow as pa

from scripts.ops import maintain_env_config_indexes as script


class FakeSession:
    def __init__(self, existing_indexes):
        self.existing_indexes = list(existing_indexes)
        self.created = []
        self.optimize_calls = []
        self.tables = {}
        self.schema_table = SimpleNamespace(
            get=lambda table_name: SimpleNamespace(
                partition_column="",
                partition_type="",
            )
        )
        self.db_conn = object()

    def get_schema(self, table_name):
        assert table_name == script.TABLE_NAME
        return script.EVALUATION_ENV_SCHEMA

    def list_indices(self, table_name):
        assert table_name == script.TABLE_NAME
        return [SimpleNamespace(name=name) for name in self.existing_indexes]

    def create_scalar_index(self, table_name, column, *, index_type):
        assert table_name == script.TABLE_NAME
        self.created.append((column, index_type))
        self.existing_indexes.append(f"{column}_idx")

    def optimize(self, table_name):
        self.optimize_calls.append(table_name)

    def list_index_coverage(self, table_name):
        assert table_name == script.TABLE_NAME
        return [
            SimpleNamespace(
                index_name=name,
                num_indexed_rows=10,
                num_unindexed_rows=0,
                fully_indexed=True,
            )
            for name in self.existing_indexes
        ]


def test_maintain_creates_missing_job_index_and_fully_optimizes(monkeypatch):
    session = FakeSession(
        ["env_name_idx", "env_id_idx", "group_id_idx", "finished_idx"]
    )
    monkeypatch.setattr(script, "_pin_exact_dldb_table", lambda value: None)

    summary = script.maintain_env_config_indexes(session)

    assert session.created == [("job_id", "BTREE")]
    assert session.optimize_calls == [script.TABLE_NAME]
    assert summary["indexes_created"] == [
        {
            "column": "job_id",
            "index_name": "job_id_idx",
            "index_type": "BTREE",
        }
    ]
    assert summary["optimized"] is True
    assert summary["errors"] == []


def test_pin_exact_treats_blank_partition_column_as_simple_table(monkeypatch):
    session = FakeSession([])
    marker = object()
    calls = []

    def fake_from_table_name(db_conn, schema_table, table_name):
        calls.append((db_conn, schema_table, table_name))
        return marker

    monkeypatch.setattr(
        "dldb.table.SimpleTable.from_table_name",
        fake_from_table_name,
    )

    script._pin_exact_dldb_table(session)

    assert session.tables[script.TABLE_NAME] is marker
    assert calls == [(session.db_conn, session.schema_table, script.TABLE_NAME)]


def test_dry_run_reports_missing_without_writes(monkeypatch):
    session = FakeSession([])
    monkeypatch.setattr(script, "_pin_exact_dldb_table", lambda value: None)

    summary = script.maintain_env_config_indexes(session, dry_run=True)

    assert session.created == []
    assert session.optimize_calls == []
    assert summary["missing_indexes_before"] == summary["expected_indexes"]
    assert summary["coverage"] == []


def test_schema_mismatch_stops_before_index_mutation(monkeypatch):
    session = FakeSession([])
    session.get_schema = lambda table_name: pa.schema([pa.field("id", pa.int64())])
    monkeypatch.setattr(script, "_pin_exact_dldb_table", lambda value: None)

    try:
        script.maintain_env_config_indexes(session)
    except ValueError as exc:
        assert "missing schema fields" in str(exc)
    else:
        raise AssertionError("schema mismatch should fail")

    assert session.created == []
    assert session.optimize_calls == []
