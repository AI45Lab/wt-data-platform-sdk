from types import SimpleNamespace

import pyarrow as pa

from scripts.ops import maintain_env_config_indexes as script


class FakeSession:
    def __init__(self, existing_indexes, table_name=script.TEST_ENV_CONFIG_TABLE):
        self.table_name = table_name
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
        assert table_name == self.table_name
        return script.EVALUATION_ENV_SCHEMA

    def list_indices(self, table_name):
        assert table_name == self.table_name
        return [SimpleNamespace(name=name) for name in self.existing_indexes]

    def create_scalar_index(self, table_name, column, *, index_type):
        assert table_name == self.table_name
        self.created.append((column, index_type))
        self.existing_indexes.append(f"{column}_idx")

    def optimize(self, table_name):
        self.optimize_calls.append(table_name)

    def list_index_coverage(self, table_name):
        assert table_name == self.table_name
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
    monkeypatch.setattr(
        script,
        "_pin_exact_dldb_table",
        lambda value, table_name: None,
    )

    summary = script.maintain_env_config_indexes(session)

    assert session.created == [("job_id", "BTREE")]
    assert session.optimize_calls == [script.TEST_ENV_CONFIG_TABLE]
    assert summary["table_name"] == script.TEST_ENV_CONFIG_TABLE
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

    script._pin_exact_dldb_table(session, script.TEST_ENV_CONFIG_TABLE)

    assert session.tables[script.TEST_ENV_CONFIG_TABLE] is marker
    assert calls == [
        (session.db_conn, session.schema_table, script.TEST_ENV_CONFIG_TABLE)
    ]


def test_dry_run_reports_missing_without_writes(monkeypatch):
    session = FakeSession([])
    monkeypatch.setattr(
        script,
        "_pin_exact_dldb_table",
        lambda value, table_name: None,
    )

    summary = script.maintain_env_config_indexes(session, dry_run=True)

    assert session.created == []
    assert session.optimize_calls == []
    assert summary["missing_indexes_before"] == summary["expected_indexes"]
    assert summary["coverage"] == []


def test_production_table_can_be_selected_explicitly(monkeypatch):
    production_table = "evaluation_env_config"
    session = FakeSession([], table_name=production_table)
    monkeypatch.setattr(
        script,
        "_pin_exact_dldb_table",
        lambda value, table_name: None,
    )

    summary = script.maintain_env_config_indexes(
        session,
        table_name=production_table,
        dry_run=True,
    )

    assert summary["table_name"] == production_table


def test_schema_mismatch_stops_before_index_mutation(monkeypatch):
    session = FakeSession([])
    session.get_schema = lambda table_name: pa.schema([pa.field("id", pa.int64())])
    monkeypatch.setattr(
        script,
        "_pin_exact_dldb_table",
        lambda value, table_name: None,
    )

    try:
        script.maintain_env_config_indexes(session)
    except ValueError as exc:
        assert "missing schema fields" in str(exc)
    else:
        raise AssertionError("schema mismatch should fail")

    assert session.created == []
    assert session.optimize_calls == []
