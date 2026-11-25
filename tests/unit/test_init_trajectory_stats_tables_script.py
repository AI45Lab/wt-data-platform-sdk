from types import SimpleNamespace

import pyarrow as pa
import pytest

from scripts.trajectory_stats import init_trajectory_stats_tables as script
from wt_sdk.config import (
    DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE,
    DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE,
)
from wt_sdk.trajectory_stats.schemas import (
    CATEGORY_TRAJECTORY_STATS_SCHEMA,
    SESSION_TRAJECTORY_METRICS_SCHEMA,
)


class FakeSession:
    def __init__(self):
        self.schemas = {}
        self.records = {}
        self.create_calls = []
        self.schema_table = SimpleNamespace(get=self.records.get)

    def table_exists(self, table_name):
        return table_name in self.schemas

    def get_schema(self, table_name):
        return self.schemas[table_name]

    def create_table(self, table_name, schema, **partition_kwargs):
        self.create_calls.append((table_name, schema, partition_kwargs))
        self.schemas[table_name] = schema
        self.records[table_name] = SimpleNamespace(
            partition_column=partition_kwargs.get("partition_column"),
            partition_type=partition_kwargs.get("partition_type"),
            partitions=partition_kwargs.get("partitions"),
        )


def test_dry_run_uses_production_serving_db_without_connecting(monkeypatch, capsys):
    monkeypatch.setenv("WT_SDK_PROFILE", "test")
    monkeypatch.setenv("WT_SDK_SERVING_DB_URI", "s3://explicit-serving")
    monkeypatch.setattr(
        script.dldb,
        "connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("dry-run must not connect")
        ),
    )

    assert script.init_trajectory_stats_tables(dry_run=True) == 0

    output = capsys.readouterr().out
    assert "Production serving database: s3://explicit-serving" in output
    assert DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE in output
    assert DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE in output
    assert "HASH(job_id) x 128" in output


@pytest.mark.parametrize(
    "confirm_create,confirm_production",
    [(False, False), (True, False), (False, True)],
)
def test_creation_requires_both_confirmations(
    monkeypatch, confirm_create, confirm_production
):
    monkeypatch.setattr(
        script.dldb,
        "connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unconfirmed creation must not connect")
        ),
    )

    assert script.init_trajectory_stats_tables(
        confirm_create=confirm_create,
        confirm_production=confirm_production,
    ) == 2


def test_initialization_creates_both_tables_once_and_reuses_connection(monkeypatch):
    session = FakeSession()
    connections = []

    def connect(db_uri, **kwargs):
        connections.append((db_uri, kwargs))
        return session

    monkeypatch.setattr(script.dldb, "connect", connect)

    for _ in range(2):
        assert script.init_trajectory_stats_tables(
            confirm_create=True,
            confirm_production=True,
        ) == 0

    assert len(connections) == 2
    assert len(session.create_calls) == 2
    session_call, category_call = session.create_calls
    assert session_call[0] == DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE
    assert session_call[1] == SESSION_TRAJECTORY_METRICS_SCHEMA
    assert session_call[2] == {
        "partition_column": "job_id",
        "partition_type": "HASH",
        "partitions": 128,
    }
    assert category_call == (
        DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE,
        CATEGORY_TRAJECTORY_STATS_SCHEMA,
        {},
    )


def test_existing_schema_mismatch_raises_without_creating_tables(monkeypatch):
    session = FakeSession()
    session.schemas[DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE] = pa.schema(
        [pa.field("wrong", pa.string())]
    )
    session.records[DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE] = SimpleNamespace(
        partition_column="job_id",
        partition_type="HASH",
        partitions=128,
    )
    monkeypatch.setattr(script.dldb, "connect", lambda *args, **kwargs: session)

    with pytest.raises(ValueError, match="schema does not match"):
        script.init_trajectory_stats_tables(
            confirm_create=True,
            confirm_production=True,
        )

    assert session.create_calls == []


def test_existing_partition_mismatch_raises_without_creating_tables(monkeypatch):
    session = FakeSession()
    session.schemas[DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE] = (
        SESSION_TRAJECTORY_METRICS_SCHEMA
    )
    session.records[DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE] = SimpleNamespace(
        partition_column="session_id",
        partition_type="HASH",
        partitions=128,
    )
    monkeypatch.setattr(script.dldb, "connect", lambda *args, **kwargs: session)

    with pytest.raises(ValueError, match="partitioning does not match"):
        script.init_trajectory_stats_tables(
            confirm_create=True,
            confirm_production=True,
        )

    assert session.create_calls == []
