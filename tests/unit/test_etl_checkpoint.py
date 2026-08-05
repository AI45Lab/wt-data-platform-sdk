import sys

import pandas as pd
import pytest

import scripts.ops.init_etl_checkpoint_table as init_checkpoint_script
import wt_sdk.etl.checkpoint as checkpoint_module
from wt_sdk.etl import Checkpoint, InMemoryCheckpointStore
from wt_sdk.etl.checkpoint import (
    DldbCheckpointStore,
    ETL_CHECKPOINT_SCHEMA,
    PRODUCTION_CHECKPOINT_TABLE,
    TEST_CHECKPOINT_TABLE,
    resolve_checkpoint_table,
    resolve_etl_state_db_uri,
)


def test_in_memory_checkpoint_identity_includes_pipeline_version_and_bucket():
    store = InMemoryCheckpointStore()
    checkpoint = Checkpoint(
        pipeline_name="serving_publish",
        pipeline_version="2",
        source_table="landing_test",
        target_table="serving_test",
        bucket=17,
        committed_until_ms=123,
    )

    store.save(checkpoint)

    assert (
        store.load(
            pipeline_name="serving_publish",
            pipeline_version="2",
            source_table="landing_test",
            target_table="serving_test",
            bucket=17,
        )
        == checkpoint
    )
    assert checkpoint.checkpoint_id == (
        "serving_publish|2|landing_test|serving_test|17"
    )


def test_etl_state_uri_requires_explicit_configuration(monkeypatch):
    monkeypatch.delenv("WT_SDK_ETL_STATE_DB_URI", raising=False)

    with pytest.raises(ValueError, match="ETL state database is required"):
        resolve_etl_state_db_uri()

    monkeypatch.setenv("WT_SDK_ETL_STATE_DB_URI", "s3://etl-state")
    assert resolve_etl_state_db_uri() == "s3://etl-state"
    assert resolve_etl_state_db_uri("s3://explicit") == "s3://explicit"


def test_checkpoint_table_follows_shared_profile_unless_overridden():
    assert resolve_checkpoint_table("test") == TEST_CHECKPOINT_TABLE
    assert resolve_checkpoint_table("production") == PRODUCTION_CHECKPOINT_TABLE
    assert resolve_checkpoint_table("prod") == PRODUCTION_CHECKPOINT_TABLE
    assert resolve_checkpoint_table("test", "custom_checkpoint") == "custom_checkpoint"


def test_dldb_checkpoint_store_pins_exact_table_and_round_trips(monkeypatch):
    class Record:
        partition_type = ""

    class SchemaTable:
        def get(self, table_name):
            assert table_name == PRODUCTION_CHECKPOINT_TABLE
            return Record()

    class Session:
        def __init__(self):
            self.schema_table = SchemaTable()
            self.db_conn = object()
            self.tables = {}
            self.frame = pd.DataFrame()

        def table_exists(self, table_name):
            return table_name == PRODUCTION_CHECKPOINT_TABLE

        def get_schema(self, table_name):
            assert table_name == PRODUCTION_CHECKPOINT_TABLE
            return ETL_CHECKPOINT_SCHEMA

        def upsert(self, table_name, columns, datas):
            assert table_name == PRODUCTION_CHECKPOINT_TABLE
            assert columns == ["id"]
            self.frame = datas

        def filter(self, table_name, query, limit, checkout_latest):
            assert table_name == PRODUCTION_CHECKPOINT_TABLE
            assert "serving_publish|1|landing_test|serving_test|3" in query
            assert limit == 1
            assert checkout_latest is True
            return self.frame

        def shutdown(self):
            return None

    session = Session()
    sentinel = object()
    monkeypatch.setattr(checkpoint_module.dldb, "connect", lambda *args, **kwargs: session)

    import dldb.table

    monkeypatch.setattr(
        dldb.table,
        "open_table_by_partition_type",
        lambda *args, **kwargs: sentinel,
    )
    store = DldbCheckpointStore("s3://state")
    checkpoint = Checkpoint(
        pipeline_name="serving_publish",
        pipeline_version="1",
        source_table="landing_test",
        target_table="serving_test",
        bucket=3,
        committed_until_ms=5_000,
        last_run_id="serving_publish__v1__run-1",
        status="IDLE",
        updated_at_ms=6_000,
    )

    store.save(checkpoint)
    loaded = store.load(
        pipeline_name="serving_publish",
        pipeline_version="1",
        source_table="landing_test",
        target_table="serving_test",
        bucket=3,
    )

    assert loaded == checkpoint
    assert session.tables[PRODUCTION_CHECKPOINT_TABLE] is sentinel


def test_init_script_creates_test_and_production_tables_by_default(
    monkeypatch,
    capsys,
):
    initialized = []

    class FakeStore:
        def __init__(self, db_uri, table_name):
            assert db_uri == "s3://wind-tunnel-etl"
            self.table_name = table_name

        def initialize(self):
            initialized.append(self.table_name)
            return True

        def close(self):
            return None

    monkeypatch.setattr(init_checkpoint_script, "DldbCheckpointStore", FakeStore)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "init_etl_checkpoint_table.py",
            "--db-uri",
            "s3://wind-tunnel-etl",
            "--confirm-create",
        ],
    )

    assert init_checkpoint_script.main() == 0
    assert initialized == [TEST_CHECKPOINT_TABLE, PRODUCTION_CHECKPOINT_TABLE]
    output = capsys.readouterr().out
    assert TEST_CHECKPOINT_TABLE in output
    assert PRODUCTION_CHECKPOINT_TABLE in output
