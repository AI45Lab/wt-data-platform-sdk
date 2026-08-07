import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "ops" / "cleanup_data.py"
SPEC = importlib.util.spec_from_file_location("cleanup_data_script", SCRIPT_PATH)
cleanup_data = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup_data)


class FakeCleanupSession:
    def __init__(self):
        self.tables = {}
        self.schema_table = SimpleNamespace(
            get=lambda table_name: SimpleNamespace(
                partition_type="VALUE",
                partition_column="",
            )
        )
        self.db_conn = object()
        self.filter_calls = []
        self.delete_calls = []
        self.shutdown_called = False

    def table_exists(self, table_name):
        return table_name == "evaluation_env_config"

    def list_tables(self):
        return ["evaluation_env_config"]

    def count_rows(self, table_name):
        assert table_name == "evaluation_env_config"
        return 2

    def filter(self, table_name, query, limit=None, checkout_latest=False):
        self.filter_calls.append(
            {
                "table_name": table_name,
                "query": query,
                "limit": limit,
                "checkout_latest": checkout_latest,
            }
        )
        return pd.DataFrame(
            [
                {
                    "id": 1,
                    "job_id": "gateway",
                    "env_id": "env-1",
                    "env_name": "test-env",
                }
            ]
        )

    def delete(self, table_name, query):
        self.delete_calls.append({"table_name": table_name, "query": query})

    def shutdown(self):
        self.shutdown_called = True


def test_resolve_db_uri_uses_env_config_database(monkeypatch):
    monkeypatch.setenv("WT_SDK_ENV_CONFIG_DB_URI", "s3://env-config-db")

    assert cleanup_data._resolve_db_uri("evaluation_env_config", None) == "s3://env-config-db"
    assert cleanup_data._resolve_db_uri("landing_test", "s3://override") == "s3://override"
    assert cleanup_data._uses_latest_snapshot_by_default("evaluation_env_config") is True
    assert cleanup_data._uses_latest_snapshot_by_default("landing_test") is False


def test_pin_exact_table_skips_unpartitioned_schema_record(capsys):
    session = FakeCleanupSession()

    cleanup_data._pin_exact_dldb_table(session, "evaluation_env_config")

    assert session.tables == {}
    assert capsys.readouterr().out == ""


def test_cleanup_env_config_dry_run_uses_env_db_and_latest(monkeypatch):
    fake_session = FakeCleanupSession()
    connect_args = {}

    def fake_connect(db_uri, storage_options):
        connect_args["db_uri"] = db_uri
        connect_args["storage_options"] = storage_options
        return fake_session

    monkeypatch.setenv("WT_SDK_ENV_CONFIG_DB_URI", "s3://env-config-db")
    monkeypatch.setattr(cleanup_data.dldb, "connect", fake_connect)
    monkeypatch.setattr(
        "sys.argv",
        [
            "cleanup_data.py",
            "--table",
            "evaluation_env_config",
            "--query",
            "job_id = 'gateway'",
            "--dry-run",
        ],
    )

    assert cleanup_data.main() == 0
    assert connect_args["db_uri"] == "s3://env-config-db"
    assert fake_session.delete_calls == []
    assert len(fake_session.filter_calls) == 2
    assert all(call["checkout_latest"] is True for call in fake_session.filter_calls)
    assert fake_session.shutdown_called is True
