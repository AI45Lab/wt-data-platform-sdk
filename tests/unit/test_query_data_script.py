import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "inspect" / "query_data.py"
SPEC = importlib.util.spec_from_file_location("query_data_script", SCRIPT_PATH)
query_data = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(query_data)


def test_dataframe_to_json_records_preserves_nested_values():
    frame = pd.DataFrame(
        [
            {
                "id": "event-1",
                "messages": json.dumps(
                    [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]
                ),
                "meta_json": json.dumps(
                    {"task_id": "task-1", "env_state": json.dumps({"step": 3})}
                ),
                "reward": 1.0,
                "optional": float("nan"),
            }
        ]
    )

    records = query_data._dataframe_to_json_records(frame)

    assert records == [
        {
            "id": "event-1",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "hello"}],
                }
            ],
            "meta_json": {
                "task_id": "task-1",
                "env_state": {"step": 3},
            },
            "reward": 1.0,
            "optional": None,
        }
    ]


def test_write_json_output_creates_parent_directories(tmp_path):
    output_path = tmp_path / "nested" / "sample.json"

    resolved_path = query_data._write_json_output(
        str(output_path),
        {"returned_rows": 1, "rows": [{"id": "event-1"}]},
    )

    assert resolved_path == output_path.resolve()
    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "returned_rows": 1,
        "rows": [{"id": "event-1"}],
    }


def test_resolve_db_uri_uses_env_config_database(monkeypatch):
    monkeypatch.setenv("WT_SDK_ENV_CONFIG_DB_URI", "s3://env-config-db")

    assert query_data._resolve_db_uri("evaluation_env_config", None) == "s3://env-config-db"
    assert query_data._resolve_db_uri("env_config_test", None) == "s3://env-config-db"
    assert query_data._uses_latest_snapshot_by_default("evaluation_env_config") is True
    assert query_data._uses_latest_snapshot_by_default("env_config_test") is True


def test_resolve_db_uri_respects_explicit_database(monkeypatch):
    monkeypatch.setenv("WT_SDK_ENV_CONFIG_DB_URI", "s3://env-config-db")

    assert (
        query_data._resolve_db_uri("evaluation_env_config", "s3://override-db")
        == "s3://override-db"
    )
    assert query_data._resolve_db_uri("landing_test", "s3://override-db") == "s3://override-db"
    assert query_data._uses_latest_snapshot_by_default("landing_test") is False


def test_pin_exact_table_skips_unpartitioned_schema_record(capsys):
    session = SimpleNamespace(
        schema_table=SimpleNamespace(
            get=lambda table_name: SimpleNamespace(
                partition_type="VALUE",
                partition_column="",
            )
        ),
        tables={},
        db_conn=object(),
    )

    query_data._pin_exact_dldb_table(session, "evaluation_env_config")

    assert session.tables == {}
    assert capsys.readouterr().out == ""


def test_partitioned_schema_record_detection():
    assert query_data._is_partitioned_schema_record(
        SimpleNamespace(partition_type="HASH", partition_column="job_id")
    )
    assert not query_data._is_partitioned_schema_record(
        SimpleNamespace(partition_type="VALUE", partition_column="")
    )
    assert not query_data._is_partitioned_schema_record(
        SimpleNamespace(partition_type="", partition_column="")
    )


def test_parse_column_list_rejects_empty_and_wildcard():
    assert query_data._parse_column_list("job_id, session_id") == ["job_id", "session_id"]
    assert query_data._parse_column_list(None) is None

    for raw_columns in ("", " , ", "*"):
        try:
            query_data._parse_column_list(raw_columns)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected {raw_columns!r} to be rejected")


def test_distinct_rows_counts_single_and_composite_values():
    records = [
        {"job_id": "job-a", "session_id": "s1"},
        {"job_id": "job-a", "session_id": "s1"},
        {"job_id": "job-a", "session_id": "s2"},
        {"job_id": "job-b", "session_id": "s1"},
        {"job_id": None, "session_id": "s3"},
    ]

    assert query_data._distinct_rows(records, ["job_id"]) == [
        {"job_id": "job-a"},
        {"job_id": "job-b"},
        {"job_id": None},
    ]
    assert query_data._distinct_rows(records, ["job_id", "session_id"]) == [
        {"job_id": "job-a", "session_id": "s1"},
        {"job_id": "job-a", "session_id": "s2"},
        {"job_id": "job-b", "session_id": "s1"},
        {"job_id": None, "session_id": "s3"},
    ]
