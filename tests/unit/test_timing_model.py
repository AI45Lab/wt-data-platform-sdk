import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest
from dldb.utils import stable_hash

from wt_sdk import EnvConfigManager, GatewayConfig, LandingRecord, TableConfig, WTGatewayClient
import wt_sdk.client as client_module
import wt_sdk.env_config_client as env_manager_module

class FakeSession:
    def __init__(self, *, attach_df_timing: bool, shutdown_result: Optional[Dict[str, Any]] = None):
        self.attach_df_timing = attach_df_timing
        self.shutdown_result = shutdown_result
        self.last_call: Optional[Dict[str, Any]] = None
        self.last_filter_kwargs: Optional[Dict[str, Any]] = None
        self.last_count_kwargs: Optional[Dict[str, Any]] = None
        self.last_delete_kwargs: Optional[Dict[str, Any]] = None
        self.last_update_kwargs: Optional[Dict[str, Any]] = None
        self.filter_calls: List[Dict[str, Any]] = []
        self.created_indexes: List[Dict[str, Any]] = []
        self.optimized_partitions: List[Dict[str, Any]] = []
        self.indexes: Dict[tuple, set] = {}
        self.rows: Dict[str, List[Dict[str, Any]]] = {}

    def _set_last_call(self, api: str, rows: Optional[int] = None) -> Dict[str, Any]:
        timing = {
            "api": api,
            "elapsed_ms": 12.5,
            "ok": True,
            "rows": rows,
            "bytes": 256,
            "rows_per_s": 80.0 if rows is not None else None,
            "mb_per_s": 1.5,
        }
        self.last_call = timing
        return timing

    def add(self, table_name: str, df: pd.DataFrame, partition=None):
        _ = partition
        records = df.to_dict("records")
        self.rows.setdefault(table_name, []).extend(records)
        self._set_last_call("add", len(records))

    def filter(
        self,
        table_name: str,
        query: str,
        limit: int = None,
        columns: List[str] = None,
        offset: int = None,
        *,
        partitions: list = None,
        partition_cond: str = None,
        order_by: str = None,
        ascending: bool = True,
        checkout_latest: bool = False,
    ) -> pd.DataFrame:
        _ = partition_cond
        self.last_filter_kwargs = {
            "table_name": table_name,
            "query": query,
            "partitions": partitions,
            "partition_cond": partition_cond,
            "limit": limit,
            "order_by": order_by,
            "ascending": ascending,
            "checkout_latest": checkout_latest,
            "columns": columns,
            "offset": offset,
        }
        self.filter_calls.append(dict(self.last_filter_kwargs))
        df = pd.DataFrame(self.rows.get(table_name, []))
        if not df.empty and partitions and "__partition" in df.columns:
            df = df[df["__partition"].isin(partitions)]
        if not df.empty and query:
            for condition in query.split(" AND "):
                condition = condition.strip().strip("()")
                if " IN (" in condition:
                    key, raw_values = condition.split(" IN ", 1)
                    values = re.findall(r"'((?:''|[^'])*)'", raw_values)
                    values = [value.replace("''", "'") for value in values]
                    df = df[df[key.strip()].astype(str).isin(values)]
                elif " = '" in condition:
                    key, raw_value = condition.split(" = ", 1)
                    value = raw_value.strip().strip("'")
                    df = df[df[key.strip()] == value]
                elif " > " in condition:
                    key, raw_value = condition.split(" > ", 1)
                    df = df[df[key.strip()].astype(int) > int(raw_value.strip())]
                elif " IS NOT NULL" in condition:
                    key = condition.replace(" IS NOT NULL", "").strip()
                    df = df[df[key].notna()]

        if order_by and not df.empty:
            df = df.sort_values(order_by, ascending=ascending)
        if offset is not None:
            df = df.iloc[offset:]
        if columns is not None and not df.empty:
            df = df[columns]
        if limit is not None:
            df = df.head(limit)

        df = df.reset_index(drop=True)
        timing = self._set_last_call("filter", len(df))
        if self.attach_df_timing:
            df.attrs["dldb"] = timing
        return df

    def count_rows(self, table_name: str, partition=None) -> int:
        self.last_count_kwargs = {
            "table_name": table_name,
            "partition": partition,
        }
        count = len(self.rows.get(table_name, []))
        self._set_last_call("count_rows", count)
        return count

    def delete(self, table_name: str, where: str, partition=None):
        self.last_delete_kwargs = {
            "table_name": table_name,
            "where": where,
            "partition": partition,
        }
        records = self.rows.get(table_name, [])
        if " = '" in where:
            key, raw_value = where.split(" = ", 1)
            value = raw_value.strip().strip("'")
            self.rows[table_name] = [row for row in records if row.get(key.strip()) != value]
        elif "IS NOT NULL" in where:
            self.rows[table_name] = []
        self._set_last_call("delete")

    def update(self, table_name: str, where: str, values: Dict[str, Any], partition=None):
        self.last_update_kwargs = {
            "table_name": table_name,
            "where": where,
            "values": values,
            "partition": partition,
        }
        records = self.rows.get(table_name, [])
        if " = '" in where:
            key, raw_value = where.split(" = ", 1)
            value = raw_value.strip().strip("'")
            for record in records:
                if record.get(key.strip()) == value:
                    record.update(values)
        self._set_last_call("update", len(values))

    def create_scalar_index(self, table_name: str, column: str, *, partition=None, index_type: str = "BTREE"):
        self.created_indexes.append(
            {
                "table_name": table_name,
                "column": column,
                "partition": partition,
                "index_type": index_type,
            }
        )
        self.indexes.setdefault((table_name, partition), set()).add(f"{column}_idx")
        self._set_last_call("create_scalar_index")

    def list_indices(self, table_name: str, partition=None):
        class _Index:
            def __init__(self, name: str):
                self.name = name

        return [
            _Index(name)
            for name in sorted(self.indexes.get((table_name, partition), set()))
        ]

    def optimize(self, table_name: str, *, partition=None, **kwargs):
        self.optimized_partitions.append(
            {
                "table_name": table_name,
                "partition": partition,
                "kwargs": kwargs,
            }
        )
        self._set_last_call("optimize")

    def shutdown(self):
        return self.shutdown_result

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.rows


class _FakeSchemaRecord:
    def __init__(self, partition_column: str, partition_type: str = "VALUE", partitions: int = -1):
        self.partition_column = partition_column
        self.partition_type = partition_type
        self.partitions = partitions


class _FakeSchemaTable:
    def __init__(self, partition_column: str, partition_type: str = "VALUE", partitions: int = -1):
        self._record = _FakeSchemaRecord(partition_column, partition_type, partitions)

    def get(self, table_name: str):
        _ = table_name
        return self._record


def _capture_logger(monkeypatch, module):
    messages: List[str] = []
    monkeypatch.setattr(module.logger, "info", lambda message: messages.append(str(message)))
    monkeypatch.setattr(module.logger, "debug", lambda message: None)
    monkeypatch.setattr(module.logger, "warning", lambda message: None)
    monkeypatch.setattr(module.logger, "error", lambda message: None)
    return messages


def _make_landing_record(record_id: str = "rec-1") -> LandingRecord:
    return LandingRecord(
        dataset_type="SFT",
        dt="2025-01-12",
        id=record_id,
        session_id="session-1",
        created_at=1736640000,
        job_id="job-123",
        messages='[{"role":"user","content":"hello"}]',
        response='{"role":"assistant","content":"world"}',
    )


def test_gateway_config_resolves_env_overrides(monkeypatch):
    monkeypatch.setenv("WT_SDK_DLDB_MODEL", "metrics")
    monkeypatch.setenv("WT_SDK_LOG_DLDB_TIMING", "1")

    config = GatewayConfig(dldb_model="debug", enable_dldb_timing_logs=False)

    assert config.to_dldb_config()["model"] == "metrics"
    assert config.resolved_enable_dldb_timing_logs() is True


def test_wt_gateway_client_debug_logs_df_attrs_and_last_call(monkeypatch):
    fake_session = FakeSession(attach_df_timing=True)
    captured = _capture_logger(monkeypatch, client_module)
    connect_kwargs = {}

    def fake_connect(db_uri, **kwargs):
        connect_kwargs["db_uri"] = db_uri
        connect_kwargs.update(kwargs)
        return fake_session

    monkeypatch.setattr(client_module.dldb, "connect", fake_connect)

    client = WTGatewayClient(
        GatewayConfig(
            tables=TableConfig(landing_table="landing_test"),
            dldb_model="debug",
            enable_dldb_timing_logs=True,
        )
    )

    client.ingest_landing(_make_landing_record())
    df = client.pull_data("SFT", limit=10)

    assert connect_kwargs["model"] == "debug"
    assert len(df) == 1
    assert any("dldb_timing api=add" in message for message in captured)
    assert any("dldb_timing api=filter" in message and "table_name=landing_test" in message for message in captured)


def test_wt_gateway_client_close_returns_metrics_summary(monkeypatch):
    summary = {
        "model": "metrics",
        "total_calls": 3,
        "total_errors": 0,
        "total_latency_seconds_sum": 0.12,
        "total_rows": 10,
        "total_bytes": 2048,
        "by_api": {"filter": {"calls_total": 1}},
    }
    fake_session = FakeSession(attach_df_timing=False, shutdown_result=summary)
    captured = _capture_logger(monkeypatch, client_module)

    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(
        GatewayConfig(
            dldb_model="metrics",
            enable_dldb_timing_logs=True,
        )
    )

    returned = client.close()

    assert returned == summary
    assert any("dldb_metrics_summary" in message for message in captured)


def test_wt_gateway_client_writes_metrics_log_file(monkeypatch):
    summary = {
        "model": "metrics",
        "total_calls": 2,
        "total_errors": 0,
        "total_latency_seconds_sum": 0.05,
        "total_rows": 1,
        "total_bytes": 256,
        "by_api": {"add": {"calls_total": 1}, "filter": {"calls_total": 1}},
    }
    fake_session = FakeSession(attach_df_timing=False, shutdown_result=summary)
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    log_path = Path(__file__).parents[1] / "artifacts" / "metrics_log.txt"
    log_path.unlink(missing_ok=True)

    client = WTGatewayClient(
        GatewayConfig(
            tables=TableConfig(landing_table="landing_test"),
            dldb_model="metrics",
            enable_dldb_timing_logs=False,
            dldb_metrics_log_path=str(log_path),
        )
    )

    client.ingest_landing(_make_landing_record())
    client.pull_data("SFT", limit=10)
    returned = client.close()

    assert returned == summary

    events = [json.loads(line) for line in log_path.read_text().splitlines()]
    event_types = [event["event"] for event in events]

    assert event_types.count("dldb_timing") == 2
    assert "dldb_metrics_summary" in event_types
    assert events[-1]["payload"] == summary
    assert any(event["payload"].get("api") == "add" for event in events)
    assert any(event["payload"].get("api") == "filter" for event in events)


def test_pull_data_prunes_job_id_partition_from_where_sql(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "is_terminal": True,
            "created_at": 100,
            "id": "rec-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(
        GatewayConfig(
            tables=TableConfig(landing_table="landing_test"),
            dldb_model="metrics",
        )
    )

    client.pull_data(
        "RL",
        where_sql="job_id = 'job-123' AND is_terminal = True",
        limit=100,
        checkout_latest=True,
    )

    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]
    assert fake_session.last_filter_kwargs["partition_cond"] is None


def test_pull_and_iter_data_batches_can_read_named_serving_table(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["serving_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "serving-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(
        GatewayConfig(tables=TableConfig(landing_table="landing_test", serving_table="serving_test"))
    )

    page = client.pull_data(
        "RL",
        where_sql="job_id = 'job-123'",
        table="serving_test",
    )
    assert list(page["id"]) == ["serving-1"]
    assert fake_session.last_filter_kwargs["table_name"] == "serving_test"
    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]

    batches = list(
        client.iter_data_batches(
            "RL",
            where_sql="job_id = 'job-123'",
            chunk_size=10,
            table="serving_test",
        )
    )
    assert len(batches) == 1
    assert list(batches[0]["id"]) == ["serving-1"]
    assert fake_session.last_filter_kwargs["table_name"] == "serving_test"
    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]


def test_export_data_batches_builds_fixed_manifest_and_defaults_to_serving(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["serving_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-a",
            "created_at": 100,
            "id": "serving-2",
            "__partition": 2,
        },
        {
            "dataset_type": "RL",
            "job_id": "job-b",
            "created_at": 100,
            "id": "serving-1",
            "__partition": 1,
        },
        {
            "dataset_type": "SFT",
            "job_id": "job-b",
            "created_at": 101,
            "id": "serving-ignored",
            "__partition": 1,
        },
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(
        GatewayConfig(tables=TableConfig(landing_table="landing_test", serving_table="serving_test"))
    )
    monkeypatch.setattr(client, "_list_existing_partitions_for_table", lambda table_name: [1, 2])

    batches = list(
        client.export_data_batches(
            filter_query="dataset_type = 'RL'",
            batch_size=1,
            columns=["created_at"],
        )
    )

    assert [value for batch in batches for value in batch["created_at"].tolist()] == [100, 100]
    assert all(list(batch.columns) == ["created_at"] for batch in batches)
    assert all(batch.attrs["wt_export"]["table"] == "serving_test" for batch in batches)
    assert [call["columns"] for call in fake_session.filter_calls[:2]] == [["id"], ["id"]]
    assert all(call["checkout_latest"] is True for call in fake_session.filter_calls)
    assert all(call["table_name"] == "serving_test" for call in fake_session.filter_calls)


def test_export_data_batches_rejects_duplicate_ids_before_yielding(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["serving_test"] = [
        {"dataset_type": "RL", "id": "duplicate", "__partition": 1},
        {"dataset_type": "RL", "id": "duplicate", "__partition": 2},
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(serving_table="serving_test")))
    monkeypatch.setattr(client, "_list_existing_partitions_for_table", lambda table_name: [1, 2])

    with pytest.raises(RuntimeError, match="globally unique IDs"):
        list(client.export_data_batches(filter_query="dataset_type = 'RL'", batch_size=1))

    assert len(fake_session.filter_calls) == 2


def test_query_data_converts_job_id_partition_string_to_hash_bucket(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "session_id": "session-1",
            "created_at": 100,
            "id": "rec-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    client.query_data(
        filter_query="job_id = 'job-123' AND session_id = 'session-1'",
        partition="job-123",
    )

    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]


def test_query_data_supports_production_job_id_with_hash_separators(monkeypatch):
    job_id = "dataset#harness#model#task-type#20260804#owner#integration-run"
    bucket = stable_hash(job_id) % 128
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": job_id,
            "session_id": "session-production-job-id",
            "created_at": 100,
            "id": "rec-production-job-id",
            "__partition": bucket,
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))
    result = client.query_data(
        filter_query=f"job_id = '{job_id}' AND session_id = 'session-production-job-id'",
        partition=job_id,
    )

    assert [record["id"] for record in result] == ["rec-production-job-id"]
    assert fake_session.last_filter_kwargs["partitions"] == [bucket]
    assert f"job_id = '{job_id}'" in fake_session.last_filter_kwargs["query"]


def test_query_data_adds_job_id_filter_when_partition_string_is_raw_job_id(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "session_id": "session-1",
            "created_at": 100,
            "id": "rec-1",
        },
        {
            "dataset_type": "RL",
            "job_id": "job-collision",
            "session_id": "session-1",
            "created_at": 101,
            "id": "rec-2",
        },
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    client.query_data(
        filter_query="session_id = 'session-1'",
        partition="job-123",
    )

    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]
    assert "job_id = 'job-123'" in fake_session.last_filter_kwargs["query"]


def test_count_landing_with_raw_job_id_partition_filters_within_hash_bucket(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "rec-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    count = client.count_landing(partition="job-123")

    assert count == 1
    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]
    assert fake_session.last_filter_kwargs["query"] == "job_id = 'job-123'"


def test_count_serving_uses_the_same_job_id_hash_collision_filter(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["serving_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "rec-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(serving_table="serving_test")))
    count = client.count_serving(partition="job-123")

    assert count == 1
    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]
    assert fake_session.last_filter_kwargs["query"] == "job_id = 'job-123'"


def test_query_data_can_query_named_serving_table(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["serving_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "session_id": "session-1",
            "created_at": 100,
            "id": "rec-1",
            "tags": None,
            "messages": '[{"role":"user","content":"hello"}]',
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(serving_table="serving_test")))
    result = client.query_data(
        filter_query="session_id = 'session-1'",
        partition="job-123",
        order_by="created_at",
        table="serving_test",
    )

    assert result == [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "session_id": "session-1",
            "created_at": 100,
            "id": "rec-1",
            "messages": '[{"role":"user","content":"hello"}]',
        }
    ]
    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]
    assert "job_id = 'job-123'" in fake_session.last_filter_kwargs["query"]
    assert fake_session.last_filter_kwargs["order_by"] == "created_at"


def test_keyword_search_defaults_to_search_text_and_rejects_nested_fields(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(serving_table="serving_test")))

    client.search("example")
    assert fake_session.last_filter_kwargs["query"] == "(search_text LIKE '%example%')"

    client.search("")
    assert fake_session.last_filter_kwargs["query"] == "id IS NOT NULL"

    with pytest.raises(ValueError, match="opaque JSON/list field 'chosen_trace'"):
        client.search("example", search_fields=["chosen_trace"])


def test_public_row_read_apis_can_deserialize_json_columns(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    bucket = stable_hash("job-json") % 128
    row = {
        "dataset_type": "RL",
        "job_id": "job-json",
        "session_id": "session-json",
        "created_at": 100,
        "id": "record-json",
        "search_text": "json payload",
        "messages": '[{"role":"user","content":null}]',
        "response": '{"role":"assistant","content":"answer"}',
        "chosen_trace": '[{"role":"assistant","content":"chosen"}]',
        "rejected_trace": None,
        "meta_json": '{"provider":"openai","optional":null}',
        "__partition": bucket,
    }
    fake_session.rows["landing_test"] = [dict(row)]
    fake_session.rows["serving_test"] = [dict(row)]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(
        GatewayConfig(tables=TableConfig(landing_table="landing_test", serving_table="serving_test"))
    )

    query_result = client.query_data(
        "job_id = 'job-json'",
        deserialize_json=True,
    )[0]
    pull_result = client.pull_data(
        "RL",
        where_sql="job_id = 'job-json'",
        deserialize_json=True,
    ).iloc[0]
    iter_result = list(
        client.iter_data_batches(
            "RL",
            where_sql="job_id = 'job-json'",
            deserialize_json=True,
        )
    )[0].iloc[0]
    search_result = client.search(
        "json",
        where_sql="job_id = 'job-json'",
        table="landing_test",
        deserialize_json=True,
    ).iloc[0]
    by_id_result = client.get_by_id(
        "record-json",
        deserialize_json=True,
    )
    max_result = client.get_max_created_at(
        "dataset_type = 'RL' AND job_id = 'job-json'",
        deserialize_json=True,
    )

    monkeypatch.setattr(client, "_list_existing_partitions_for_table", lambda table_name: [bucket])
    export_result = list(
        client.export_data_batches(
            filter_query="job_id = 'job-json'",
            batch_size=1,
            columns=["id", "messages", "meta_json"],
            deserialize_json=True,
        )
    )[0].iloc[0]

    for result in (
        query_result,
        pull_result,
        iter_result,
        search_result,
        by_id_result,
        max_result,
        export_result,
    ):
        assert result["messages"] == [{"role": "user", "content": None}]
        assert result["meta_json"] == {"provider": "openai", "optional": None}

    # deserialize_json changes only the presentation boundary; stored source rows stay strings.
    assert isinstance(fake_session.rows["landing_test"][0]["messages"], str)
    assert isinstance(fake_session.rows["serving_test"][0]["meta_json"], str)


def test_get_tags_distribution_uses_non_empty_filter(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["serving_test"] = [
        {"id": "serving-1", "tags": ["safe", "trainable"]},
        {"id": "serving-2", "tags": ["safe"]},
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(serving_table="serving_test")))

    assert client.get_tags_distribution() == {"safe": 2, "trainable": 1}
    assert fake_session.last_filter_kwargs["query"] == "id IS NOT NULL"


def test_get_by_id_defaults_to_serving_and_never_falls_back(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "landing-only",
            "tags": None,
        }
    ]
    fake_session.rows["serving_test"] = []
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(
        GatewayConfig(tables=TableConfig(landing_table="landing_test", serving_table="serving_test"))
    )

    assert client.get_by_id("landing-only") is None
    assert fake_session.last_filter_kwargs["table_name"] == "serving_test"

    record = client.get_by_id("landing-only", table="landing_test")
    assert record["id"] == "landing-only"
    assert fake_session.last_filter_kwargs["table_name"] == "landing_test"

    record_with_nulls = client.get_by_id(
        "landing-only",
        table="landing_test",
        exclude_none=False,
    )
    assert record_with_nulls["tags"] is None


def test_landing_index_maintenance_tracks_dirty_bucket_and_optimizes(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))
    client.ingest_landing(_make_landing_record())

    bucket = stable_hash("job-123") % 128
    assert client.get_dirty_landing_index_partitions() == [bucket]

    summary = client.maintain_landing_indexes()

    assert summary["partitions"] == [bucket]
    assert [item["column"] for item in summary["indexes_created"]] == [
        "id",
        "job_id",
        "session_id",
        "created_at",
        "is_terminal",
        "is_trainable",
    ]
    assert fake_session.optimized_partitions == [
        {
            "table_name": "landing_test",
            "partition": bucket,
            "kwargs": {
                "cleanup_older_than": None,
                "delete_unverified": False,
                "retrain": False,
            },
        }
    ]
    assert client.get_dirty_landing_index_partitions() == []


def test_get_max_created_at_prunes_landing_hash_partition(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "rec-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    client.get_max_created_at("dataset_type = 'RL' AND job_id = 'job-123'")

    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]
    assert fake_session.last_filter_kwargs["order_by"] == "created_at"


def test_search_landing_prunes_job_id_hash_partition(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "rec-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    client.search("", table="landing_test", dataset_type="RL", where_sql="job_id = 'job-123'", limit=10)

    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]


def test_delete_landing_prunes_job_id_hash_partition(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "rec-1",
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    client.delete_landing("job_id = 'job-123'")

    assert fake_session.last_filter_kwargs["partitions"] == [stable_hash("job-123") % 128]
    assert fake_session.last_delete_kwargs["partition"] == stable_hash("job-123") % 128


def test_update_landing_converts_job_id_partition_string_to_hash_bucket(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "rec-1",
            "is_terminal": False,
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    client.update_landing(
        filter_query="id = 'rec-1'",
        updates={"is_terminal": True},
        partition="job-123",
    )

    assert fake_session.last_update_kwargs["partition"] == stable_hash("job-123") % 128


def test_update_landing_adds_job_id_filter_when_partition_string_is_raw_job_id(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("job_id", "HASH", 128)
    fake_session.rows["landing_test"] = [
        {
            "dataset_type": "RL",
            "job_id": "job-123",
            "created_at": 100,
            "id": "rec-1",
            "is_terminal": False,
        }
    ]
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(GatewayConfig(tables=TableConfig(landing_table="landing_test")))

    client.update_landing(
        filter_query="id = 'rec-1'",
        updates={"is_terminal": True},
        partition="job-123",
    )

    assert fake_session.last_update_kwargs["partition"] == stable_hash("job-123") % 128
    assert "job_id = 'job-123'" in fake_session.last_update_kwargs["where"]


def test_pull_data_keeps_dt_partition_cond_for_dt_tables(monkeypatch):
    fake_session = FakeSession(attach_df_timing=False)
    fake_session.schema_table = _FakeSchemaTable("dt")
    monkeypatch.setattr(client_module.dldb, "connect", lambda db_uri, **kwargs: fake_session)

    client = WTGatewayClient(
        GatewayConfig(
            tables=TableConfig(landing_table="landing_test"),
            dldb_model="metrics",
        )
    )

    client.pull_data(
        "RL",
        where_sql="job_id = 'job-123' AND is_terminal = True",
        start_time=1773772800,
        end_time=1773859200,
        limit=100,
    )

    assert fake_session.last_filter_kwargs["partitions"] is None
    assert "dt >=" in fake_session.last_filter_kwargs["partition_cond"]


def test_env_config_manager_logs_timing_and_returns_summary(monkeypatch):
    summary = {
        "model": "metrics",
        "total_calls": 4,
        "total_errors": 0,
        "total_latency_seconds_sum": 0.2,
        "total_rows": 4,
        "total_bytes": 1024,
        "by_api": {"add": {"calls_total": 1}, "filter": {"calls_total": 1}},
    }
    fake_session = FakeSession(attach_df_timing=True, shutdown_result=summary)
    captured = _capture_logger(monkeypatch, env_manager_module)
    connect_kwargs = {}

    def fake_connect(db_uri, **kwargs):
        connect_kwargs["db_uri"] = db_uri
        connect_kwargs.update(kwargs)
        return fake_session

    monkeypatch.setattr(env_manager_module.dldb, "connect", fake_connect)
    monkeypatch.setenv("WT_SDK_ENV_CONFIG_DB_URI", "s3://test-env-config")

    manager = EnvConfigManager(
        table_name="evaluation_env_config",
        dldb_model="metrics",
        enable_dldb_timing_logs=True,
    )

    manager.save_config(
        {
            "env_name": "CartPole-v1",
            "env_id": "env-001",
            "job_id": "job-001",
            "group_id": "group-1",
            "env_params": {"gravity": 9.8},
            "image": "cartpole:latest",
            "finished": False,
        }
    )
    configs = manager.get_env_configs(limit=10, offset=0)
    manager.update_config("env-001", {"finished": True})
    manager.delete_config("env-001")
    returned = manager.close()

    assert connect_kwargs["model"] == "metrics"
    assert connect_kwargs["db_uri"] == "s3://test-env-config"
    assert len(configs) == 1
    assert returned == summary
    assert any("dldb_timing api=add" in message for message in captured)
    assert any("dldb_timing api=filter" in message for message in captured)
    assert any("dldb_timing api=update" in message for message in captured)
    assert any("dldb_timing api=delete" in message for message in captured)
    assert any("dldb_metrics_summary" in message for message in captured)
