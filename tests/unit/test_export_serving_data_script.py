import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "delivery" / "export_serving_data.py"
SPEC = importlib.util.spec_from_file_location("export_serving_data_script", SCRIPT_PATH)
export_script = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(export_script)


class _FakeClient:
    def __init__(self, batches, *, error=None, table="wind_tunnel_serving"):
        self.config = SimpleNamespace(tables=SimpleNamespace(serving_table=table))
        self.batches = batches
        self.error = error
        self.calls = []

    def export_data_batches(self, **kwargs):
        self.calls.append(kwargs)
        for batch in self.batches:
            yield batch
        if self.error is not None:
            raise self.error


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_default_columns_are_explicit_and_exclude_search_text():
    columns = export_script.parse_columns(None)

    assert columns == list(export_script.DEFAULT_DELIVERY_COLUMNS)
    assert set(columns) == set(export_script.SERVING_SCHEMA.names) - {"search_text"}
    assert "id" in columns
    assert "serving_updated_at" in columns
    assert "search_text" not in columns


def test_explicit_columns_may_include_search_text_and_are_validated():
    assert export_script.parse_columns("id, search_text") == ["id", "search_text"]

    with pytest.raises(ValueError, match="unknown serving columns"):
        export_script.parse_columns("id,not_a_column")
    with pytest.raises(ValueError, match="duplicate"):
        export_script.parse_columns("id,id")


def test_build_config_defaults_to_production_and_ignores_table_name_environment_overrides(
    monkeypatch,
):
    monkeypatch.setenv("WT_SDK_LANDING_TABLE", "unexpected_landing")
    monkeypatch.setenv("WT_SDK_SERVING_TABLE", "unexpected_serving")

    config = export_script.build_config()

    assert config.tables.profile == "production"
    assert config.tables.landing_table == "wind_tunnel_landing"
    assert config.tables.serving_table == "wind_tunnel_serving"


def test_build_config_allows_explicit_serving_test():
    config = export_script.build_config("serving_test")

    assert config.tables.profile == "test"
    assert config.tables.landing_table == "landing_test"
    assert config.tables.serving_table == "serving_test"


def test_export_writes_jsonl_shards_manifest_and_success_marker(tmp_path):
    client = _FakeClient(
        [
            pd.DataFrame(
                [
                    {"id": "one", "meta_json": {"nested": None}},
                    {"id": "two", "meta_json": [1, 2]},
                    {"id": "three", "meta_json": None},
                ]
            ),
            pd.DataFrame(
                [
                    {"id": "four", "meta_json": {"ok": True}},
                    {"id": "five", "meta_json": None},
                ]
            ),
        ]
    )

    result = export_script.export_serving_data(
        client,
        filter_query="dataset_type = 'RL'",
        columns=["id", "meta_json"],
        output_dir=tmp_path,
        rows_per_file=2,
        export_id="export-fixed",
    )

    assert result == (tmp_path / "export-fixed").resolve()
    assert not (tmp_path / ".export-fixed.partial").exists()
    assert (result / "_SUCCESS").is_file()
    assert [path.name for path in sorted(result.glob("part-*.jsonl"))] == [
        "part-00000.jsonl",
        "part-00001.jsonl",
        "part-00002.jsonl",
    ]
    assert _read_jsonl(result / "part-00000.jsonl") == [
        {"id": "one", "meta_json": {"nested": None}},
        {"id": "two", "meta_json": [1, 2]},
    ]
    assert _read_jsonl(result / "part-00002.jsonl") == [
        {"id": "five", "meta_json": None}
    ]

    manifest = json.loads((result / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["table"] == "wind_tunnel_serving"
    assert manifest["filter"] == "dataset_type = 'RL'"
    assert manifest["columns"] == ["id", "meta_json"]
    assert manifest["format"] == "jsonl"
    assert manifest["rows_per_file"] == 2
    assert manifest["row_count"] == 5
    assert [part["rows"] for part in manifest["files"]] == [2, 2, 1]
    assert all(part["bytes"] > 0 for part in manifest["files"])
    assert client.calls == [
        {
            "filter_query": "dataset_type = 'RL'",
            "batch_size": 2,
            "columns": ["id", "meta_json"],
            "deserialize_json": True,
        }
    ]


def test_empty_export_publishes_manifest_without_part_files(tmp_path):
    result = export_script.export_serving_data(
        _FakeClient([]),
        filter_query="id IS NOT NULL",
        columns=["id"],
        output_dir=tmp_path,
        export_id="export-empty",
    )

    manifest = json.loads((result / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["row_count"] == 0
    assert manifest["files"] == []
    assert list(result.glob("part-*.jsonl")) == []
    assert (result / "_SUCCESS").exists()


def test_export_escapes_unicode_line_separators_without_changing_parsed_values(tmp_path):
    original = "before\u2028middle\u2029after"
    result = export_script.export_serving_data(
        _FakeClient([pd.DataFrame([{"id": "unicode-separators", "meta_json": original}])]),
        filter_query="id = 'unicode-separators'",
        columns=["id", "meta_json"],
        output_dir=tmp_path,
        export_id="export-unicode-separators",
    )

    raw_line = (result / "part-00000.jsonl").read_text(encoding="utf-8")

    assert "\u2028" not in raw_line
    assert "\u2029" not in raw_line
    assert "\\u2028" in raw_line
    assert "\\u2029" in raw_line
    assert json.loads(raw_line)["meta_json"] == original


def test_failed_export_keeps_partial_directory_without_success_marker(tmp_path):
    client = _FakeClient(
        [pd.DataFrame([{"id": "one"}, {"id": "two"}])],
        error=RuntimeError("source read failed"),
    )

    with pytest.raises(export_script.ExportFailed, match="partial data kept") as raised:
        export_script.export_serving_data(
            client,
            filter_query="id IS NOT NULL",
            columns=["id"],
            output_dir=tmp_path,
            rows_per_file=1,
            export_id="export-failed",
        )

    partial = tmp_path / ".export-failed.partial"
    assert raised.value.partial_dir == partial.resolve()
    assert partial.is_dir()
    assert not (tmp_path / "export-failed").exists()
    assert not (partial / "_SUCCESS").exists()
    assert not (partial / "manifest.json").exists()
    assert [path.name for path in sorted(partial.glob("part-*.jsonl"))] == [
        "part-00000.jsonl",
        "part-00001.jsonl",
    ]


def test_export_allows_serving_test(tmp_path):
    result = export_script.export_serving_data(
        _FakeClient([], table="serving_test"),
        filter_query="id IS NOT NULL",
        columns=["id"],
        output_dir=tmp_path,
        export_id="export-test-table",
    )

    manifest = json.loads((result / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["table"] == "serving_test"


def test_export_refuses_unknown_table(tmp_path):
    client = _FakeClient([], table="unexpected_serving")

    with pytest.raises(ValueError, match="must be one of"):
        export_script.export_serving_data(
            client,
            filter_query="id IS NOT NULL",
            columns=["id"],
            output_dir=tmp_path,
        )
