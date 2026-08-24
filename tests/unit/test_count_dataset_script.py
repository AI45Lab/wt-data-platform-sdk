import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "delivery" / "count_dataset.py"
SPEC = importlib.util.spec_from_file_location("count_datasets_script", SCRIPT_PATH)
count_script = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = count_script
SPEC.loader.exec_module(count_script)


class _FakeClient:
    def __init__(self, batches, *, table="wind_tunnel_serving"):
        self.config = SimpleNamespace(tables=SimpleNamespace(serving_table=table))
        self.batches = batches
        self.calls = []

    def export_data_batches(self, **kwargs):
        self.calls.append(kwargs)
        yield from self.batches


def test_parser_defaults_and_supported_tables():
    parser = count_script.build_parser()

    defaults = parser.parse_args([])
    assert defaults.table == "wind_tunnel_serving"
    assert defaults.filter_query == "id IS NOT NULL"
    assert parser.parse_args(["--table", "serving_test"]).table == "serving_test"

    with pytest.raises(SystemExit):
        parser.parse_args(["--table", "wind_tunnel_landing"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--batch-size", "10"])


def test_build_config_pins_production_and_allows_explicit_test(monkeypatch):
    monkeypatch.setenv("WT_SDK_LANDING_TABLE", "unexpected_landing")
    monkeypatch.setenv("WT_SDK_SERVING_TABLE", "unexpected_serving")

    production = count_script.build_config()
    test = count_script.build_config("serving_test")

    assert production.tables.profile == "production"
    assert production.tables.landing_table == "wind_tunnel_landing"
    assert production.tables.serving_table == "wind_tunnel_serving"
    assert test.tables.profile == "test"
    assert test.tables.landing_table == "landing_test"
    assert test.tables.serving_table == "serving_test"


def test_dataset_comes_from_first_job_id_component_not_contains_matching():
    assert count_script.dataset_from_job_id("foo#harness#model#run") == "foo"
    assert count_script.dataset_from_job_id(" foo #harness#model#run") == "foo"
    assert count_script.dataset_from_job_id("foobar#harness#model#run") == "foobar"
    assert count_script.dataset_from_job_id("bar#foo#model#run") == "bar"

    assert count_script.dataset_from_job_id("missing-separator") is None
    assert count_script.dataset_from_job_id("#empty") is None
    assert count_script.dataset_from_job_id(None) is None
    assert count_script.dataset_from_job_id("bad\nname#harness") is None


def test_count_reads_one_fixed_manifest_in_batches_and_aggregates_locally():
    client = _FakeClient(
        [
            pd.DataFrame(
                {
                    "job_id": [
                        "foo#harness#model#one",
                        "foobar#harness#model#two",
                        "foo#harness#model#three",
                    ]
                }
            ),
            pd.DataFrame(
                {
                    "job_id": [
                        "bar#foo#model#four",
                        "missing-separator",
                        None,
                    ]
                }
            ),
        ]
    )

    result = count_script.count_datasets(
        client,
        filter_query="serving_updated_at >= 100",
        batch_size=2,
    )

    assert result.counts == {"bar": 1, "foo": 2, "foobar": 1}
    assert result.total_rows == 6
    assert result.counted_rows == 4
    assert result.invalid_rows == 2
    assert result.invalid_examples == ("missing-separator", None)
    assert client.calls == [
        {
            "filter_query": "serving_updated_at >= 100",
            "batch_size": 2,
            "columns": ["job_id"],
            "deserialize_json": False,
        }
    ]


def test_count_rejects_unknown_table_empty_filter_and_invalid_batch_size():
    with pytest.raises(ValueError, match="must be one of"):
        count_script.count_datasets(_FakeClient([], table="wind_tunnel_landing"))
    with pytest.raises(ValueError, match="filter_query must be non-empty"):
        count_script.count_datasets(_FakeClient([]), filter_query="  ")
    for value in (True, 0, -1):
        with pytest.raises(ValueError, match="positive integer"):
            count_script.count_datasets(_FakeClient([]), batch_size=value)


def test_count_requires_job_id_column():
    with pytest.raises(RuntimeError, match="job_id column"):
        count_script.count_datasets(_FakeClient([pd.DataFrame({"id": ["one"]})]))


def test_format_result_and_invalid_warning(capsys):
    result = count_script.DatasetCountResult(
        counts={"foo": 2, "foobar": 1},
        total_rows=4,
        invalid_rows=1,
        invalid_examples=("bad-id",),
    )

    rendered = count_script.format_result(result)
    count_script.print_invalid_warning(result)
    output = capsys.readouterr()

    assert rendered == (
        "dataset | row_count\n"
        "--------+----------\n"
        "foo     |         2\n"
        "foobar  |         1\n"
        "\n"
        "Rows in fixed manifest: 4\n"
        "Rows counted: 3\n"
        "Rows skipped: 1"
    )
    assert "skipped 1 malformed job_id" in output.err
    assert "'bad-id'" in output.err


def test_empty_result_prints_headers_and_zero_summary():
    result = count_script.DatasetCountResult({}, 0, 0, ())

    assert count_script.format_result(result) == (
        "dataset | row_count\n"
        "--------+----------\n"
        "\n"
        "Rows in fixed manifest: 0\n"
        "Rows counted: 0\n"
        "Rows skipped: 0"
    )
