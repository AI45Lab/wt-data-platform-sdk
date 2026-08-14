import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "delivery" / "count_dataset.py"
SPEC = importlib.util.spec_from_file_location(
    "count_dataset_script",
    SCRIPT_PATH,
)
count_script = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(count_script)


def test_parser_defaults_to_five_and_requires_positive_concurrency():
    parser = count_script.build_parser()

    assert parser.parse_args([]).table == "wind_tunnel_serving"
    assert parser.parse_args([]).concurrency == 5
    assert parser.parse_args([]).verbose is False
    assert parser.parse_args(["--table", "serving_test"]).table == "serving_test"
    assert parser.parse_args(["--concurrency", "2"]).concurrency == 2
    assert parser.parse_args(["--verbose"]).verbose is True
    for raw_value in ("0", "-1", "not-an-integer"):
        with pytest.raises(SystemExit):
            parser.parse_args(["--concurrency", raw_value])


def test_extract_datasets_deduplicates_sorts_and_reports_malformed_job_ids():
    job_ids = [
        "zeta#harness#model",
        "alpha#harness#one",
        " alpha #harness#two",
        "zeta#harness#other",
        "missing-separator",
        "#empty-dataset",
        None,
        42,
    ]

    datasets, invalid = count_script.extract_datasets(job_ids)

    assert datasets == ["alpha", "zeta"]
    assert invalid == ["missing-separator", "#empty-dataset", None, 42]


def test_dataset_filter_escapes_sql_and_like_metacharacters():
    dataset = "data'100%_done\\now"

    assert count_script.build_dataset_filter(dataset) == (
        "(job_id LIKE '%data''100\\%\\_done\\\\now%' ESCAPE '\\')"
    )


def test_run_query_data_uses_current_python_absolute_script_and_json_output(
    monkeypatch,
    tmp_path,
    capsys,
):
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        output_path = Path(command[command.index("--output") + 1])
        output_path.write_text(json.dumps({"filtered_rows": 17}), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="ignored", stderr="")

    monkeypatch.setattr(count_script.subprocess, "run", fake_run)
    output_path = tmp_path / "result.json"

    payload = count_script._run_query_data(
        ["--query", "id IS NOT NULL", "--count"],
        output_path,
        verbose=True,
    )

    assert payload == {"filtered_rows": 17}
    assert captured["command"] == [
        sys.executable,
        str(count_script.QUERY_SCRIPT),
        "--table",
        "wind_tunnel_serving",
        "--query",
        "id IS NOT NULL",
        "--count",
        "--output",
        str(output_path),
    ]
    assert captured["kwargs"] == {
        "cwd": count_script.REPOSITORY_ROOT,
        "capture_output": True,
        "text": True,
        "check": False,
    }
    verbose_output = capsys.readouterr()
    assert verbose_output.out == ""
    assert "Executing query:" in verbose_output.err
    assert "--table wind_tunnel_serving" in verbose_output.err
    assert "--query 'id IS NOT NULL' --count" in verbose_output.err


def test_run_query_data_reports_child_failure_without_parsing_stdout(
    monkeypatch,
    tmp_path,
):
    def fake_run(command, **kwargs):
        return subprocess.CompletedProcess(
            command,
            3,
            stdout="ordinary output",
            stderr="database unavailable",
        )

    monkeypatch.setattr(count_script.subprocess, "run", fake_run)

    with pytest.raises(count_script.QueryCommandError, match="database unavailable"):
        count_script._run_query_data(["--distinct", "job_id"], tmp_path / "missing.json")


def test_discover_job_ids_requires_structured_values(monkeypatch, tmp_path):
    monkeypatch.setattr(
        count_script,
        "_run_query_data",
        lambda arguments, output_path, **kwargs: {
            "values": [{"job_id": "a#one"}, {"job_id": None}]
        },
    )
    assert count_script._discover_job_ids(tmp_path / "distinct.json") == ["a#one", None]

    monkeypatch.setattr(
        count_script,
        "_run_query_data",
        lambda arguments, output_path, **kwargs: {"values": ["not-an-object"]},
    )
    with pytest.raises(count_script.QueryCommandError, match="invalid value at index 0"):
        count_script._discover_job_ids(tmp_path / "invalid.json")


def test_query_dataset_count_rejects_missing_or_invalid_filtered_rows(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        count_script,
        "_run_query_data",
        lambda arguments, output_path, **kwargs: {"filtered_rows": None},
    )

    with pytest.raises(count_script.QueryCommandError, match="invalid filtered_rows"):
        count_script._query_dataset_count("alpha", tmp_path / "count.json")


def test_count_datasets_honors_worker_limit_and_preserves_failures(
    monkeypatch,
    tmp_path,
):
    real_executor = count_script.ThreadPoolExecutor
    worker_limits = []
    calls = []

    def recording_executor(*, max_workers):
        worker_limits.append(max_workers)
        return real_executor(max_workers=max_workers)

    def fake_query_count(dataset, output_path, *, table, verbose):
        calls.append((dataset, output_path.name, table, verbose))
        if dataset == "broken":
            raise count_script.QueryCommandError("read failed")
        return len(dataset)

    monkeypatch.setattr(count_script, "ThreadPoolExecutor", recording_executor)
    monkeypatch.setattr(count_script, "_query_dataset_count", fake_query_count)

    counts, errors = count_script.count_datasets(
        ["alpha", "broken", "z"],
        tmp_path,
        concurrency=2,
        table="serving_test",
        verbose=True,
    )

    assert worker_limits == [2]
    assert counts == {"alpha": 5, "z": 1}
    assert errors == {"broken": "read failed"}
    assert sorted(calls) == [
        ("alpha", "count-00000.json", "serving_test", True),
        ("broken", "count-00001.json", "serving_test", True),
        ("z", "count-00002.json", "serving_test", True),
    ]


def test_main_prints_sorted_table_last_and_warns_about_invalid_values(
    monkeypatch,
    capsys,
):
    captured = {}
    monkeypatch.setattr(
        count_script,
        "_discover_job_ids",
        lambda output_path, **kwargs: ["zeta#one", "alpha#two", "bad-job-id", None],
    )

    def fake_count(datasets, output_dir, concurrency, *, table, verbose):
        captured["datasets"] = datasets
        captured["table"] = table
        captured["concurrency"] = concurrency
        captured["verbose"] = verbose
        return {"alpha": 8, "zeta": 13}, {}

    monkeypatch.setattr(count_script, "count_datasets", fake_count)

    assert count_script.main([]) == 0
    output = capsys.readouterr()

    assert captured == {
        "datasets": ["alpha", "zeta"],
        "table": "wind_tunnel_serving",
        "concurrency": 5,
        "verbose": False,
    }
    assert output.out == (
        "dataset | row_count\n"
        "--------+----------\n"
        "alpha   |         8\n"
        "zeta    |        13\n"
    )
    assert "skipped 2 malformed job_id" in output.err
    assert "may overlap" in output.err


def test_main_prints_error_rows_and_returns_nonzero_after_partial_failure(
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(
        count_script,
        "_discover_job_ids",
        lambda output_path, **kwargs: ["alpha#one", "broken#two"],
    )

    def fake_count(datasets, output_dir, concurrency, *, table, verbose):
        assert table == "serving_test"
        assert concurrency == 2
        assert verbose is True
        return {"alpha": 3}, {"broken": "query timed out"}

    monkeypatch.setattr(count_script, "count_datasets", fake_count)

    assert count_script.main(["--table", "serving_test", "--concurrency", "2", "--verbose"]) == 1
    output = capsys.readouterr()

    assert output.out.endswith("alpha   |         3\n" "broken  |     ERROR\n")
    assert "dataset 'broken': query timed out" in output.err


def test_main_stops_when_distinct_query_fails(monkeypatch, capsys):
    def fail_discovery(output_path, **kwargs):
        raise count_script.QueryCommandError("access denied")

    monkeypatch.setattr(count_script, "_discover_job_ids", fail_discovery)

    assert count_script.main([]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert "cannot discover job_id values: access denied" in output.err


def test_empty_result_still_prints_table_headers():
    assert count_script.format_result_table([], {}, {}) == (
        "dataset | row_count\n" "--------+----------"
    )
