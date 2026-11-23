from datetime import timedelta

import pytest

from scripts.ops import maintain_table_indexes as script
from scripts.ops.maintain_table_indexes import resolve_table_role


def _install_fake_client(monkeypatch, summary):
    calls = {}

    class FakeClient:
        def __init__(self, config):
            calls["config"] = config

        def maintain_table_indexes(self, table_name, **kwargs):
            calls["table_name"] = table_name
            calls["kwargs"] = kwargs
            return summary

        def close(self):
            calls["closed"] = True

    monkeypatch.setattr(script, "WTGatewayClient", FakeClient)
    return calls


@pytest.mark.parametrize(
    ("table_name", "expected_role"),
    [
        ("wt_landing", "landing"),
        ("landing_test", "landing"),
        ("wt_serving", "serving"),
        ("serving_test", "serving"),
    ],
)
def test_resolve_table_role_for_standard_tables(table_name, expected_role):
    assert resolve_table_role(table_name) == expected_role


def test_resolve_table_role_rejects_custom_table():
    with pytest.raises(ValueError, match="Unsupported table"):
        resolve_table_role("custom_trajectory_table")


def test_cli_cleanup_only_forwards_partition_scope_and_retention(monkeypatch):
    calls = _install_fake_client(monkeypatch, {"errors": []})
    monkeypatch.setattr(
        script.sys,
        "argv",
        [
            "maintain_table_indexes.py",
            "--table",
            "wt_serving",
            "--partition",
            "7",
            "--cleanup-only",
            "--cleanup-older-than-hours",
            "3.5",
        ],
    )

    assert script.main() == 0
    assert calls["table_name"] == "wt_serving"
    assert calls["config"].tables.db_uri == script.db_uri_for_table("wt_serving")
    assert calls["kwargs"] == {
        "partitions": [7],
        "all_partitions": False,
        "columns": None,
        "create_missing": True,
        "optimize": True,
        "cleanup_only": True,
        "cleanup_older_than": timedelta(hours=3.5),
    }
    assert calls["closed"] is True


@pytest.mark.parametrize(
    "conflicting_args",
    [
        ["--columns", "id"],
        ["--no-create-missing"],
        ["--no-optimize"],
    ],
)
def test_cli_cleanup_only_rejects_index_and_optimize_controls(
    monkeypatch, capsys, conflicting_args
):
    monkeypatch.setattr(
        script.sys,
        "argv",
        [
            "maintain_table_indexes.py",
            "--table",
            "wt_landing",
            "--partition",
            "job-123",
            "--cleanup-only",
            *conflicting_args,
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        script.main()

    assert exc_info.value.code == 2
    assert "--cleanup-only cannot be combined with" in capsys.readouterr().err


def test_cli_cleanup_only_still_requires_partition_scope(monkeypatch, capsys):
    monkeypatch.setattr(
        script.sys,
        "argv",
        ["maintain_table_indexes.py", "--table", "wt_landing", "--cleanup-only"],
    )

    with pytest.raises(SystemExit) as exc_info:
        script.main()

    assert exc_info.value.code == 2
    assert "provide --all-partitions or at least one --partition" in capsys.readouterr().err


def test_cli_cleanup_only_returns_nonzero_for_partition_errors(monkeypatch):
    calls = _install_fake_client(
        monkeypatch,
        {"errors": [{"partition": 7, "action": "cleanup_old_versions"}]},
    )
    monkeypatch.setattr(
        script.sys,
        "argv",
        [
            "maintain_table_indexes.py",
            "--table",
            "wt_landing",
            "--all-partitions",
            "--cleanup-only",
        ],
    )

    assert script.main() == 1
    assert calls["kwargs"]["all_partitions"] is True
    assert calls["kwargs"]["partitions"] is None
    assert calls["closed"] is True
