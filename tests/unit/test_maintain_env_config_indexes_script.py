from datetime import timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest

from scripts.ops import maintain_env_config_indexes as script


class FakePartitionedTable:
    def __init__(self, partitions):
        self._partitions = list(partitions)

    def list_partitions(self):
        return list(self._partitions)


class FakeSession:
    def __init__(
        self,
        existing_indexes,
        table_name=script.TEST_ENV_CONFIG_TABLE,
        partition_type="HASH",
        partition_column="job_id",
        partitions=(3, 7),
    ):
        self.table_name = table_name
        self.existing_indexes = list(existing_indexes)
        self.created = []
        self.optimize_calls = []
        self.cleanup_calls = []
        self.list_indices_calls = []
        self.coverage_calls = []
        self.tables = {}
        self._partition_type = partition_type
        self._partition_column = partition_column
        self._partitions = list(partitions)
        self.schema_table = SimpleNamespace(
            get=lambda table_name: SimpleNamespace(
                partition_column=self._partition_column,
                partition_type=self._partition_type,
            )
        )
        self.db_conn = object()

    def _get_table(self, table_name, partition):
        assert table_name == self.table_name
        return FakePartitionedTable(self._partitions)

    def get_schema(self, table_name):
        assert table_name == self.table_name
        return script.EVALUATION_ENV_SCHEMA

    def list_indices(self, table_name, partition=None):
        assert table_name == self.table_name
        self.list_indices_calls.append(partition)
        return [SimpleNamespace(name=name) for name in self.existing_indexes]

    def create_scalar_index(self, table_name, column, *, partition=None, index_type):
        assert table_name == self.table_name
        self.created.append((column, index_type, partition))
        self.existing_indexes.append(f"{column}_idx")

    def optimize(self, table_name, partition=None, cleanup_older_than=None):
        self.optimize_calls.append((table_name, partition))
        self.cleanup_older_than = cleanup_older_than

    def cleanup_old_versions(
        self,
        table_name,
        partition=None,
        cleanup_older_than=None,
        delete_unverified=False,
    ):
        self.cleanup_calls.append(
            {
                "table_name": table_name,
                "partition": partition,
                "cleanup_older_than": cleanup_older_than,
                "delete_unverified": delete_unverified,
            }
        )

    def list_index_coverage(self, table_name, partition=None):
        assert table_name == self.table_name
        self.coverage_calls.append(partition)
        return [
            SimpleNamespace(
                index_name=name,
                num_indexed_rows=10,
                num_unindexed_rows=0,
                fully_indexed=True,
            )
            for name in self.existing_indexes
        ]

    def shutdown(self):
        pass


def test_maintain_creates_missing_indexes_per_hash_bucket():
    session = FakeSession(
        ["env_name_idx", "env_id_idx", "group_id_idx", "finished_idx"]
    )
    summary = script.maintain_env_config_indexes(session)

    assert session.created == [
        ("job_id", "BTREE", 3),
        ("is_successed", "BTREE", 3),
        ("job_id", "BTREE", 7),
        ("is_successed", "BTREE", 7),
    ]
    assert session.optimize_calls == [
        (script.TEST_ENV_CONFIG_TABLE, 3),
        (script.TEST_ENV_CONFIG_TABLE, 7),
    ]
    assert summary["table_name"] == script.TEST_ENV_CONFIG_TABLE
    assert summary["partition_type"] == "HASH"
    assert summary["targets"] == ["3", "7"]
    assert summary["indexes_created"] == [
        {
            "partition": 3,
            "column": "job_id",
            "index_name": "job_id_idx",
            "index_type": "BTREE",
        },
        {
            "partition": 3,
            "column": "is_successed",
            "index_name": "is_successed_idx",
            "index_type": "BTREE",
        },
        {
            "partition": 7,
            "column": "job_id",
            "index_name": "job_id_idx",
            "index_type": "BTREE",
        },
        {
            "partition": 7,
            "column": "is_successed",
            "index_name": "is_successed_idx",
            "index_type": "BTREE",
        },
    ]
    assert summary["optimized"] == [3, 7]
    assert summary["cleanup_only"] is False
    assert summary["cleaned_up"] == []
    assert session.cleanup_calls == []
    assert {row["partition"] for row in summary["coverage"]} == {3, 7}
    assert summary["errors"] == []


def test_dry_run_reports_missing_per_bucket_without_writes():
    session = FakeSession([])

    summary = script.maintain_env_config_indexes(session, dry_run=True)

    assert session.created == []
    assert session.optimize_calls == []
    assert summary["missing_indexes_before"] == {
        "3": summary["expected_indexes"],
        "7": summary["expected_indexes"],
    }
    assert summary["coverage"] == []


def test_cleanup_only_cleans_each_bucket_without_index_or_optimize_work():
    session = FakeSession([])
    retention = timedelta(hours=6)

    summary = script.maintain_env_config_indexes(
        session,
        cleanup_only=True,
        cleanup_older_than=retention,
    )

    assert session.created == []
    assert session.optimize_calls == []
    assert session.list_indices_calls == []
    assert session.coverage_calls == []
    assert session.cleanup_calls == [
        {
            "table_name": script.TEST_ENV_CONFIG_TABLE,
            "partition": partition,
            "cleanup_older_than": retention,
            "delete_unverified": False,
        }
        for partition in (3, 7)
    ]
    assert summary["cleanup_only"] is True
    assert summary["cleaned_up"] == [3, 7]
    assert summary["optimize_requested"] is False
    assert summary["optimized"] == []
    assert summary["coverage"] == []
    assert summary["errors"] == []


def test_cleanup_only_dry_run_reports_targets_without_writes_or_index_reads():
    session = FakeSession([])

    summary = script.maintain_env_config_indexes(
        session,
        cleanup_only=True,
        dry_run=True,
    )

    assert summary["targets"] == ["3", "7"]
    assert summary["dry_run"] is True
    assert summary["cleaned_up"] == []
    assert session.created == []
    assert session.optimize_calls == []
    assert session.cleanup_calls == []
    assert session.list_indices_calls == []
    assert session.coverage_calls == []


def test_cleanup_only_legacy_unpartitioned_table_uses_whole_table_path():
    session = FakeSession([], partition_type="", partition_column="")

    summary = script.maintain_env_config_indexes(session, cleanup_only=True)

    assert summary["targets"] == ["None"]
    assert summary["cleaned_up"] == [None]
    assert session.cleanup_calls == [
        {
            "table_name": script.TEST_ENV_CONFIG_TABLE,
            "partition": None,
            "cleanup_older_than": None,
            "delete_unverified": False,
        }
    ]
    assert session.optimize_calls == []


def test_cleanup_only_records_errors_per_bucket_and_continues():
    session = FakeSession([])
    original_cleanup = session.cleanup_old_versions

    def cleanup_old_versions(table_name, partition=None, **kwargs):
        if partition == 3:
            raise RuntimeError("cleanup failed")
        return original_cleanup(table_name, partition=partition, **kwargs)

    session.cleanup_old_versions = cleanup_old_versions

    summary = script.maintain_env_config_indexes(session, cleanup_only=True)

    assert summary["cleaned_up"] == [7]
    assert summary["errors"] == [
        {
            "action": "cleanup_old_versions",
            "partition": 3,
            "error": "cleanup failed",
        }
    ]


def test_legacy_unpartitioned_table_keeps_whole_table_path():
    session = FakeSession(
        ["env_name_idx", "env_id_idx", "group_id_idx", "finished_idx"],
        partition_type="",
        partition_column="",
    )
    summary = script.maintain_env_config_indexes(session)

    assert session.created == [
        ("job_id", "BTREE", None),
        ("is_successed", "BTREE", None),
    ]
    assert session.optimize_calls == [(script.TEST_ENV_CONFIG_TABLE, None)]
    assert summary["partition_type"] is None
    assert summary["targets"] == ["None"]
    assert summary["optimized"] == [None]


def test_production_table_can_be_selected_explicitly():
    production_table = "wt_env_config"
    session = FakeSession([], table_name=production_table)
    summary = script.maintain_env_config_indexes(
        session,
        table_name=production_table,
        dry_run=True,
    )

    assert summary["table_name"] == production_table


def test_cli_cleanup_only_forwards_retention(monkeypatch):
    session = FakeSession([])
    monkeypatch.setattr(script.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(
        script.sys,
        "argv",
        [
            "maintain_env_config_indexes.py",
            "--cleanup-only",
            "--cleanup-older-than-hours",
            "2.5",
        ],
    )

    assert script.main() == 0
    assert [call["cleanup_older_than"] for call in session.cleanup_calls] == [
        timedelta(hours=2.5),
        timedelta(hours=2.5),
    ]
    assert session.created == []
    assert session.optimize_calls == []


@pytest.mark.parametrize("conflicting_arg", ["--no-create-missing", "--no-optimize"])
def test_cli_cleanup_only_rejects_index_and_optimize_controls(
    monkeypatch, capsys, conflicting_arg
):
    monkeypatch.setattr(
        script.sys,
        "argv",
        ["maintain_env_config_indexes.py", "--cleanup-only", conflicting_arg],
    )

    with pytest.raises(SystemExit) as exc_info:
        script.main()

    assert exc_info.value.code == 2
    assert "--cleanup-only cannot be combined with" in capsys.readouterr().err


def test_cli_uses_shared_profile_for_env_table(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "production")
    session = FakeSession([], table_name=script.DEFAULT_ENV_CONFIG_TABLE)
    monkeypatch.setattr(script.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(
        script.sys,
        "argv",
        ["maintain_env_config_indexes.py", "--dry-run"],
    )

    assert script.main() == 0


def test_cli_explicit_table_overrides_shared_profile(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "test")
    session = FakeSession([], table_name=script.DEFAULT_ENV_CONFIG_TABLE)
    monkeypatch.setattr(script.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(
        script.sys,
        "argv",
        [
            "maintain_env_config_indexes.py",
            "--table",
            "wt_env_config",
            "--dry-run",
        ],
    )

    assert script.main() == 0


def test_schema_mismatch_stops_before_index_mutation():
    session = FakeSession([])
    session.get_schema = lambda table_name: pa.schema([pa.field("id", pa.int64())])

    try:
        script.maintain_env_config_indexes(session)
    except ValueError as exc:
        assert "missing schema fields" in str(exc)
    else:
        raise AssertionError("schema mismatch should fail")

    assert session.created == []
    assert session.optimize_calls == []
