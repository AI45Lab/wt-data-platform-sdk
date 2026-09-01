from types import SimpleNamespace

from scripts.inspect import show_env_config_status as script


class FakeLanceTable:
    def stats(self):
        return {
            "total_bytes": 4096,
            "num_rows": 25,
            "num_indices": 5,
            "fragment_stats": {
                "num_fragments": 3,
                "num_small_fragments": 2,
                "lengths": {
                    "min": 2,
                    "max": 15,
                    "mean": 8,
                    "p25": 4,
                    "p50": 8,
                    "p75": 12,
                    "p99": 15,
                },
            },
        }


class FakeSession:
    def __init__(self, *, table_name=script.TEST_ENV_CONFIG_TABLE):
        self.table_name = table_name
        self.shutdown_called = False
        self.schema_table = SimpleNamespace(
            get=lambda name: (
                SimpleNamespace(partition_column="", partition_type="")
                if name == self.table_name
                else None
            )
        )
        self.table = SimpleNamespace(table=FakeLanceTable())

    def list_indices(self, table_name):
        assert table_name == self.table_name
        return [
            SimpleNamespace(
                name=f"{column}_idx",
                index_type="BTree",
                columns=[column],
                num_indexed_rows=25,
                num_unindexed_rows=0,
                size_bytes=100,
                num_segments=1,
                index_version=0,
            )
            for column in script.SCALAR_INDEX_COLUMNS
        ]

    def list_index_coverage(self, table_name):
        assert table_name == self.table_name
        return [
            SimpleNamespace(
                index_name=f"{column}_idx",
                num_indexed_rows=25,
                num_unindexed_rows=0,
                fully_indexed=True,
            )
            for column in script.SCALAR_INDEX_COLUMNS
        ]

    def _get_table(self, table_name):
        assert table_name == self.table_name
        return self.table

    def shutdown(self):
        self.shutdown_called = True


def test_inspect_reports_fragments_and_complete_expected_indexes():
    session = FakeSession()

    report = script.inspect_env_config_status(session)

    assert report["table_name"] == script.TEST_ENV_CONFIG_TABLE
    assert report["read_only"] is True
    assert report["state"] == "fragmented"
    assert report["stats"]["fragment_stats"]["num_fragments"] == 3
    assert report["missing_indexes"] == []
    assert report["unexpected_indexes"] == []
    assert report["index_tails"] == []


def test_inspect_reports_missing_index_and_unindexed_tail():
    session = FakeSession()
    session.list_indices = lambda table_name: [
        item
        for item in FakeSession.list_indices(session, table_name)
        if item.name != "job_id_idx"
    ]
    session.list_index_coverage = lambda table_name: [
        SimpleNamespace(
            index_name="env_id_idx",
            num_indexed_rows=20,
            num_unindexed_rows=5,
            fully_indexed=False,
        )
    ]

    report = script.inspect_env_config_status(session)

    assert report["state"] == "fragmented,missing_indexes,unindexed_tail"
    assert report["missing_indexes"] == ["job_id_idx"]
    assert report["index_tails"] == report["index_coverage"]


def test_main_selects_production_table_and_closes_session(monkeypatch, capsys):
    session = FakeSession(table_name="evaluation_env_config")
    monkeypatch.setattr(script.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(
        "sys.argv",
        ["show_env_config_status.py", "--profile", "production", "--json"],
    )

    result = script.main()

    assert result == 0
    assert session.shutdown_called is True
    assert '"table_name": "evaluation_env_config"' in capsys.readouterr().out


def test_inspect_rejects_partitioned_table():
    session = FakeSession()
    session.schema_table = SimpleNamespace(
        get=lambda name: SimpleNamespace(partition_column="job_id")
    )

    try:
        script.inspect_env_config_status(session)
    except ValueError as exc:
        assert "show_partition_status.py" in str(exc)
    else:
        raise AssertionError("partitioned table should be rejected")
