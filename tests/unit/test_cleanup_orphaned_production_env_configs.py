import pandas as pd
import pytest

from scripts.ops import cleanup_orphaned_production_env_configs as script


class FakeLandingClient:
    def list_table_partitions(self, *, table):
        assert table == script.DEFAULT_LANDING_TABLE
        return [3, 7]

    def query_data(self, **kwargs):
        assert kwargs["table"] == script.DEFAULT_LANDING_TABLE
        assert kwargs["columns"] == ["job_id"]
        assert kwargs["checkout_latest"] is True
        return {
            3: [{"job_id": "job-a"}, {"job_id": None}],
            7: [{"job_id": " job-b "}, {"job_id": ""}],
        }[kwargs["partition"]]


class FakeEnvManager:
    def __init__(self):
        self.deleted = []

    def _delete_where(self, predicate):
        self.deleted.append(predicate)


def test_collects_job_ids_per_existing_bucket_and_ignores_blank_values():
    assert script.collect_production_landing_job_ids(FakeLandingClient()) == {"job-a", "job-b"}


def test_find_orphan_rows_preserves_blank_job_ids_for_manual_review():
    rows = pd.DataFrame(
        [
            {"id": 1, "job_id": "job-a", "env_id": "a", "env_name": "A"},
            {"id": 2, "job_id": "stale", "env_id": "s", "env_name": "S"},
            {"id": 3, "job_id": None, "env_id": "n", "env_name": "N"},
            {"id": 4, "job_id": "   ", "env_id": "b", "env_name": "B"},
        ]
    )

    orphan_rows, blank_count = script.find_orphan_rows(rows, {"job-a"})

    assert orphan_rows["id"].tolist() == [2]
    assert blank_count == 2


def test_delete_env_rows_by_id_batches_numeric_predicates():
    manager = FakeEnvManager()

    deleted = script.delete_env_rows_by_id(manager, [4, 9, 12], batch_size=2)

    assert deleted == 3
    assert manager.deleted == ["id IN (4, 9)", "id IN (12)"]


def test_empty_landing_source_is_a_safety_error_when_env_rows_exist():
    rows = pd.DataFrame([{"id": 1, "job_id": "stale", "env_id": "s", "env_name": "S"}])

    with pytest.raises(RuntimeError, match="zero non-blank job_id"):
        script._refuse_empty_landing_source(set(), rows, allow_empty_landing=False)

    script._refuse_empty_landing_source(set(), rows, allow_empty_landing=True)


def test_execute_requires_explicit_confirmation(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        ["cleanup_orphaned_production_env_configs.py", "--execute"],
    )

    with pytest.raises(SystemExit):
        script.main()
