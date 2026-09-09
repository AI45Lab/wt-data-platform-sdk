import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "ops" / "cleanup_production_job_ids.py"
SPEC = importlib.util.spec_from_file_location("cleanup_production_job_ids", SCRIPT_PATH)
script = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = script
SPEC.loader.exec_module(script)


class FakeActiveClient:
    def __init__(self):
        self.rows = {
            "wind_tunnel_landing": {
                "job-a": [{"id": "l-a", "job_id": "job-a"}],
                "job-b": [],
            },
            "wind_tunnel_serving": {
                "job-a": [],
                "job-b": [{"id": "s-b", "job_id": "job-b"}],
            },
        }
        self.deleted = []
        self.closed = False

    def query_data(self, *, filter_query, table, **kwargs):
        job_id = filter_query.removeprefix("job_id IN ('").removesuffix("')")
        return list(self.rows[table].get(job_id, []))

    def delete_landing(self, predicate):
        self.deleted.append(("landing", predicate))
        job_id = predicate.removeprefix("job_id IN ('").removesuffix("')")
        rows = self.rows["wind_tunnel_landing"].pop(job_id, [])
        return len(rows)

    def delete_serving(self, predicate):
        self.deleted.append(("serving", predicate))
        job_id = predicate.removeprefix("job_id IN ('").removesuffix("')")
        rows = self.rows["wind_tunnel_serving"].pop(job_id, [])
        return len(rows)

    def close(self):
        self.closed = True


class FakeEnvManager:
    def __init__(self):
        self.table_name = "evaluation_env_config"
        self.rows = {
            "job-a": [{"id": 1, "job_id": "job-a"}],
            "job-b": [],
        }
        self.deleted = []
        self.closed = False
        self.session = SimpleNamespace(
            filter=self.filter,
            delete=self.delete,
        )

    def filter(self, table_name, predicate, **kwargs):
        job_id = predicate.removeprefix("job_id IN ('").removesuffix("')")
        return pd.DataFrame(self.rows.get(job_id, []))

    def delete(self, table_name, predicate):
        self.deleted.append(predicate)
        job_id = predicate.removeprefix("job_id IN ('").removesuffix("')")
        self.rows.pop(job_id, None)

    def close(self):
        self.closed = True


def test_normalize_job_ids_strips_and_deduplicates():
    assert script.normalize_job_ids([" job-a ", "job-b", "job-a"]) == ["job-a", "job-b"]


def test_build_predicate_escapes_quotes():
    assert script.build_job_id_predicate(["job'a"]) == "job_id IN ('job''a')"


def test_run_deletes_matches_in_each_table_and_skips_empty_tables(capsys):
    client = FakeActiveClient()
    manager = FakeEnvManager()

    assert script.run(
        job_ids=["job-a", "job-b"],
        execute=True,
        client=client,
        env_manager=manager,
    ) == 0

    assert manager.deleted == ["job_id IN ('job-a')"]
    assert ("landing", "job_id IN ('job-a')") in client.deleted
    assert ("serving", "job_id IN ('job-b')") in client.deleted
    output = capsys.readouterr().out
    assert "[SKIP] evaluation_env_config: job-b has no matching rows" in output
    assert "[VERIFY] wind_tunnel_landing: no requested job_id remains" in output
    assert "[VERIFY] wind_tunnel_serving: no requested job_id remains" in output


def test_run_preview_does_not_delete(capsys):
    client = FakeActiveClient()
    manager = FakeEnvManager()

    assert script.run(
        job_ids=["job-a", "job-b"],
        execute=False,
        client=client,
        env_manager=manager,
    ) == 0

    assert manager.deleted == []
    assert client.deleted == []
    assert "Preview complete: 3 matching rows across 3 production tables." in capsys.readouterr().out


def test_test_profile_uses_test_table_names_and_landing_delete():
    assert script.PROFILE_TABLES["test"] == {
        "env": "env_config_test",
        "landing": "v2_landing_test",
        "serving": "serving_test",
    }
    assert script._table_role("v2_landing_test", "test") == "landing"
    assert script._table_role("serving_test", "test") == "serving"
