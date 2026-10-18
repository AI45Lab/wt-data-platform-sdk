from wt_sdk.env_config_client import EnvConfigManager
import wt_sdk.env_config_client as env_config_module


class FakeSession:
    """Records filter/update calls so tests can assert read/write behavior."""

    def __init__(self, matched_ids=None):
        self.filter_calls = []
        self.update_calls = []
        self._matched_ids = matched_ids if matched_ids is not None else []

    def filter(self, table_name, query, **kwargs):
        self.filter_calls.append({"table": table_name, "query": query, **kwargs})
        import pandas as pd

        return pd.DataFrame({"id": self._matched_ids})

    def update(self, table_name, where, values):
        self.update_calls.append(
            {"table": table_name, "where": where, "values": dict(values)}
        )

    def shutdown(self):
        return None


def _manager_with(monkeypatch, session):
    monkeypatch.setattr(
        env_config_module.dldb, "connect", lambda db_uri, **kwargs: session
    )
    manager = EnvConfigManager(table_name="env_config_test")
    return manager


def test_update_configs_where_returns_matched_count_and_updates_once(monkeypatch):
    session = FakeSession(matched_ids=[4, 5, 6])
    manager = _manager_with(monkeypatch, session)

    n = manager.update_configs_where(
        "job_id = 'job-9' AND finished = false", {"finished": True}
    )

    assert n == 3
    assert len(session.update_calls) == 1
    call = session.update_calls[0]
    assert call["table"] == "env_config_test"
    assert call["where"] == "job_id = 'job-9' AND finished = false"
    assert call["values"] == {"finished": True}
    # Pre-count is a cheap id-projection read
    assert session.filter_calls[-1]["columns"] == ["id"]
    manager.close()


def test_update_configs_where_encodes_env_params_dict(monkeypatch):
    session = FakeSession(matched_ids=[1])
    manager = _manager_with(monkeypatch, session)

    manager.update_configs_where("env_name = 'A'", {"env_params": {"v": 2}})

    assert session.update_calls[0]["values"]["env_params"] == '{"v": 2}'
    manager.close()


def test_update_configs_where_skips_write_when_nothing_matches(monkeypatch):
    session = FakeSession(matched_ids=[])
    manager = _manager_with(monkeypatch, session)

    n = manager.update_configs_where("finished = false", {"finished": True})

    assert n == 0
    assert session.update_calls == []
    manager.close()


def test_update_configs_where_without_count_read_returns_minus_one(monkeypatch):
    session = FakeSession(matched_ids=[1, 2])
    manager = _manager_with(monkeypatch, session)

    n = manager.update_configs_where(
        "finished = false", {"finished": True}, count_only_read=False
    )

    assert n == -1
    assert session.filter_calls == []
    assert len(session.update_calls) == 1
    manager.close()


def test_update_configs_where_rejects_empty_filter(monkeypatch):
    session = FakeSession()
    manager = _manager_with(monkeypatch, session)

    for bad in ("", "   "):
        try:
            manager.update_configs_where(bad, {"finished": True})
            raise AssertionError("expected ValueError")
        except ValueError as exc:
            assert "non-empty" in str(exc)
    assert session.update_calls == []
    manager.close()


def test_update_configs_where_rejects_empty_and_immutable_fields(monkeypatch):
    session = FakeSession()
    manager = _manager_with(monkeypatch, session)

    try:
        manager.update_configs_where("finished = false", {})
        raise AssertionError("expected ValueError")
    except ValueError:
        pass

    for field in ("id", "env_id", "created_at", "nope"):
        try:
            manager.update_configs_where("finished = false", {field: 1})
            raise AssertionError(f"expected ValueError for {field}")
        except ValueError as exc:
            assert field in str(exc)

    assert session.update_calls == []
    manager.close()
