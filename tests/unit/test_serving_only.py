import pytest

import wt_sdk.client as client_module
from wt_sdk import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_LANDING_DB_URI,
    DEFAULT_SERVING_DB_URI,
    GatewayConfig,
    TableConfig,
)


class FakeSession:
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.tables = {}
        self.shutdown_calls = 0

    def shutdown(self):
        self.shutdown_calls += 1


def _connect_recorder(monkeypatch):
    calls = []
    sessions = {}

    def connect(db_uri, **kwargs):
        session = FakeSession(db_uri)
        calls.append((db_uri, kwargs))
        sessions[db_uri] = session
        return session

    monkeypatch.setattr(client_module.dldb, "connect", connect)
    return calls, sessions


def test_default_config_preserves_eager_dual_connections(monkeypatch):
    calls, sessions = _connect_recorder(monkeypatch)

    client = WTGatewayClient(GatewayConfig())

    assert client.config.serving_only is False
    assert [db_uri for db_uri, _ in calls] == [
        DEFAULT_LANDING_DB_URI,
        DEFAULT_SERVING_DB_URI,
    ]
    assert client._landing_session is sessions[DEFAULT_LANDING_DB_URI]
    assert client._serving_session is sessions[DEFAULT_SERVING_DB_URI]
    assert client._last_session is client._landing_session

    client.close()
    assert sessions[DEFAULT_LANDING_DB_URI].shutdown_calls == 1
    assert sessions[DEFAULT_SERVING_DB_URI].shutdown_calls == 1


def test_default_shared_database_still_uses_one_session(monkeypatch):
    calls, sessions = _connect_recorder(monkeypatch)

    client = WTGatewayClient(
        GatewayConfig(tables=TableConfig(db_uri="memory://shared"))
    )

    assert [db_uri for db_uri, _ in calls] == ["memory://shared"]
    assert client._serving_session is client._landing_session
    client.close()
    assert sessions["memory://shared"].shutdown_calls == 1


def test_serving_only_connects_only_serving_and_rejects_landing(monkeypatch):
    calls, sessions = _connect_recorder(monkeypatch)
    config = GatewayConfig(
        tables=TableConfig(
            landing_db_uri="/gpfs/must-not-be-opened",
            serving_db_uri="s3://serving-only",
        ),
        serving_only=True,
    )

    client = WTGatewayClient(config)

    assert [db_uri for db_uri, _ in calls] == ["s3://serving-only"]
    assert client._landing_session is None
    assert client._serving_session is sessions["s3://serving-only"]
    assert client._last_session is client._serving_session
    assert client.session_for(config.tables.serving_table) is client._serving_session

    with pytest.raises(RuntimeError, match="serving_only=True"):
        client.session_for(config.tables.landing_table)


def test_serving_only_cache_invalidation_and_close_skip_landing(monkeypatch):
    calls, sessions = _connect_recorder(monkeypatch)
    config = GatewayConfig(
        tables=TableConfig(serving_db_uri="s3://serving-only"),
        serving_only=True,
    )
    client = WTGatewayClient(config)
    serving = sessions["s3://serving-only"]
    serving.tables.update({config.tables.serving_table: object(), "other": object()})

    client.invalidate_table_cache(config.tables.serving_table)
    assert list(serving.tables) == ["other"]

    client.invalidate_table_cache()
    assert serving.tables == {}

    client.close()
    assert serving.shutdown_calls == 1
    assert len(calls) == 1
