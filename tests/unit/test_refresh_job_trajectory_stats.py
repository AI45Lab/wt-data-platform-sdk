import pytest

from scripts.trajectory_stats import refresh_job_trajectory_stats as script
from wt_sdk.trajectory_stats.service import TrajectoryStatsUpdate

JOB_ID = "selfdev#claudecode#glm-5.3#exploit#20261126#max#batch2_usable#21287#003"


def _install_fakes(monkeypatch, result):
    clients = []
    stores = []
    service_calls = []

    class FakeClient:
        def __init__(self, config):
            self.config = config
            self.closed = False
            clients.append(self)

        def close(self):
            self.closed = True

    class FakeStore:
        def __init__(self, db_uri):
            self.db_uri = db_uri
            self.ready = False
            self.closed = False
            stores.append(self)

        def verify_ready(self):
            self.ready = True

        def close(self):
            self.closed = True

    class FakeService:
        def __init__(self, client, store):
            assert client is clients[0]
            assert store is stores[0]

        def refresh_delivered_sessions(self, job_ids, *, landing_page_size):
            service_calls.append((job_ids, landing_page_size))
            if isinstance(result, Exception):
                raise result
            return result

    monkeypatch.setattr(script, "WTGatewayClient", FakeClient)
    monkeypatch.setattr(script, "TrajectoryStatsStore", FakeStore)
    monkeypatch.setattr(script, "TrajectoryStatsService", FakeService)
    return clients, stores, service_calls


def test_refresh_job_uses_production_service_and_closes_resources(monkeypatch, capsys):
    clients, stores, service_calls = _install_fakes(
        monkeypatch,
        TrajectoryStatsUpdate(
            sessions_updated=1107,
            categories_upserted=1,
            categories_deleted=0,
        ),
    )

    result = script.refresh_job(f"  {JOB_ID}  ", batch_size=25)

    assert result.sessions_updated == 1107
    assert service_calls == [([JOB_ID], 25)]
    assert len(clients) == len(stores) == 1
    assert clients[0].config.tables.profile == "production"
    assert stores[0].db_uri == clients[0].config.tables.serving_db_uri
    assert stores[0].ready is True
    assert stores[0].closed is True
    assert clients[0].closed is True
    output = capsys.readouterr().out
    assert "updated 1107 session rows" in output
    assert "upserted 1 categories" in output
    assert "deleted 0 stale categories" in output


def test_refresh_job_empty_delivered_set_returns_zero_update(monkeypatch, capsys):
    clients, stores, service_calls = _install_fakes(
        monkeypatch,
        TrajectoryStatsUpdate(),
    )

    result = script.refresh_job(JOB_ID)

    assert result == TrajectoryStatsUpdate()
    assert service_calls == [([JOB_ID], 100)]
    assert stores[0].closed is True
    assert clients[0].closed is True
    assert "updated 0 session rows" in capsys.readouterr().out


def test_refresh_job_closes_resources_when_refresh_fails(monkeypatch):
    clients, stores, _ = _install_fakes(
        monkeypatch,
        RuntimeError("refresh failed"),
    )

    with pytest.raises(RuntimeError, match="refresh failed"):
        script.refresh_job(JOB_ID)

    assert stores[0].closed is True
    assert clients[0].closed is True


@pytest.mark.parametrize(
    "argv",
    [
        ["--job-id", "   "],
        ["--job-id", JOB_ID, "--batch-size", "0"],
        ["--job-id", JOB_ID, "--batch-size", "-1"],
    ],
)
def test_invalid_cli_arguments_fail_before_connecting(monkeypatch, argv):
    def fail(*args, **kwargs):
        raise AssertionError("invalid arguments must not connect or refresh")

    monkeypatch.setattr(script, "WTGatewayClient", fail)
    monkeypatch.setattr(script, "refresh_job", fail)

    with pytest.raises(SystemExit) as exc_info:
        script.main(argv)

    assert exc_info.value.code == 2
