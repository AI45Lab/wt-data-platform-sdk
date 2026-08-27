from wt_sdk.config import DEFAULT_ENV_CONFIG_TABLE, TEST_ENV_CONFIG_TABLE
from wt_sdk.env_config_client import EnvConfigManager
import wt_sdk.env_config_client as env_config_module


class FakeSession:
    def shutdown(self):
        return None


def _capture_connect(monkeypatch):
    calls = []

    def fake_connect(db_uri, **kwargs):
        calls.append({"db_uri": db_uri, **kwargs})
        return FakeSession()

    monkeypatch.setattr(env_config_module.dldb, "connect", fake_connect)
    return calls


def test_manager_defaults_to_test_env_table(monkeypatch):
    monkeypatch.delenv("WT_SDK_PROFILE", raising=False)
    _capture_connect(monkeypatch)

    manager = EnvConfigManager()

    assert manager.table_name == TEST_ENV_CONFIG_TABLE
    manager.close()


def test_manager_uses_production_env_table_from_environment(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "production")
    _capture_connect(monkeypatch)

    manager = EnvConfigManager()

    assert manager.table_name == DEFAULT_ENV_CONFIG_TABLE
    manager.close()


def test_manager_explicit_profile_and_table_precedence(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "production")
    _capture_connect(monkeypatch)

    test_manager = EnvConfigManager(profile="test")
    explicit_manager = EnvConfigManager(
        table_name="custom_env_config",
        profile="test",
    )

    assert test_manager.table_name == TEST_ENV_CONFIG_TABLE
    assert explicit_manager.table_name == "custom_env_config"
    test_manager.close()
    explicit_manager.close()


def test_manager_preserves_existing_positional_constructor_arguments(monkeypatch):
    calls = _capture_connect(monkeypatch)

    manager = EnvConfigManager(
        "custom_env_config",
        "s3://custom-env-db",
        {"allow_http": "true"},
    )

    assert manager.table_name == "custom_env_config"
    assert calls == [
        {
            "db_uri": "s3://custom-env-db",
            "storage_options": {"allow_http": "true"},
            "model": None,
        }
    ]
    manager.close()
