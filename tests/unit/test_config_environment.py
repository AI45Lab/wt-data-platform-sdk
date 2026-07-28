from wt_sdk.config import (
    DEFAULT_ENV_CONFIG_DB_URI,
    GatewayConfig,
    S3Config,
    TableConfig,
    resolve_env_config_db_uri,
)


def test_s3_config_reads_environment_and_omits_missing_values(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("WT_SDK_S3_ENDPOINT", "http://s3.example.test:8060")

    options = S3Config().to_storage_options()

    assert options == {
        "allow_http": "true",
        "aws_access_key_id": "test-access-key",
        "aws_secret_access_key": "test-secret",
        "aws_endpoint": "http://s3.example.test:8060",
    }

    monkeypatch.delenv("AWS_ACCESS_KEY_ID")
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY")
    monkeypatch.delenv("WT_SDK_S3_ENDPOINT")
    options = S3Config().to_storage_options()

    assert options == {"allow_http": "true"}


def test_table_config_defaults_to_production(monkeypatch):
    monkeypatch.delenv("WT_SDK_PROFILE", raising=False)
    monkeypatch.delenv("WT_SDK_LANDING_TABLE", raising=False)
    monkeypatch.delenv("WT_SDK_SERVING_TABLE", raising=False)

    config = TableConfig()

    assert config.profile == "production"
    assert config.landing_table == "wind_tunnel_landing"
    assert config.serving_table == "wind_tunnel_serving"


def test_gateway_config_switches_to_test_profile_from_environment(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "test")
    monkeypatch.setenv("WT_SDK_DB_URI", "s3://test-dldb")

    config = GatewayConfig()

    assert config.tables.db_uri == "s3://test-dldb"
    assert config.tables.profile == "test"
    assert config.tables.landing_table == "landing_test"
    assert config.tables.serving_table == "serving_test"


def test_explicit_table_config_overrides_environment_profile(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "test")

    config = TableConfig(landing_table="custom_landing", serving_table="custom_serving")

    assert config.landing_table == "custom_landing"
    assert config.serving_table == "custom_serving"


def test_env_config_database_uses_dedicated_override(monkeypatch):
    monkeypatch.delenv("WT_SDK_ENV_CONFIG_DB_URI", raising=False)
    assert resolve_env_config_db_uri() == DEFAULT_ENV_CONFIG_DB_URI

    monkeypatch.setenv("WT_SDK_ENV_CONFIG_DB_URI", "s3://test-env-config")
    assert resolve_env_config_db_uri() == "s3://test-env-config"
    assert resolve_env_config_db_uri("s3://explicit-env-config") == "s3://explicit-env-config"
