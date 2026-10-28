from wt_sdk.config import (
    DEFAULT_ENV_CONFIG_TABLE,
    DEFAULT_ENV_CONFIG_DB_URI,
    DEFAULT_LANDING_DB_URI,
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_DB_URI,
    DEFAULT_SERVING_TABLE,
    TEST_ENV_CONFIG_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
    S3Config,
    TableConfig,
    resolve_env_config_db_uri,
    resolve_env_config_table_name,
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


def test_table_config_defaults_to_test_without_etl_configuration(monkeypatch):
    monkeypatch.delenv("WT_SDK_PROFILE", raising=False)
    monkeypatch.delenv("WT_SDK_ETL_STATE_DB_URI", raising=False)
    monkeypatch.delenv("WT_SDK_LANDING_TABLE", raising=False)
    monkeypatch.delenv("WT_SDK_SERVING_TABLE", raising=False)
    monkeypatch.delenv("WT_SDK_LANDING_DB_URI", raising=False)
    monkeypatch.delenv("WT_SDK_SERVING_DB_URI", raising=False)

    config = TableConfig()

    assert config.profile == "test"
    assert config.landing_table == TEST_LANDING_TABLE
    assert config.serving_table == TEST_SERVING_TABLE
    assert config.landing_db_uri == DEFAULT_LANDING_DB_URI
    assert config.serving_db_uri == DEFAULT_SERVING_DB_URI


def test_gateway_config_switches_to_test_profile_from_environment(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "test")
    monkeypatch.delenv("WT_SDK_LANDING_DB_URI", raising=False)
    monkeypatch.delenv("WT_SDK_SERVING_DB_URI", raising=False)

    config = GatewayConfig()

    assert config.tables.profile == "test"
    assert config.tables.landing_table == TEST_LANDING_TABLE
    assert config.tables.serving_table == TEST_SERVING_TABLE


def test_legacy_shared_db_uri_env_is_not_consulted(monkeypatch):
    monkeypatch.setenv("WT_SDK_DB_URI", "s3://legacy-dldb")
    monkeypatch.delenv("WT_SDK_LANDING_DB_URI", raising=False)
    monkeypatch.delenv("WT_SDK_SERVING_DB_URI", raising=False)

    config = TableConfig()

    assert config.landing_db_uri == DEFAULT_LANDING_DB_URI
    assert config.serving_db_uri == DEFAULT_SERVING_DB_URI


def test_per_role_db_uri_env_variables_and_precedence(monkeypatch):
    monkeypatch.delenv("WT_SDK_DB_URI", raising=False)
    monkeypatch.setenv("WT_SDK_LANDING_DB_URI", "s3://landing-db")
    monkeypatch.setenv("WT_SDK_SERVING_DB_URI", "s3://serving-db")

    from_env = TableConfig()
    assert from_env.landing_db_uri == "s3://landing-db"
    assert from_env.serving_db_uri == "s3://serving-db"

    # Explicit values and the shared db_uri argument win over the environment.
    explicit = TableConfig(landing_db_uri="s3://explicit-landing")
    assert explicit.landing_db_uri == "s3://explicit-landing"
    assert explicit.serving_db_uri == "s3://serving-db"

    shared = TableConfig(db_uri="memory://shared")
    assert shared.landing_db_uri == "memory://shared"
    assert shared.serving_db_uri == "memory://shared"


def test_production_profile_resolution_does_not_require_etl_state_uri(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "production")
    monkeypatch.delenv("WT_SDK_ETL_STATE_DB_URI", raising=False)
    monkeypatch.delenv("WT_SDK_LANDING_TABLE", raising=False)
    monkeypatch.delenv("WT_SDK_SERVING_TABLE", raising=False)

    config = TableConfig()

    assert config.profile == "production"
    assert config.landing_table == DEFAULT_LANDING_TABLE
    assert config.serving_table == DEFAULT_SERVING_TABLE


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


def test_env_config_table_defaults_to_test_profile(monkeypatch):
    monkeypatch.delenv("WT_SDK_PROFILE", raising=False)

    assert resolve_env_config_table_name() == TEST_ENV_CONFIG_TABLE


def test_env_config_table_resolves_profile_and_alias(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "production")

    assert resolve_env_config_table_name() == DEFAULT_ENV_CONFIG_TABLE
    assert resolve_env_config_table_name(profile="prod") == DEFAULT_ENV_CONFIG_TABLE
    assert resolve_env_config_table_name(profile="test") == TEST_ENV_CONFIG_TABLE


def test_explicit_env_config_table_overrides_profile(monkeypatch):
    monkeypatch.setenv("WT_SDK_PROFILE", "production")

    assert (
        resolve_env_config_table_name("custom_env_config", profile="test")
        == "custom_env_config"
    )
