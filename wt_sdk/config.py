import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from wt_sdk.dldb_timing import (
    resolve_dldb_metrics_log_path,
    resolve_dldb_model,
    resolve_enable_dldb_timing_logs,
)


# 2026.10 storage split: env-config + landing are the hot layer on GPFS
# (worker-local mount, no storage_options), serving is the cold/delivery
# layer on S3.
WT_GPFS_ROOT = "/mnt/shared-storage-user/adpc-share/wt-data-platform"
DEFAULT_LANDING_DB_URI = f"{WT_GPFS_ROOT}/landing"
DEFAULT_SERVING_DB_URI = "s3://wt-data-platform"
DEFAULT_ENV_CONFIG_DB_URI = f"{WT_GPFS_ROOT}/env-config"

DEFAULT_LANDING_TABLE = "wt_landing"
DEFAULT_SERVING_TABLE = "wt_serving"
DEFAULT_ENV_CONFIG_TABLE = "wt_env_config"
TEST_LANDING_TABLE = "landing_test"
TEST_SERVING_TABLE = "serving_test"
TEST_ENV_CONFIG_TABLE = "env_test"


def _env(*names: str) -> Optional[str]:
    """Return the first non-empty value among supported environment variables."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _resolve_table_profile(profile: Optional[str]) -> str:
    value = (profile or _env("WT_SDK_PROFILE") or "test").strip().lower()
    aliases = {"prod": "production", "production": "production", "test": "test"}
    if value not in aliases:
        raise ValueError("WT_SDK_PROFILE must be one of: production, prod, test")
    return aliases[value]


def is_remote_uri(db_uri: str) -> bool:
    """True when the db URI is object storage (s3://, gs://, ...), False for
    local filesystem paths such as the GPFS hot-layer directories."""
    return "://" in db_uri


def db_uri_for_table(table_name: str) -> str:
    """Return the database hosting a canonical landing/serving table name."""
    if table_name in (DEFAULT_LANDING_TABLE, TEST_LANDING_TABLE):
        return DEFAULT_LANDING_DB_URI
    if table_name in (DEFAULT_SERVING_TABLE, TEST_SERVING_TABLE):
        return DEFAULT_SERVING_DB_URI
    raise ValueError(
        f"Unknown trajectory table {table_name!r}; cannot resolve its database"
    )


def storage_options_for(db_uri: str, s3: Optional["S3Config"] = None) -> Optional[Dict[str, Any]]:
    """S3 storage options for remote URIs; None for local (GPFS) paths."""
    if not is_remote_uri(db_uri):
        return None
    return (s3 or S3Config()).to_storage_options()


def resolve_env_config_db_uri(explicit_db_uri: Optional[str] = None) -> str:
    """Resolve the separate database used by evaluation environment configs."""
    return explicit_db_uri or _env("WT_SDK_ENV_CONFIG_DB_URI") or DEFAULT_ENV_CONFIG_DB_URI


def resolve_env_config_table_name(
    explicit_table_name: Optional[str] = None,
    profile: Optional[str] = None,
) -> str:
    """Resolve the profile-specific environment-config table name."""
    if explicit_table_name and explicit_table_name.strip():
        return explicit_table_name.strip()

    resolved_profile = _resolve_table_profile(profile)
    if resolved_profile == "test":
        return TEST_ENV_CONFIG_TABLE
    return DEFAULT_ENV_CONFIG_TABLE


@dataclass
class S3Config:
    """S3 connection settings resolved from explicit values or environment variables."""

    allow_http: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_endpoint: Optional[str] = None
    request_timeout: Optional[str] = None

    def __post_init__(self):
        self.allow_http = self.allow_http or _env("WT_SDK_S3_ALLOW_HTTP") or "true"
        self.aws_access_key_id = self.aws_access_key_id or _env("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = self.aws_secret_access_key or _env("AWS_SECRET_ACCESS_KEY")
        self.aws_endpoint = self.aws_endpoint or _env(
            "WT_SDK_S3_ENDPOINT",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT_URL",
        )
        self.request_timeout = self.request_timeout or _env("WT_SDK_S3_REQUEST_TIMEOUT")

    def to_storage_options(self) -> Dict[str, Any]:
        options = {
            "allow_http": self.allow_http,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_endpoint": self.aws_endpoint,
        }
        if self.request_timeout:
            # lance expects a Duration with unit (e.g. "120s"); append "s" for bare numbers.
            timeout = self.request_timeout
            if timeout.isdigit():
                timeout = f"{timeout}s"
            options["timeout"] = timeout
        return {key: value for key, value in options.items() if value is not None}


@dataclass
class TableConfig:
    """Logical table configuration with a test-safe default profile.

    Landing and serving live in separate databases since the 2026.10 storage
    split (GPFS vs S3). Resolution order per role: explicit per-role value,
    then the shared ``db_uri`` argument (both roles, for disposable/test
    databases), then the per-role environment variable, then the role default.
    The legacy ``WT_SDK_DB_URI`` variable is intentionally NOT consulted:
    upgrading to this SDK generation means the new stack; pre-cutover tools
    pin the legacy S3 URIs explicitly.
    """

    landing_db_uri: Optional[str] = None
    serving_db_uri: Optional[str] = None
    # Shared explicit override for both roles (e.g. TableConfig(db_uri="memory://t")).
    db_uri: Optional[str] = None
    landing_table: Optional[str] = None
    serving_table: Optional[str] = None
    profile: Optional[str] = None

    def __post_init__(self):
        self.profile = _resolve_table_profile(self.profile)
        self.landing_db_uri = (
            self.landing_db_uri or self.db_uri
            or _env("WT_SDK_LANDING_DB_URI") or DEFAULT_LANDING_DB_URI
        )
        self.serving_db_uri = (
            self.serving_db_uri or self.db_uri
            or _env("WT_SDK_SERVING_DB_URI") or DEFAULT_SERVING_DB_URI
        )

        if self.profile == "test":
            default_landing = TEST_LANDING_TABLE
            default_serving = TEST_SERVING_TABLE
        else:
            default_landing = DEFAULT_LANDING_TABLE
            default_serving = DEFAULT_SERVING_TABLE

        self.landing_table = self.landing_table or _env("WT_SDK_LANDING_TABLE") or default_landing
        self.serving_table = self.serving_table or _env("WT_SDK_SERVING_TABLE") or default_serving

    def db_uri_for(self, table_role: str) -> str:
        if table_role == "landing":
            return self.landing_db_uri
        if table_role == "serving":
            return self.serving_db_uri
        raise ValueError(f"Unknown table role: {table_role!r}")

    def table_name_for(self, table_role: str) -> str:
        if table_role == "landing":
            return self.landing_table
        if table_role == "serving":
            return self.serving_table
        raise ValueError(f"Unknown table role: {table_role!r}")

    def landing_uri(self) -> str:
        return f"{self.landing_db_uri}/{self.landing_table}.lance"

    def serving_uri(self) -> str:
        return f"{self.serving_db_uri}/{self.serving_table}.lance"


@dataclass
class GatewayConfig:
    s3: S3Config = None
    tables: TableConfig = None
    use_memory_queue: bool = False
    flush_every: int = 1000
    dldb_model: Optional[str] = None
    dldb_heartbeat_interval_s: Optional[float] = None
    enable_dldb_timing_logs: bool = False
    log_dldb_metrics_summary_on_close: bool = True
    dldb_metrics_log_path: Optional[str] = None

    def __post_init__(self):
        if self.s3 is None:
            self.s3 = S3Config()
        if self.tables is None:
            self.tables = TableConfig()

    def resolved_dldb_model(self) -> Optional[str]:
        return resolve_dldb_model(self.dldb_model)

    def resolved_enable_dldb_timing_logs(self) -> bool:
        return resolve_enable_dldb_timing_logs(self.enable_dldb_timing_logs)

    def resolved_dldb_metrics_log_path(self) -> Optional[str]:
        return resolve_dldb_metrics_log_path(self.dldb_metrics_log_path)

    def to_dldb_config(self, db_uri: Optional[str] = None) -> Dict[str, Any]:
        """dldb connect kwargs for one database.

        ``storage_options`` applies only to object-store URIs; local
        filesystem databases (the GPFS hot layer) must not receive S3
        credentials/options. Without ``db_uri`` the legacy behavior of
        always including S3 options is kept for callers that never saw
        a URI (e.g. tests).
        """
        config = {
            "use_memory_queue": self.use_memory_queue,
            "flush_every": self.flush_every,
            "model": self.resolved_dldb_model(),
        }
        if db_uri is None or is_remote_uri(db_uri):
            config["storage_options"] = self.s3.to_storage_options()
        if self.dldb_heartbeat_interval_s is not None:
            config["heartbeat_interval_s"] = self.dldb_heartbeat_interval_s
        return config


# Compatibility snapshot for existing imports. New WTGatewayClient instances create
# a fresh GatewayConfig so configuration set before process startup is always used.
default_config = GatewayConfig()
