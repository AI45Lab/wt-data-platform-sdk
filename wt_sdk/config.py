import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from wt_sdk.dldb_timing import (
    resolve_dldb_metrics_log_path,
    resolve_dldb_model,
    resolve_enable_dldb_timing_logs,
)


DEFAULT_DB_URI = "s3://wind-tunnel-dldb"
DEFAULT_ENV_CONFIG_DB_URI = "s3://wind-tunnel-env-config"
DEFAULT_LANDING_TABLE = "wind_tunnel_landing"
DEFAULT_SERVING_TABLE = "wind_tunnel_serving"
TEST_LANDING_TABLE = "landing_test"
TEST_SERVING_TABLE = "serving_test"


def _env(*names: str) -> Optional[str]:
    """Return the first non-empty value among supported environment variables."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _resolve_table_profile(profile: Optional[str]) -> str:
    value = (profile or _env("WT_SDK_PROFILE") or "production").strip().lower()
    aliases = {"prod": "production", "production": "production", "test": "test"}
    if value not in aliases:
        raise ValueError("WT_SDK_PROFILE must be one of: production, prod, test")
    return aliases[value]


def resolve_env_config_db_uri(explicit_db_uri: Optional[str] = None) -> str:
    """Resolve the separate database used by evaluation environment configs."""
    return explicit_db_uri or _env("WT_SDK_ENV_CONFIG_DB_URI") or DEFAULT_ENV_CONFIG_DB_URI


@dataclass
class S3Config:
    """S3 connection settings resolved from explicit values or environment variables."""

    allow_http: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_endpoint: Optional[str] = None

    def __post_init__(self):
        self.allow_http = self.allow_http or _env("WT_SDK_S3_ALLOW_HTTP") or "true"
        self.aws_access_key_id = self.aws_access_key_id or _env("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = self.aws_secret_access_key or _env("AWS_SECRET_ACCESS_KEY")
        self.aws_endpoint = self.aws_endpoint or _env(
            "WT_SDK_S3_ENDPOINT",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT_URL",
        )

    def to_storage_options(self) -> Dict[str, Any]:
        options = {
            "allow_http": self.allow_http,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_endpoint": self.aws_endpoint,
        }
        return {key: value for key, value in options.items() if value is not None}


@dataclass
class TableConfig:
    """Logical table configuration with a production-safe default profile."""

    db_uri: Optional[str] = None
    landing_table: Optional[str] = None
    serving_table: Optional[str] = None
    profile: Optional[str] = None

    def __post_init__(self):
        self.profile = _resolve_table_profile(self.profile)
        self.db_uri = self.db_uri or _env("WT_SDK_DB_URI") or DEFAULT_DB_URI

        if self.profile == "test":
            default_landing = TEST_LANDING_TABLE
            default_serving = TEST_SERVING_TABLE
        else:
            default_landing = DEFAULT_LANDING_TABLE
            default_serving = DEFAULT_SERVING_TABLE

        self.landing_table = self.landing_table or _env("WT_SDK_LANDING_TABLE") or default_landing
        self.serving_table = self.serving_table or _env("WT_SDK_SERVING_TABLE") or default_serving

    def landing_uri(self) -> str:
        return f"{self.db_uri}/{self.landing_table}.lance"

    def serving_uri(self) -> str:
        return f"{self.db_uri}/{self.serving_table}.lance"


@dataclass
class GatewayConfig:
    s3: S3Config = None
    tables: TableConfig = None
    use_memory_queue: bool = False
    flush_every: int = 1000
    dldb_model: Optional[str] = None
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

    def to_dldb_config(self) -> Dict[str, Any]:
        return {
            "use_memory_queue": self.use_memory_queue,
            "flush_every": self.flush_every,
            "model": self.resolved_dldb_model(),
            "storage_options": self.s3.to_storage_options(),
        }


# Compatibility snapshot for existing imports. New WTGatewayClient instances create
# a fresh GatewayConfig so configuration set before process startup is always used.
default_config = GatewayConfig()
