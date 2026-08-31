"""Durable and in-memory checkpoint stores for ETL scan windows."""

import os
from dataclasses import asdict, replace
from typing import Optional, Protocol

import dldb
import pandas as pd
import pyarrow as pa

import wt_sdk._time as sdk_time
from wt_sdk.config import S3Config

from .exceptions import CheckpointError
from .models import Checkpoint, checkpoint_identity


PRODUCTION_CHECKPOINT_TABLE = "wind_tunnel_etl_checkpoints"
TEST_CHECKPOINT_TABLE = "etl_checkpoints_test"
DEFAULT_CHECKPOINT_TABLE = PRODUCTION_CHECKPOINT_TABLE

ETL_CHECKPOINT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("pipeline_name", pa.string(), nullable=False),
        pa.field("pipeline_version", pa.string(), nullable=False),
        pa.field("source_table", pa.string(), nullable=False),
        pa.field("target_table", pa.string(), nullable=False),
        pa.field("bucket", pa.int32(), nullable=False),
        pa.field("committed_until_ms", pa.int64(), nullable=False),
        pa.field("last_run_id", pa.string(), nullable=True),
        pa.field("active_window_start_ms", pa.int64(), nullable=True),
        pa.field("active_window_end_ms", pa.int64(), nullable=True),
        pa.field("last_processed_id", pa.string(), nullable=True),
        pa.field("status", pa.string(), nullable=False),
        pa.field("updated_at_ms", pa.int64(), nullable=False),
    ]
)


class CheckpointStore(Protocol):
    def load(
        self,
        *,
        pipeline_name: str,
        pipeline_version: str,
        source_table: str,
        target_table: str,
        bucket: int,
    ) -> Optional[Checkpoint]: ...

    def save(self, checkpoint: Checkpoint) -> None: ...

    def delete(
        self,
        *,
        pipeline_name: str,
        pipeline_version: str,
        source_table: str,
        target_table: str,
        bucket: int,
    ) -> bool: ...

    def close(self) -> None: ...


class InMemoryCheckpointStore:
    """Hermetic store for unit tests and non-resumable local composition."""

    def __init__(self) -> None:
        self._values: dict[str, Checkpoint] = {}

    def load(
        self,
        *,
        pipeline_name: str,
        pipeline_version: str,
        source_table: str,
        target_table: str,
        bucket: int,
    ) -> Optional[Checkpoint]:
        key = checkpoint_identity(
            pipeline_name,
            pipeline_version,
            source_table,
            target_table,
            bucket,
        )
        return self._values.get(key)

    def save(self, checkpoint: Checkpoint) -> None:
        self._values[checkpoint.checkpoint_id] = checkpoint

    def delete(
        self,
        *,
        pipeline_name: str,
        pipeline_version: str,
        source_table: str,
        target_table: str,
        bucket: int,
    ) -> bool:
        checkpoint_id = checkpoint_identity(
            pipeline_name,
            pipeline_version,
            source_table,
            target_table,
            bucket,
        )
        return self._values.pop(checkpoint_id, None) is not None

    def close(self) -> None:
        return None


class DldbCheckpointStore:
    """Checkpoint store backed by one unpartitioned dldb control table."""

    def __init__(
        self,
        db_uri: str,
        *,
        table_name: str = DEFAULT_CHECKPOINT_TABLE,
        s3: Optional[S3Config] = None,
    ) -> None:
        if not db_uri or not db_uri.strip():
            raise ValueError("ETL checkpoint db_uri is required")
        if not table_name or not table_name.strip():
            raise ValueError("ETL checkpoint table_name is required")
        self.db_uri = db_uri.strip()
        self.table_name = table_name.strip()
        self.session = dldb.connect(
            self.db_uri,
            storage_options=(s3 or S3Config()).to_storage_options(),
        )

    def initialize(self) -> bool:
        """Create the table if absent; return True only when it was created."""

        if self.session.table_exists(self.table_name):
            actual = self.session.get_schema(self.table_name)
            if actual != ETL_CHECKPOINT_SCHEMA:
                raise CheckpointError(
                    f"checkpoint table '{self.table_name}' schema does not match "
                    "ETL_CHECKPOINT_SCHEMA"
                )
            return False
        self.session.create_table(self.table_name, ETL_CHECKPOINT_SCHEMA)
        return True

    def verify_ready(self) -> None:
        if not self.session.table_exists(self.table_name):
            raise CheckpointError(
                f"checkpoint table '{self.table_name}' does not exist in {self.db_uri}; "
                "initialize it explicitly before incremental ETL"
            )
        actual = self.session.get_schema(self.table_name)
        if actual != ETL_CHECKPOINT_SCHEMA:
            raise CheckpointError(
                f"checkpoint table '{self.table_name}' schema does not match "
                "ETL_CHECKPOINT_SCHEMA"
            )

    def load(
        self,
        *,
        pipeline_name: str,
        pipeline_version: str,
        source_table: str,
        target_table: str,
        bucket: int,
    ) -> Optional[Checkpoint]:
        self.verify_ready()
        checkpoint_id = checkpoint_identity(
            pipeline_name,
            pipeline_version,
            source_table,
            target_table,
            bucket,
        )
        frame = self.session.filter(
            self.table_name,
            query=f"id = '{_escape_sql(checkpoint_id)}'",
            limit=1,
            checkout_latest=True,
        )
        if frame is None or frame.empty:
            return None
        row = frame.iloc[0].to_dict()
        return Checkpoint(
            pipeline_name=str(row["pipeline_name"]),
            pipeline_version=str(row["pipeline_version"]),
            source_table=str(row["source_table"]),
            target_table=str(row["target_table"]),
            bucket=int(row["bucket"]),
            committed_until_ms=int(row["committed_until_ms"]),
            last_run_id=_optional_string(row.get("last_run_id")),
            active_window_start_ms=_optional_int(row.get("active_window_start_ms")),
            active_window_end_ms=_optional_int(row.get("active_window_end_ms")),
            last_processed_id=_optional_string(row.get("last_processed_id")),
            status=str(row["status"]),
            updated_at_ms=int(row["updated_at_ms"]),
        )

    def save(self, checkpoint: Checkpoint) -> None:
        self.verify_ready()
        effective = checkpoint
        if effective.updated_at_ms <= 0:
            effective = replace(effective, updated_at_ms=sdk_time.now_ms())
        row = asdict(effective)
        row["id"] = effective.checkpoint_id
        table = pa.Table.from_pylist([row], schema=ETL_CHECKPOINT_SCHEMA)
        frame = table.to_pandas(types_mapper=pd.ArrowDtype)
        self.session.upsert(
            self.table_name,
            columns=["id"],
            datas=frame,
        )

    def delete(
        self,
        *,
        pipeline_name: str,
        pipeline_version: str,
        source_table: str,
        target_table: str,
        bucket: int,
    ) -> bool:
        """Delete one exact checkpoint identity, primarily for safe test cleanup."""

        checkpoint_id = checkpoint_identity(
            pipeline_name,
            pipeline_version,
            source_table,
            target_table,
            bucket,
        )
        if self.load(
            pipeline_name=pipeline_name,
            pipeline_version=pipeline_version,
            source_table=source_table,
            target_table=target_table,
            bucket=bucket,
        ) is None:
            return False
        self.session.delete(
            self.table_name,
            f"id = '{_escape_sql(checkpoint_id)}'",
        )
        return True

    def close(self) -> None:
        self.session.shutdown()


def resolve_etl_state_db_uri(explicit: Optional[str] = None) -> str:
    value = explicit or os.getenv("WT_SDK_ETL_STATE_DB_URI")
    if not value or not value.strip():
        raise ValueError(
            "ETL state database is required; pass db_uri or set WT_SDK_ETL_STATE_DB_URI"
        )
    return value.strip()


def resolve_checkpoint_table(profile: str, explicit: Optional[str] = None) -> str:
    """Resolve the checkpoint table from one shared WT SDK profile."""

    if explicit is not None:
        normalized_table = explicit.strip()
        if not normalized_table:
            raise ValueError("checkpoint table override must be non-empty")
        return normalized_table
    normalized_profile = profile.strip().lower()
    if normalized_profile == "test":
        return TEST_CHECKPOINT_TABLE
    if normalized_profile in {"production", "prod"}:
        return PRODUCTION_CHECKPOINT_TABLE
    raise ValueError("checkpoint profile must be 'test' or 'production'")


def _optional_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return int(value)


def _optional_string(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


__all__ = [
    "CheckpointStore",
    "DEFAULT_CHECKPOINT_TABLE",
    "DldbCheckpointStore",
    "ETL_CHECKPOINT_SCHEMA",
    "InMemoryCheckpointStore",
    "PRODUCTION_CHECKPOINT_TABLE",
    "TEST_CHECKPOINT_TABLE",
    "resolve_checkpoint_table",
    "resolve_etl_state_db_uri",
]
