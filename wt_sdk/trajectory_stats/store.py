"""dldb persistence for the two production trajectory-statistics tables."""

from __future__ import annotations

from typing import Iterable, Mapping, Optional

import dldb
import pandas as pd
import pyarrow as pa

from wt_sdk.config import (
    DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE,
    DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE,
    S3Config,
    TableConfig,
    storage_options_for,
)

from .schemas import (
    CATEGORY_TRAJECTORY_STATS_SCHEMA,
    CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS,
    SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN,
    SESSION_TRAJECTORY_METRICS_PARTITION_TYPE,
    SESSION_TRAJECTORY_METRICS_PARTITIONS,
    SESSION_TRAJECTORY_METRICS_SCHEMA,
    SESSION_TRAJECTORY_METRICS_UPSERT_KEYS,
)


CategoryKey = tuple[str, str, str, str, str]


class TrajectoryStatsStore:
    """Manage both statistics tables through one serving-database connection."""

    def __init__(
        self,
        db_uri: Optional[str] = None,
        *,
        s3: Optional[S3Config] = None,
    ) -> None:
        self.db_uri = (
            db_uri or TableConfig(profile="production").serving_db_uri
        ).strip()
        if not self.db_uri:
            raise ValueError("trajectory statistics db_uri is required")
        self.session_table = DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE
        self.category_table = DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE
        connect_kwargs = {}
        storage_options = storage_options_for(self.db_uri, s3)
        if storage_options is not None:
            connect_kwargs["storage_options"] = storage_options
        self.session = dldb.connect(self.db_uri, **connect_kwargs)

    def verify_ready(self) -> None:
        """Verify schemas and partitioning without creating or replacing tables."""
        self._verify_table(
            self.session_table,
            SESSION_TRAJECTORY_METRICS_SCHEMA,
            partition_column=SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN,
            partition_type=SESSION_TRAJECTORY_METRICS_PARTITION_TYPE,
            partitions=SESSION_TRAJECTORY_METRICS_PARTITIONS,
        )
        self._verify_table(
            self.category_table,
            CATEGORY_TRAJECTORY_STATS_SCHEMA,
        )

    def list_session_rows(self) -> list[dict[str, object]]:
        self._verify_table(
            self.session_table,
            SESSION_TRAJECTORY_METRICS_SCHEMA,
            partition_column=SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN,
            partition_type=SESSION_TRAJECTORY_METRICS_PARTITION_TYPE,
            partitions=SESSION_TRAJECTORY_METRICS_PARTITIONS,
        )
        frame = self.session.filter(
            self.session_table,
            query="job_id IS NOT NULL",
            limit=None,
            checkout_latest=True,
        )
        return _frame_records(frame)

    def list_category_rows(self) -> list[dict[str, object]]:
        self._verify_table(self.category_table, CATEGORY_TRAJECTORY_STATS_SCHEMA)
        frame = self.session.filter(
            self.category_table,
            query="dataset IS NOT NULL",
            limit=None,
            checkout_latest=True,
        )
        return _frame_records(frame)

    def upsert_session_rows(
        self,
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        return self._upsert(
            self.session_table,
            SESSION_TRAJECTORY_METRICS_SCHEMA,
            SESSION_TRAJECTORY_METRICS_UPSERT_KEYS,
            rows,
        )

    def upsert_category_rows(
        self,
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        return self._upsert(
            self.category_table,
            CATEGORY_TRAJECTORY_STATS_SCHEMA,
            CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS,
            rows,
        )

    def delete_category_rows(self, keys: Iterable[CategoryKey]) -> int:
        """Delete exact category keys; never clear the whole category table."""
        normalized = sorted({_category_key(key) for key in keys})
        if not normalized:
            return 0
        self._verify_table(self.category_table, CATEGORY_TRAJECTORY_STATS_SCHEMA)
        predicates = []
        for key in normalized:
            predicates.append(
                "(" + " AND ".join(
                    f"{column} = '{_escape_sql(value)}'"
                    for column, value in zip(CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS, key)
                ) + ")"
            )
        self.session.delete(self.category_table, " OR ".join(predicates))
        return len(normalized)

    def _upsert(
        self,
        table_name: str,
        schema: pa.Schema,
        keys: tuple[str, ...],
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        materialized = [dict(row) for row in rows]
        if not materialized:
            return 0
        if table_name == self.session_table:
            self._verify_table(
                table_name,
                schema,
                partition_column=SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN,
                partition_type=SESSION_TRAJECTORY_METRICS_PARTITION_TYPE,
                partitions=SESSION_TRAJECTORY_METRICS_PARTITIONS,
            )
        else:
            self._verify_table(table_name, schema)
        arrow_table = pa.Table.from_pylist(materialized, schema=schema)
        frame = arrow_table.to_pandas(types_mapper=pd.ArrowDtype)
        self.session.upsert(table_name, columns=list(keys), datas=frame)
        return len(materialized)

    def _verify_table(
        self,
        table_name: str,
        expected_schema: pa.Schema,
        *,
        partition_column: Optional[str] = None,
        partition_type: Optional[str] = None,
        partitions: Optional[int] = None,
    ) -> None:
        if not self.session.table_exists(table_name):
            raise ValueError(
                f"Trajectory statistics table {table_name!r} does not exist in "
                f"{self.db_uri}; run the initialization script first"
            )
        if self.session.get_schema(table_name) != expected_schema:
            raise ValueError(f"Table {table_name!r} schema does not match the SDK schema")
        record = self.session.schema_table.get(table_name)
        if record is None:
            raise ValueError(f"Table {table_name!r} has no dldb schema metadata")
        actual_column = getattr(record, "partition_column", None) or None
        actual_type = str(getattr(record, "partition_type", None) or "").upper() or None
        expected_type = partition_type.upper() if partition_type else None
        if actual_column != partition_column or actual_type != expected_type:
            raise ValueError(f"Table {table_name!r} partitioning does not match")
        if partitions is not None and getattr(record, "partitions", None) != partitions:
            raise ValueError(f"Table {table_name!r} bucket count does not match")

    def close(self) -> None:
        self.session.shutdown()

    def __enter__(self) -> "TrajectoryStatsStore":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        _ = exc_type, exc_value, traceback
        self.close()


def _frame_records(frame) -> list[dict[str, object]]:
    if frame is None or frame.empty:
        return []
    return [
        {key: _none_if_missing(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _none_if_missing(value):
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _category_key(key) -> CategoryKey:
    values = tuple(key)
    if len(values) != len(CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS):
        raise ValueError("category key must contain five dimensions")
    if not all(isinstance(value, str) for value in values):
        raise ValueError("category key dimensions must be strings")
    return values


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")
