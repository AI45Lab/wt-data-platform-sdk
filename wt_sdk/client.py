import re
import sqlite3
import tempfile
import time
from typing import List, Optional, Union, Dict, Any, Iterator
from loguru import logger
import dldb
import pandas as pd
import wt_sdk._time as sdk_time
from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
)
from wt_sdk.dldb_timing import (
    append_dldb_metrics_log,
    build_dldb_timing_payload,
    extract_dldb_last_call,
    extract_dldb_timing_from_df,
    format_dldb_metrics_summary,
    format_dldb_timing_log,
)
from wt_sdk.core.schemas import (
    LANDING_SCALAR_INDEXES,
    LANDING_PARTITIONS,
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    SERVING_PARTITION_COLUMN,
    SERVING_SCALAR_INDEXES,
)
from wt_sdk.models import (
    LandingRecord,
    ServingRecord,
    LandingRecordBatch,
    ServingRecordBatch,
)
from wt_sdk.utils import (
    landing_record_to_dataframe,
    landing_batch_to_dataframe,
    serving_record_to_dataframe,
    serving_batch_to_dataframe,
    dataframe_to_dict_records,
    deserialize_json_columns,
    dataframe_to_landing_records,
    dataframe_to_serving_records,
)


class WTGatewayClient:
    """Business client for dldb-backed landing and serving tables.

    See README.md for configuration, API examples, and partition constraints.
    """

    # Logical partition keys. Runtime methods prefer the dldb table metadata
    # when it is available, so existing dt-partitioned tables still work.
    LANDING_PARTITION_KEY = LANDING_PARTITION_COLUMN
    SERVING_PARTITION_KEY = SERVING_PARTITION_COLUMN

    def __init__(self, config: Optional[GatewayConfig] = None):
        """Initialize with explicit config or fresh environment-based defaults."""
        self.config = config or GatewayConfig()
        self._enable_dldb_timing_logs = self.config.resolved_enable_dldb_timing_logs()
        self._dldb_model = self.config.resolved_dldb_model()
        self._dldb_metrics_log_path = self.config.resolved_dldb_metrics_log_path()

        # Pass db_name as first positional argument, then other config as kwargs
        dldb_config = self.config.to_dldb_config()
        self.session = dldb.connect(
            self.config.tables.db_uri,
            **dldb_config
        )

        self.landing_uri = self.config.tables.landing_uri()
        self.serving_uri = self.config.tables.serving_uri()
        logger.info(f"WTGatewayClient initialized")
        logger.info(f"Landing table URI: {self.landing_uri}")
        logger.info(f"Serving table URI: {self.serving_uri}")

    def _extract_dldb_timing_from_df(self, df: Optional[pd.DataFrame]) -> Optional[Dict[str, Any]]:
        return extract_dldb_timing_from_df(df)

    def _extract_dldb_last_call(self) -> Optional[Dict[str, Any]]:
        return extract_dldb_last_call(self.session)

    def _log_dldb_timing(
        self,
        api_name: str,
        timing: Optional[Dict[str, Any]],
        *,
        table_name: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self._enable_dldb_timing_logs and not self._dldb_metrics_log_path:
            return

        payload = build_dldb_timing_payload(
            api_name,
            timing,
            table_name=table_name,
            extra=extra,
        )
        append_dldb_metrics_log(self._dldb_metrics_log_path, "dldb_timing", payload)

        if not self._enable_dldb_timing_logs:
            return

        log_line = format_dldb_timing_log(
            api_name,
            timing,
            table_name=table_name,
            extra=extra,
        )
        if log_line:
            logger.info(log_line)

    def _log_dldb_metrics_summary(self, summary: Optional[Dict[str, Any]]) -> None:
        append_dldb_metrics_log(self._dldb_metrics_log_path, "dldb_metrics_summary", summary)

        if not self.config.log_dldb_metrics_summary_on_close:
            return

        log_line = format_dldb_metrics_summary(summary)
        if log_line:
            logger.info(log_line)

    def _filter_table(
        self,
        table_name: str,
        query: str,
        limit: Optional[int] = None,
        columns: Optional[List[str]] = None,
        offset: Optional[int] = None,
        *,
        partitions: Optional[list] = None,
        partition_cond: Optional[str] = None,
        order_by: Optional[str] = None,
        ascending: bool = True,
        checkout_latest: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        self._pin_exact_dldb_table(table_name)
        df = self.session.filter(
            table_name,
            query=query,
            limit=limit,
            columns=columns,
            offset=offset,
            partitions=partitions,
            partition_cond=partition_cond,
            order_by=order_by,
            ascending=ascending,
            checkout_latest=checkout_latest,
        )
        timing = self._extract_dldb_timing_from_df(df) or self._extract_dldb_last_call()
        log_extra = {
            "limit": limit,
            "order_by": order_by,
            "ascending": ascending,
            "partition_cond": partition_cond,
            "checkout_latest": checkout_latest if checkout_latest else None,
            "columns_count": len(columns) if columns else None,
            "partitions_count": len(partitions) if partitions else None,
        }
        if extra:
            log_extra.update(extra)
        self._log_dldb_timing("filter", timing, table_name=table_name, extra=log_extra)
        return df

    def _count_rows(self, table_name: str, partition: Optional[str] = None) -> int:
        self._pin_exact_dldb_table(table_name)
        count = self.session.count_rows(table_name, partition)
        self._log_dldb_timing(
            "count_rows",
            self._extract_dldb_last_call(),
            table_name=table_name,
            extra={"partition": partition},
        )
        return count

    # ==================== Private Helper Methods ====================

    def _get_partition_key_for_table(self, table_name: str, fallback: str) -> str:
        """Return the partition key recorded in dldb metadata, if available."""
        try:
            schema_table = getattr(self.session, "schema_table", None)
            record = schema_table.get(table_name) if schema_table is not None else None
            if record is not None and record.partition_column:
                return record.partition_column
        except Exception:
            pass
        return fallback

    def _get_partition_metadata_for_table(self, table_name: str, fallback_key: str) -> Dict[str, Any]:
        """Return dldb partition metadata, falling back to SDK constants."""
        metadata = {
            "partition_column": fallback_key,
            "partition_type": LANDING_PARTITION_TYPE if fallback_key == self.LANDING_PARTITION_KEY else "VALUE",
            "partitions": LANDING_PARTITIONS if fallback_key == self.LANDING_PARTITION_KEY else None,
        }
        try:
            schema_table = getattr(self.session, "schema_table", None)
            record = schema_table.get(table_name) if schema_table is not None else None
            if record is None:
                return metadata
            metadata["partition_column"] = getattr(record, "partition_column", None) or metadata["partition_column"]
            metadata["partition_type"] = getattr(record, "partition_type", None) or metadata["partition_type"]
            record_partitions = getattr(record, "partitions", None)
            if isinstance(record_partitions, int) and record_partitions > 0:
                metadata["partitions"] = record_partitions
        except Exception:
            pass
        return metadata

    def _pin_exact_dldb_table(self, table_name: str) -> None:
        """Open the exact logical table by dldb metadata, avoiding prefix collisions."""
        try:
            schema_table = getattr(self.session, "schema_table", None)
            record = schema_table.get(table_name) if schema_table is not None else None
            if record is None:
                return
            from dldb.table import open_table_by_partition_type
            self.session.tables[table_name] = open_table_by_partition_type(
                self.session.db_conn,
                self.session.schema_table,
                table_name,
                record.partition_type,
            )
        except Exception as exc:
            logger.debug(f"Failed to pin exact dldb table '{table_name}': {exc}")

    def _extract_partition_values_from_query(self, query: str, partition_key: str) -> Optional[List[str]]:
        """Extract simple equality/IN filters for the partition key from SQL text."""
        if not query or not partition_key:
            return None

        quoted_key = re.escape(partition_key)
        eq_pattern = rf"(?<![\w.]){quoted_key}\s*=\s*'([^']+)'"
        in_pattern = rf"(?<![\w.]){quoted_key}\s+IN\s*\(([^)]*)\)"

        partitions: List[str] = []
        partitions.extend(re.findall(eq_pattern, query, flags=re.IGNORECASE))

        for raw_values in re.findall(in_pattern, query, flags=re.IGNORECASE):
            partitions.extend(re.findall(r"'([^']+)'", raw_values))

        if not partitions:
            return None

        seen = set()
        return [value for value in partitions if not (value in seen or seen.add(value))]

    def _resolve_partitions_for_query(self, table_name: str, query: str, fallback_key: str) -> Optional[list]:
        """Resolve query partition filters into dldb partition arguments."""
        metadata = self._get_partition_metadata_for_table(table_name, fallback_key)
        partition_key = metadata.get("partition_column")
        partition_type = str(metadata.get("partition_type") or "").upper()
        values = self._extract_partition_values_from_query(query, partition_key)
        if not values:
            return None

        if partition_type == "VALUE":
            return values

        if partition_type == "HASH":
            partitions_count = metadata.get("partitions")
            if not isinstance(partitions_count, int) or partitions_count <= 0:
                logger.warning(
                    f"Cannot prune HASH partition for table '{table_name}': invalid partitions={partitions_count}"
                )
                return None
            try:
                from dldb.utils import stable_hash
            except Exception as exc:
                logger.warning(f"Cannot import dldb.utils.stable_hash for HASH partition pruning: {exc}")
                return None
            return sorted({stable_hash(value) % partitions_count for value in values})

        return None

    def _resolve_explicit_partition_for_table(
        self,
        table_name: str,
        partition: Optional[Union[str, int]],
        fallback_key: str,
    ) -> Optional[Union[str, int]]:
        """Convert a caller-supplied logical partition value to dldb's partition argument."""
        if partition is None:
            return None

        metadata = self._get_partition_metadata_for_table(table_name, fallback_key)
        partition_type = str(metadata.get("partition_type") or "").upper()

        if partition_type == "HASH":
            if isinstance(partition, int):
                return partition

            partitions_count = metadata.get("partitions")
            if not isinstance(partitions_count, int) or partitions_count <= 0:
                logger.warning(
                    f"Cannot resolve HASH partition for table '{table_name}': invalid partitions={partitions_count}"
                )
                return partition
            try:
                from dldb.utils import stable_hash
            except Exception as exc:
                logger.warning(f"Cannot import dldb.utils.stable_hash for HASH partition resolution: {exc}")
                return partition
            return stable_hash(str(partition)) % partitions_count

        return partition

    def _list_existing_partitions_for_table(self, table_name: str) -> List[Union[str, int]]:
        """List existing dldb logical partitions/buckets for a table."""
        self._pin_exact_dldb_table(table_name)
        table = self.session.tables.get(table_name)
        if table is None or not hasattr(table, "list_partitions"):
            return []
        return sorted(table.list_partitions())

    def _is_hash_partition_table(self, table_name: str, fallback_key: str) -> bool:
        metadata = self._get_partition_metadata_for_table(table_name, fallback_key)
        return str(metadata.get("partition_type") or "").upper() == "HASH"

    def _escape_sql_string(self, value: str) -> str:
        return value.replace("'", "''")

    def _add_hash_partition_filter_for_raw_value(
        self,
        table_name: str,
        query: str,
        partition: Optional[Union[str, int]],
        fallback_key: str,
    ) -> str:
        """Add a job_id predicate when a raw HASH partition value is supplied."""
        if partition is None or isinstance(partition, int):
            return query
        if not self._is_hash_partition_table(table_name, fallback_key):
            return query

        metadata = self._get_partition_metadata_for_table(table_name, fallback_key)
        partition_key = metadata.get("partition_column") or fallback_key
        if self._extract_partition_values_from_query(query, partition_key):
            return query

        partition_filter = f"{partition_key} = '{self._escape_sql_string(str(partition))}'"
        if query and query.strip():
            return f"({query}) AND {partition_filter}"
        return partition_filter

    def _resolve_landing_query_partitions(self, table_name: str, query: str) -> Optional[list]:
        """Resolve job_id partitions for landing-like tables when the query contains job_id."""
        metadata = self._get_partition_metadata_for_table(table_name, self.LANDING_PARTITION_KEY)
        if metadata.get("partition_column") != self.LANDING_PARTITION_KEY:
            return None
        return self._resolve_partitions_for_query(table_name, query, self.LANDING_PARTITION_KEY)

    def _get_table_info(self, table: str) -> Dict[str, Any]:
        """Return metadata and record converters for landing or serving."""
        if table == "landing":
            table_name = self.config.tables.landing_table
            self._pin_exact_dldb_table(table_name)
            return {
                "table_name": table_name,
                "partition_key": self._get_partition_metadata_for_table(table_name, self.LANDING_PARTITION_KEY)["partition_column"],
                "to_dataframe_single": landing_record_to_dataframe,
                "to_dataframe_batch": landing_batch_to_dataframe,
                "from_dataframe": dataframe_to_landing_records,
                "record_type": "LandingRecord",
            }
        elif table == "serving":
            table_name = self.config.tables.serving_table
            self._pin_exact_dldb_table(table_name)
            return {
                "table_name": table_name,
                "partition_key": self._get_partition_key_for_table(table_name, self.SERVING_PARTITION_KEY),
                "to_dataframe_single": serving_record_to_dataframe,
                "to_dataframe_batch": serving_batch_to_dataframe,
                "from_dataframe": dataframe_to_serving_records,
                "record_type": "ServingRecord",
            }
        else:
            raise ValueError(f"Unknown table: {table}. Must be 'landing' or 'serving'")

    def _ingest(
        self,
        table: str,
        record_or_batch: Union[LandingRecord, ServingRecord, List, LandingRecordBatch, ServingRecordBatch]
    ) -> None:
        from wt_sdk.utils import landing_record_to_arrow, landing_batch_to_arrow, serving_record_to_arrow, serving_batch_to_arrow
        from wt_sdk.core.schemas import LANDING_SCHEMA, SERVING_SCHEMA

        if isinstance(record_or_batch, list) and not record_or_batch:
            logger.warning(f"Empty records list, skipping ingestion to {table}")
            return
        if isinstance(record_or_batch, (LandingRecordBatch, ServingRecordBatch)) and len(record_or_batch) == 0:
            logger.warning(f"Empty record batch, skipping ingestion to {table}")
            return

        if table == "landing":
            record_or_batch = self._without_serving_publish_time(record_or_batch)
        else:
            record_or_batch = self._with_serving_publish_time(record_or_batch)

        info = self._get_table_info(table)
        schema = LANDING_SCHEMA if table == "landing" else SERVING_SCHEMA

        # Convert to PyArrow table with explicit schema
        if isinstance(record_or_batch, list):
            # Create batch object
            if table == "landing":
                batch = LandingRecordBatch(records=record_or_batch)
            else:
                batch = ServingRecordBatch(records=record_or_batch)
            arrow_table = landing_batch_to_arrow(batch, schema) if table == "landing" else serving_batch_to_arrow(batch, schema)
            logger.info(f"Ingested {len(arrow_table)} records to {table} table")
        elif isinstance(record_or_batch, (LandingRecordBatch, ServingRecordBatch)):
            arrow_table = landing_batch_to_arrow(record_or_batch, schema) if table == "landing" else serving_batch_to_arrow(record_or_batch, schema)
            logger.info(f"Ingested {len(arrow_table)} records to {table} table")
        else:
            # Single record
            arrow_table = landing_record_to_arrow(record_or_batch, schema) if table == "landing" else serving_record_to_arrow(record_or_batch, schema)
            logger.debug(f"Ingested single record to {table} table: {record_or_batch.id}")

        # Convert Arrow table to DataFrame (preserves Arrow schema) and pass to DLDB
        df = arrow_table.to_pandas(types_mapper=pd.ArrowDtype)
        self.session.add(info["table_name"], df)
        self._log_dldb_timing(
            "add",
            self._extract_dldb_last_call(),
            table_name=info["table_name"],
            extra={"input_rows": len(arrow_table)},
        )
        logger.info(f"Successfully added {len(arrow_table)} records to {table} table via DLDB wrapper")

    def _without_serving_publish_time(
        self,
        record_or_batch: Union[LandingRecord, List[LandingRecord], LandingRecordBatch],
    ) -> Union[LandingRecord, List[LandingRecord], LandingRecordBatch]:
        """Copy landing records while enforcing a null serving publication time."""
        def landing_copy(record: LandingRecord) -> LandingRecord:
            return record.model_copy(update={"serving_updated_at": None})

        if isinstance(record_or_batch, list):
            return [landing_copy(record) for record in record_or_batch]
        if isinstance(record_or_batch, LandingRecordBatch):
            return LandingRecordBatch(
                records=[landing_copy(record) for record in record_or_batch.records]
            )
        return landing_copy(record_or_batch)

    def _with_serving_publish_time(
        self,
        record_or_batch: Union[ServingRecord, List[ServingRecord], ServingRecordBatch],
    ) -> Union[ServingRecord, List[ServingRecord], ServingRecordBatch]:
        """Copy serving records and stamp one SDK-managed publication time."""
        publish_time = sdk_time.now_ms()

        def stamped(record: ServingRecord) -> ServingRecord:
            return record.model_copy(update={"serving_updated_at": publish_time})

        if isinstance(record_or_batch, list):
            return [stamped(record) for record in record_or_batch]
        if isinstance(record_or_batch, ServingRecordBatch):
            return ServingRecordBatch(
                records=[stamped(record) for record in record_or_batch.records]
            )
        return stamped(record_or_batch)

    def _upsert_serving_batch(
        self,
        records: Union[List[ServingRecord], ServingRecordBatch],
    ) -> None:
        """Upsert a validated serving batch by globally unique business ID."""
        from wt_sdk.core.schemas import SERVING_SCHEMA
        from wt_sdk.utils import serving_batch_to_arrow

        source_records = records.records if isinstance(records, ServingRecordBatch) else records
        if not source_records:
            logger.warning("Empty records list, skipping serving upsert")
            return

        missing_job_ids = [record.id for record in source_records if not str(record.job_id or "").strip()]
        if missing_job_ids:
            raise ValueError(
                "serving upsert requires a non-empty job_id for every record; "
                f"missing for IDs: {', '.join(missing_job_ids)}"
            )

        seen_ids = set()
        duplicate_ids = set()
        for record in source_records:
            if record.id in seen_ids:
                duplicate_ids.add(record.id)
            seen_ids.add(record.id)
        if duplicate_ids:
            raise ValueError(
                "serving upsert batch contains duplicate IDs: "
                f"{', '.join(sorted(duplicate_ids))}"
            )

        stamped = self._with_serving_publish_time(
            ServingRecordBatch(records=list(source_records))
        )
        arrow_table = serving_batch_to_arrow(stamped, SERVING_SCHEMA)
        dataframe = arrow_table.to_pandas(types_mapper=pd.ArrowDtype)
        info = self._get_table_info("serving")

        self.session.upsert(
            info["table_name"],
            columns=["id"],
            datas=dataframe,
        )
        self._log_dldb_timing(
            "upsert",
            self._extract_dldb_last_call(),
            table_name=info["table_name"],
            extra={"input_rows": len(arrow_table), "api": "upsert_serving_batch"},
        )
        logger.info(
            f"Successfully upserted {len(arrow_table)} records to serving table "
            "via DLDB wrapper"
        )

    def _query(
        self,
        table: str,
        filter_query: str = "",
        limit: Optional[int] = None,
        columns: Optional[List[str]] = None,
        partition: Optional[str] = None
    ) -> Union[List[LandingRecord], List[ServingRecord]]:
        """Internal generic query path for landing and serving tables."""
        info = self._get_table_info(table)
        effective_query = filter_query
        partitions = None
        if partition is not None:
            fallback_key = self.LANDING_PARTITION_KEY if table == "landing" else self.SERVING_PARTITION_KEY
            resolved_partition = self._resolve_explicit_partition_for_table(
                info["table_name"],
                partition,
                fallback_key,
            )
            partitions = [resolved_partition]
            effective_query = self._add_hash_partition_filter_for_raw_value(
                info["table_name"],
                filter_query,
                partition,
                fallback_key,
            )
        else:
            fallback_key = self.LANDING_PARTITION_KEY if table == "landing" else self.SERVING_PARTITION_KEY
            partitions = self._resolve_partitions_for_query(
                info["table_name"],
                filter_query,
                fallback_key,
            )

        df = self._filter_table(
            info["table_name"],
            effective_query,
            limit,
            columns,
            partitions=partitions,
            extra={"partition": partition},
        )
        records = info["from_dataframe"](df)
        logger.info(f"Queried {table} table: {len(records)} results" + (f" (partition={partition})" if partition else ""))
        return records

    def _count(self, table: str, partition: Optional[str] = None) -> int:
        """Count rows for one logical table or one resolved partition."""
        info = self._get_table_info(table)
        resolved_partition = None
        if partition is not None:
            fallback_key = self.LANDING_PARTITION_KEY if table == "landing" else self.SERVING_PARTITION_KEY
            resolved_partition = self._resolve_explicit_partition_for_table(
                info["table_name"],
                partition,
                fallback_key,
            )
            if self._is_hash_partition_table(info["table_name"], fallback_key) and not isinstance(partition, int):
                query = self._add_hash_partition_filter_for_raw_value(
                    info["table_name"],
                    "",
                    partition,
                    fallback_key,
                )
                df = self._filter_table(
                    info["table_name"],
                    query=query,
                    limit=None,
                    columns=["id"],
                    partitions=[resolved_partition],
                    extra={"partition": partition, "api": "count"},
                )
                count = len(df)
                logger.info(f"{table.capitalize()} table count: {count}" + (f" (partition={partition})" if partition else ""))
                return count
        count = self._count_rows(info["table_name"], resolved_partition)
        logger.info(f"{table.capitalize()} table count: {count}" + (f" (partition={partition})" if partition else ""))
        return count

    def _check_existing_records(
        self,
        table: str,
        record_ids: List[str]
    ) -> set:
        """Return existing IDs in batches for idempotency checks."""
        if not record_ids:
            return set()

        existing = set()

        # Batch IDs to stay within practical SQL query limits.
        batch_size = 1000
        for i in range(0, len(record_ids), batch_size):
            batch = record_ids[i:i + batch_size]
            ids_str = ", ".join([f"'{rec_id}'" for rec_id in batch])
            filter_query = f"id IN ({ids_str})"

            try:
                results = self._query(table, filter_query, limit=len(batch))
                for record in results:
                    if hasattr(record, 'id'):
                        existing.add(record.id)
                    elif isinstance(record, dict):
                        existing.add(record.get('id'))
            except Exception as e:
                # Empty or uninitialized tables have no existing IDs.
                error_msg = str(e)
                if "not exist" in error_msg:
                    logger.debug(f"Table {table} has no partitions yet, skipping duplicate check for {len(batch)} records")
                else:
                    logger.warning(f"Error checking batch of {len(batch)} records: {e}")

        logger.info(f"Checked {len(record_ids)} records, found {len(existing)} existing")
        return existing

    def _delete(self, table: str, filter_query: str) -> int:
        info = self._get_table_info(table)
        fallback_key = self.LANDING_PARTITION_KEY if table == "landing" else self.SERVING_PARTITION_KEY
        partitions = self._resolve_partitions_for_query(
            info["table_name"],
            filter_query,
            fallback_key,
        )

        # dldb does not return delete counts, so count before deletion.
        try:
            df = self._filter_table(
                info["table_name"],
                query=filter_query,
                limit=None,
                columns=["id"],  # Only need id for counting
                partition_cond=None,
                partitions=partitions,
            )
            count_before = len(df)
        except Exception as e:
            logger.debug(f"Error counting records: {e}")
            count_before = 0

        if count_before > 0:
            if partitions:
                for partition in partitions:
                    self.session.delete(info["table_name"], filter_query, partition=partition)
            else:
                self.session.delete(info["table_name"], filter_query)
            self._log_dldb_timing(
                "delete",
                self._extract_dldb_last_call(),
                table_name=info["table_name"],
                extra={"deleted_rows": count_before, "partitions_count": len(partitions) if partitions else None},
            )
            logger.info(f"Deleted {count_before} records from {table} table")

        return count_before

    # ==================== Landing Table Operations ====================

    def ingest_landing(self, record: LandingRecord) -> None:
        self._ingest("landing", record)

    def ingest_landing_batch(
        self,
        records: Union[List[LandingRecord], LandingRecordBatch]
    ) -> None:
        self._ingest("landing", records)

    def query_data(
        self,
        filter_query: str = "",
        limit: Optional[int] = None,
        columns: Optional[List[str]] = None,
        partition: Optional[Union[str, int]] = None,
        order_by: Optional[str] = None,
        ascending: bool = True,
        checkout_latest: bool = False,
        table: Optional[str] = None,
        exclude_none: bool = True,
        deserialize_json: bool = False,
    ) -> List[Dict[str, Any]]:
        """Query landing (default) or a named table.

        Returns dictionaries with null table columns omitted by default. Opaque
        JSON strings are returned unchanged unless deserialize_json=True.
        Include job_id for HASH pruning, especially with order_by or limit.
        """
        table_name = table or self.config.tables.landing_table
        effective_query = filter_query if filter_query and filter_query.strip() else "id IS NOT NULL"
        if partition is not None:
            effective_query = self._add_hash_partition_filter_for_raw_value(
                table_name,
                effective_query,
                partition,
                self.LANDING_PARTITION_KEY,
            )
            resolved_partition = self._resolve_explicit_partition_for_table(
                table_name,
                partition,
                self.LANDING_PARTITION_KEY,
            )
            partitions = [resolved_partition]
        else:
            partitions = self._resolve_partitions_for_query(
                table_name,
                effective_query,
                self.LANDING_PARTITION_KEY,
            )

        df = self._filter_table(
            table_name,
            query=effective_query,
            limit=limit,
            columns=columns,
            partitions=partitions,
            order_by=order_by,
            ascending=ascending,
            checkout_latest=checkout_latest,
            extra={
                "partition": partition,
                "api": "query_data",
            },
        )
        records = dataframe_to_dict_records(
            df,
            exclude_none=exclude_none,
            deserialize_json=deserialize_json,
        )
        logger.info(f"Queried table {table_name}: {len(records)} results")
        return records

    def update_landing(
        self,
        filter_query: str,
        updates: Dict[str, Any],
        partition: Optional[Union[str, int]] = None,
        *,
        touch_source_updated_at: bool = True,
    ) -> Dict[str, Any]:
        """Update matching landing rows and return an execution acknowledgement.

        SDK-managed timestamps, id, created_at, and job_id are immutable to
        callers. Source time is refreshed by default. Include job_id for HASH
        pruning.
        """
        if not filter_query or not filter_query.strip():
            raise ValueError("filter_query is required for update_landing")
        if not updates:
            raise ValueError("updates is required for update_landing")
        if type(touch_source_updated_at) is not bool:
            raise TypeError("touch_source_updated_at must be a bool")

        protected_columns = {
            "id",
            "created_at",
            "job_id",
            "source_updated_at",
            "serving_updated_at",
        }
        protected_updates = protected_columns.intersection(updates)
        if protected_updates:
            columns = ", ".join(sorted(protected_updates))
            raise ValueError(f"update_landing cannot update protected columns: {columns}")

        effective_updates = dict(updates)
        if touch_source_updated_at:
            effective_updates["source_updated_at"] = sdk_time.now_ms()

        info = self._get_table_info("landing")
        effective_filter_query = self._add_hash_partition_filter_for_raw_value(
            info["table_name"],
            filter_query,
            partition,
            self.LANDING_PARTITION_KEY,
        )
        update_partition = self._resolve_explicit_partition_for_table(
            info["table_name"],
            partition,
            self.LANDING_PARTITION_KEY,
        )
        resolved_partitions = None
        if update_partition is None:
            resolved_partitions = self._resolve_partitions_for_query(
                info["table_name"],
                effective_filter_query,
                self.LANDING_PARTITION_KEY,
            )
            if resolved_partitions and len(resolved_partitions) == 1:
                update_partition = resolved_partitions[0]

        result = self.session.update(
            info["table_name"],
            effective_filter_query,
            effective_updates,
            partition=update_partition,
        )
        self._log_dldb_timing(
            "update",
            self._extract_dldb_last_call(),
            table_name=info["table_name"],
            extra={
                "api": "update_landing",
                "updated_fields": len(effective_updates),
                "partition": update_partition,
                "partitions_count": len(resolved_partitions) if resolved_partitions else None,
            },
        )
        logger.info(
            f"Submitted landing update: table={info['table_name']}, "
            f"fields={list(effective_updates.keys())}, partition={update_partition}"
        )
        return {
            "updated": True,
            "table_name": info["table_name"],
            "partition": update_partition,
            "updated_fields": sorted(updates.keys()),
            "effective_updated_fields": sorted(effective_updates.keys()),
            "source_updated_at_touched": touch_source_updated_at,
            "dldb_result": result,
        }

    def count_landing(self, partition: Optional[str] = None) -> int:
        return self._count("landing", partition)

    def delete_landing(self, filter_query: str) -> int:
        return self._delete("landing", filter_query)

    # ==================== Serving Table Operations ====================

    def ingest_serving(self, record: ServingRecord) -> None:
        self._ingest("serving", record)

    def ingest_serving_batch(
        self,
        records: Union[List[ServingRecord], ServingRecordBatch]
    ) -> None:
        self._ingest("serving", records)

    def upsert_serving(self, record: ServingRecord) -> None:
        """Insert or replace one serving record matched by business ID."""
        self._upsert_serving_batch([record])

    def upsert_serving_batch(
        self,
        records: Union[List[ServingRecord], ServingRecordBatch],
    ) -> None:
        """Insert or replace serving records matched by business ID."""
        self._upsert_serving_batch(records)

    def count_serving(self, partition: Optional[str] = None) -> int:
        return self._count("serving", partition)

    def delete_serving(self, filter_query: str) -> int:
        return self._delete("serving", filter_query)

    def get_tags_distribution(self, table: Optional[str] = None) -> Dict[str, int]:
        """Return tag occurrence counts for the serving table or a named table."""
        table_name = table or self.config.tables.serving_table

        logger.info(f"Getting tags distribution from table '{table_name}'")

        df = self._filter_table(
            table_name,
            query="id IS NOT NULL",  # dldb rejects an empty WHERE expression
            limit=None,  # No limit
            columns=["tags"],  # Only fetch tags column for efficiency
            partition_cond=None,  # All partitions
        )

        tag_counts: Dict[str, int] = {}
        for tags_list in df["tags"]:
            if tags_list is not None:
                for tag in tags_list:
                    if tag:  # Skip empty strings
                        tag_counts[tag] = tag_counts.get(tag, 0) + 1

        logger.info(f"Found {len(tag_counts)} unique tags in table '{table_name}'")
        return tag_counts

    # ==================== Data Reading Methods ====================

    def iter_data_batches(
        self,
        dataset_type: str,
        where_sql: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        chunk_size: int = 10000,
        order_by: str = "created_at",
        ascending: bool = True,
        table: Optional[str] = None,
        deserialize_json: bool = False,
    ) -> Iterator[pd.DataFrame]:
        """Iterate over DataFrame batches, optionally decoding JSON columns."""
        table_name = table or self.config.tables.landing_table
        partition_metadata = self._get_partition_metadata_for_table(table_name, self.LANDING_PARTITION_KEY)
        partition_key = partition_metadata["partition_column"]
        from datetime import datetime

        filters = [f"dataset_type = '{dataset_type}'"]

        if start_time is not None:
            filters.append(f"created_at >= {start_time}")
        if end_time is not None:
            filters.append(f"created_at <= {end_time}")
        if where_sql:
            filters.append(f"({where_sql})")

        # Legacy dt tables still use date-range pruning.
        partition_cond = None
        static_partitions = None
        if partition_key == "dt" and (start_time or end_time):
            dt_parts = []
            if start_time:
                start_dt = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d')
                dt_parts.append(f"dt >= '{start_dt}'")
            if end_time:
                end_dt = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d')
                dt_parts.append(f"dt <= '{end_dt}'")
            if dt_parts:
                partition_cond = " AND ".join(dt_parts)
        else:
            static_filter = " AND ".join(filters)
            static_partitions = self._resolve_partitions_for_query(table_name, static_filter, self.LANDING_PARTITION_KEY)

        cursor = None

        while True:
            query_filters = filters.copy()
            if cursor is not None:
                query_filters.append(f"created_at > {cursor}")

            query = " AND ".join(query_filters)

            logger.info(f"Fetching data from table '{table_name}': filter={query}, chunk_size={chunk_size}")

            df = self._filter_table(
                table_name,
                query=query,
                limit=chunk_size,
                partitions=static_partitions,
                partition_cond=partition_cond,
                order_by=order_by,
                ascending=ascending,
            )

            if df is None or len(df) == 0:
                break

            output_df = deserialize_json_columns(df) if deserialize_json else df
            yield output_df
            logger.debug(f"Yielded batch: {len(df)} rows")

            cursor = int(df["created_at"].iloc[-1])

            if len(df) < chunk_size:
                break

    def export_data_batches(
        self,
        filter_query: str = "",
        batch_size: int = 10000,
        columns: Optional[List[str]] = None,
        table: Optional[str] = None,
        deserialize_json: bool = False,
    ) -> Iterator[pd.DataFrame]:
        """Export a fixed set of matching rows in verified batches.

        Defaults to serving. Keep matching rows unchanged until iteration finishes;
        otherwise the export fails. JSON columns are decoded only when requested.
        """
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if columns is not None and not columns:
            raise ValueError("columns must be None or a non-empty list")

        table_name = table or self.config.tables.serving_table
        effective_query = filter_query if filter_query and filter_query.strip() else "id IS NOT NULL"
        existing_partitions = self._list_existing_partitions_for_table(table_name)
        resolved_partitions = self._resolve_partitions_for_query(
            table_name,
            effective_query,
            self.LANDING_PARTITION_KEY,
        )
        if resolved_partitions is None:
            export_partitions = existing_partitions
        else:
            resolved_set = set(resolved_partitions)
            export_partitions = [
                partition for partition in existing_partitions if partition in resolved_set
            ]

        logger.info(
            f"Building export manifest for table '{table_name}': "
            f"partitions={len(export_partitions)}, batch_size={batch_size}"
        )

        with tempfile.TemporaryDirectory(prefix="wt-sdk-export-") as staging_dir:
            manifest_path = f"{staging_dir}/manifest.sqlite3"
            manifest = sqlite3.connect(manifest_path)
            try:
                manifest.execute(
                    "CREATE TABLE export_ids ("
                    "partition_order INTEGER NOT NULL, "
                    "record_id TEXT NOT NULL UNIQUE"
                    ")"
                )

                # Complete the manifest before yielding anything. The UNIQUE
                # constraint enforces the SDK's global caller-provided ID contract.
                for partition_order, partition in enumerate(export_partitions):
                    id_frame = self._filter_table(
                        table_name,
                        query=effective_query,
                        limit=None,
                        columns=["id"],
                        partitions=[partition],
                        order_by="id",
                        ascending=True,
                        checkout_latest=True,
                        extra={
                            "api": "export_data_batches",
                            "phase": "manifest",
                            "partition": partition,
                        },
                    )
                    if "id" not in id_frame.columns:
                        raise RuntimeError(
                            f"Export manifest query did not return the id column for partition {partition!r}"
                        )
                    if id_frame["id"].isna().any():
                        raise RuntimeError(
                            f"Export requires non-null IDs; partition {partition!r} contains a null id"
                        )

                    manifest_rows = [
                        (partition_order, str(record_id))
                        for record_id in id_frame["id"].tolist()
                    ]
                    try:
                        manifest.executemany(
                            "INSERT INTO export_ids (partition_order, record_id) VALUES (?, ?)",
                            manifest_rows,
                        )
                    except sqlite3.IntegrityError as exc:
                        raise RuntimeError(
                            "Export requires globally unique IDs, but duplicate IDs were found "
                            f"while scanning partition {partition!r}"
                        ) from exc
                manifest.commit()

                total_rows = manifest.execute("SELECT COUNT(*) FROM export_ids").fetchone()[0]
                logger.info(
                    f"Export manifest ready for table '{table_name}': {total_rows} rows"
                )

                query_columns = None
                if columns is not None:
                    query_columns = list(columns)
                    if "id" not in query_columns:
                        query_columns.append("id")

                for partition_order, partition in enumerate(export_partitions):
                    cursor = manifest.execute(
                        "SELECT record_id FROM export_ids "
                        "WHERE partition_order = ? ORDER BY record_id",
                        (partition_order,),
                    )
                    while True:
                        requested_ids = [row[0] for row in cursor.fetchmany(batch_size)]
                        if not requested_ids:
                            break

                        escaped_ids = ", ".join(
                            f"'{self._escape_sql_string(record_id)}'"
                            for record_id in requested_ids
                        )
                        batch_query = f"({effective_query}) AND id IN ({escaped_ids})"
                        frame = self._filter_table(
                            table_name,
                            query=batch_query,
                            limit=None,
                            columns=query_columns,
                            partitions=[partition],
                            order_by="id",
                            ascending=True,
                            checkout_latest=True,
                            extra={
                                "api": "export_data_batches",
                                "phase": "data",
                                "partition": partition,
                            },
                        )

                        if "id" not in frame.columns:
                            raise RuntimeError(
                                f"Export data query did not return the id column for partition {partition!r}"
                            )
                        returned_ids = [str(record_id) for record_id in frame["id"].tolist()]
                        if len(returned_ids) != len(requested_ids) or set(returned_ids) != set(requested_ids):
                            missing = sorted(set(requested_ids) - set(returned_ids))
                            unexpected = sorted(set(returned_ids) - set(requested_ids))
                            raise RuntimeError(
                                "Export source changed after manifest capture; discard the partial export "
                                f"and retry. partition={partition!r}, missing_ids={missing[:5]}, "
                                f"unexpected_ids={unexpected[:5]}"
                            )

                        if columns is not None:
                            frame = frame.loc[:, columns]
                        if deserialize_json:
                            frame = deserialize_json_columns(frame)
                        frame.attrs["wt_export"] = {
                            "table": table_name,
                            "partition": partition,
                            "manifest_rows": total_rows,
                        }
                        yield frame
            finally:
                manifest.close()

    def pull_data(
        self,
        dataset_type: str,
        where_sql: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        cursor: Optional[int] = None,
        order_by: str = "created_at",
        ascending: bool = True,
        limit: int = 10000,
        checkout_latest: bool = False,
        table: Optional[str] = None,
        deserialize_json: bool = False,
    ) -> pd.DataFrame:
        """Return one cursor page, optionally decoding JSON columns."""
        table_name = table or self.config.tables.landing_table
        partition_metadata = self._get_partition_metadata_for_table(table_name, self.LANDING_PARTITION_KEY)
        partition_key = partition_metadata["partition_column"]
        from datetime import datetime

        filters = [f"dataset_type = '{dataset_type}'"]

        if cursor is not None:
            filters.append(f"created_at > {cursor}")

        if start_time is not None:
            filters.append(f"created_at >= {start_time}")
        if end_time is not None:
            filters.append(f"created_at <= {end_time}")
        if where_sql:
            filters.append(f"({where_sql})")

        final_filter = " AND ".join(filters)

        # Legacy dt tables still use date-range pruning.
        partition_cond = None
        partitions = None
        if partition_key == "dt" and (start_time or end_time):
            dt_parts = []
            if start_time:
                start_dt = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d')
                dt_parts.append(f"dt >= '{start_dt}'")
            if end_time:
                end_dt = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d')
                dt_parts.append(f"dt <= '{end_dt}'")
            if dt_parts:
                partition_cond = " AND ".join(dt_parts)
        else:
            partitions = self._resolve_partitions_for_query(table_name, final_filter, self.LANDING_PARTITION_KEY)

        logger.info(f"Pulling data from table '{table_name}': filter={final_filter}, cursor={cursor}, limit={limit}")

        df = self._filter_table(
            table_name,
            query=final_filter,
            limit=limit,
            partitions=partitions,
            partition_cond=partition_cond,
            order_by=order_by,
            ascending=ascending,
            checkout_latest=checkout_latest,
        )

        return deserialize_json_columns(df) if deserialize_json else df

    def extract_cursor(self, df: pd.DataFrame) -> Optional[int]:
        """Return the final row's created_at cursor, or None for an empty frame."""
        if df is None or len(df) == 0:
            return None

        last_row = df.iloc[-1]
        created_at = last_row["created_at"]

        return int(created_at)

    def get_max_created_at(
        self,
        where_sql: str,
        table: Optional[str] = None,
        deserialize_json: bool = False,
    ) -> Optional[dict]:
        """Return the latest matching row, optionally decoding JSON columns."""
        table_name = table or self.config.tables.landing_table
        partitions = self._resolve_landing_query_partitions(table_name, where_sql)

        logger.info(f"Getting max record from table '{table_name}': filter={where_sql}")

        df = self._filter_table(
            table_name,
            query=where_sql,
            limit=1,
            partitions=partitions,
            order_by="created_at",
            ascending=False,  # Descending: first row is the max
            extra={"api": "get_max_created_at"},
        )

        if df is None or len(df) == 0:
            logger.info("No matching records found")
            return None

        if deserialize_json:
            df = deserialize_json_columns(df)
        record = df.iloc[0].to_dict()
        logger.info(f"Max record: id={record.get('id')}, created_at={record.get('created_at')}")

        return record

    def search(
        self,
        query: Union[str, List[float]],  # 支持Keyword或Vector
        limit: int = 10,
        tags: List[str] = None,
        where_sql: str = None,
        dataset_type: str = None,
        stream: bool = False,
        table: Optional[str] = None,
        search_fields: List[str] = None,
        deserialize_json: bool = False,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """Filter/search rows and return a DataFrame or one-frame iterator.

        Vector search is unsupported. Keyword search defaults to search_text;
        callers may provide scalar search_fields and optionally decode JSON.
        """
        table_name = table or self.config.tables.serving_table
        if isinstance(query, list):
            raise NotImplementedError(
                "Vector search is NOT supported by DLDB yet. "
                "Please use text keyword search (str) instead of vector embeddings (List[float]). "
                "Request vector-search support from the DLDB platform team if needed."
            )

        filters = []

        if dataset_type:
            filters.append(f"dataset_type = '{dataset_type}'")

        if tags:
            for tag in tags:
                filters.append(f"array_contains(tags, '{tag}')")

        if where_sql:
            filters.append(f"({where_sql})")

        if isinstance(query, str) and query.strip():
            fields_to_search = search_fields or ["search_text"]
            search_conditions = []
            opaque_fields = {
                "messages",
                "response",
                "chosen_trace",
                "rejected_trace",
                "tags",
                "blob_manifest",
            }
            for field in fields_to_search:
                if field in opaque_fields:
                    raise ValueError(
                        f"Keyword search does not support opaque JSON/list field {field!r}; "
                        "use tags= for tag filtering or choose a scalar string field."
                    )
                escaped_query = self._escape_sql_string(query)
                search_conditions.append(f"{field} LIKE '%{escaped_query}%'")

            if search_conditions:
                filters.append(f"({' OR '.join(search_conditions)})")

        final_filter = " AND ".join(filters) if filters else "id IS NOT NULL"
        partitions = self._resolve_landing_query_partitions(table_name, final_filter)

        logger.info(f"Searching table '{table_name}': query={query}, limit={limit}, stream={stream}, dataset_type={dataset_type}")

        df = self._filter_table(
            table_name,
            query=final_filter,
            limit=limit,
            columns=None,
            partitions=partitions,
            partition_cond=None,
            extra={"stream": stream, "dataset_type": dataset_type, "api": "search"},
        )
        if deserialize_json:
            df = deserialize_json_columns(df)

        if stream:
            def result_iterator():
                yield df
            return result_iterator()
        return df

    def get_by_id(
        self,
        record_id: str,
        table: Optional[str] = None,
        exclude_none: bool = True,
        deserialize_json: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Return one row by ID, optionally decoding JSON columns."""
        table_name = table or self.config.tables.serving_table
        filter_query = f"id = '{self._escape_sql_string(record_id)}'"

        logger.debug(f"Looking for record {record_id} in table {table_name}...")
        results = self._filter_table(
            table_name,
            query=filter_query,
            limit=1,
        )

        if len(results) > 0:
            logger.debug(f"Record {record_id} found in table {table_name}")
            return dataframe_to_dict_records(
                results,
                exclude_none=exclude_none,
                deserialize_json=deserialize_json,
            )[0]

        logger.debug(f"Record {record_id} not found in table {table_name}")
        return None

    # ==================== Index Management ====================

    def _resolve_index_partitions(
        self,
        table_name: str,
        fallback_key: str,
        partitions: Optional[Union[str, int, List[Union[str, int]]]],
        *,
        all_partitions: bool,
    ) -> List[int]:
        if all_partitions and partitions is not None:
            raise ValueError("Use either partitions or all_partitions=True, not both")
        if all_partitions:
            return [
                int(partition)
                for partition in self._list_existing_partitions_for_table(table_name)
            ]

        if partitions is None:
            raise ValueError(
                "Index maintenance requires explicit partitions or all_partitions=True"
            )

        if isinstance(partitions, (str, int)):
            requested = [partitions]
        else:
            requested = list(partitions)

        resolved = []
        for partition in requested:
            bucket = self._resolve_explicit_partition_for_table(
                table_name,
                partition,
                fallback_key,
            )
            if not isinstance(bucket, int):
                raise ValueError(
                    f"Index maintenance requires HASH bucket int, got {bucket!r}"
                )
            resolved.append(bucket)

        return sorted(set(resolved))

    def maintain_table_indexes(
        self,
        table_name: str,
        partitions: Optional[Union[str, int, List[Union[str, int]]]] = None,
        *,
        all_partitions: bool = False,
        columns: Optional[List[str]] = None,
        create_missing: bool = True,
        optimize: bool = True,
        cleanup_older_than=None,
        delete_unverified: bool = False,
        retrain: bool = False,
    ) -> Dict[str, Any]:
        """Maintain indexes for one of the four supported trajectory tables.

        Intended for explicit background or operations use, not the write path.
        """
        landing_tables = {DEFAULT_LANDING_TABLE, TEST_LANDING_TABLE}
        serving_tables = {DEFAULT_SERVING_TABLE, TEST_SERVING_TABLE}
        if table_name in landing_tables:
            table_role = "landing"
            fallback_key = self.LANDING_PARTITION_KEY
            configured_indexes = LANDING_SCALAR_INDEXES
        elif table_name in serving_tables:
            table_role = "serving"
            fallback_key = self.SERVING_PARTITION_KEY
            configured_indexes = SERVING_SCALAR_INDEXES
        else:
            supported = sorted(landing_tables | serving_tables)
            raise ValueError(
                f"Unsupported index-maintenance table {table_name!r}; "
                f"expected one of: {', '.join(supported)}"
            )

        metadata = self._get_partition_metadata_for_table(table_name, fallback_key)
        if str(metadata.get("partition_type") or "").upper() != "HASH":
            raise ValueError(
                f"Table index maintenance currently expects HASH partitioning, "
                f"got {metadata.get('partition_type')!r} for {table_name}"
            )

        target_partitions = self._resolve_index_partitions(
            table_name,
            fallback_key,
            partitions,
            all_partitions=all_partitions,
        )

        index_type_by_column = dict(configured_indexes)
        if columns is None:
            index_specs = list(configured_indexes)
        else:
            unknown_columns = sorted(set(columns) - set(index_type_by_column))
            if unknown_columns:
                raise ValueError(
                    f"Columns are not configured for {table_role} index maintenance: "
                    f"{', '.join(unknown_columns)}"
                )
            index_specs = [
                (column, index_type_by_column[column])
                for column in columns
            ]

        summary: Dict[str, Any] = {
            "table_name": table_name,
            "table_role": table_role,
            "partitions": target_partitions,
            "expected_indexes": [f"{column}_idx" for column, _ in index_specs],
            "indexes_created": [],
            "optimized_partitions": [],
            "errors": [],
        }

        if not target_partitions:
            logger.info(f"No {table_role} index partitions to maintain")
            return summary

        for partition in target_partitions:
            try:
                existing_indexes = {
                    index["name"] if isinstance(index, dict) else index.name
                    for index in self.session.list_indices(table_name, partition=partition)
                }
            except Exception as exc:
                existing_indexes = set()
                logger.warning(
                    f"Could not list indexes for {table_name} partition={partition}: {exc}"
                )

            if create_missing:
                for column, index_type in index_specs:
                    index_name = f"{column}_idx"
                    if index_name in existing_indexes:
                        continue
                    try:
                        self.session.create_scalar_index(
                            table_name,
                            column,
                            partition=partition,
                            index_type=index_type,
                        )
                        summary["indexes_created"].append(
                            {
                                "partition": partition,
                                "column": column,
                                "index_name": index_name,
                                "index_type": index_type,
                            }
                        )
                        self._log_dldb_timing(
                            "create_scalar_index",
                            self._extract_dldb_last_call(),
                            table_name=table_name,
                            extra={
                                "partition": partition,
                                "column": column,
                                "index_type": index_type,
                                "api": "maintain_table_indexes",
                                "table_role": table_role,
                            },
                        )
                    except Exception as exc:
                        error = {
                            "partition": partition,
                            "column": column,
                            "action": "create_scalar_index",
                            "error": str(exc),
                        }
                        summary["errors"].append(error)
                        logger.warning(f"Failed to create index during maintenance: {error}")

            if optimize:
                if not hasattr(self.session, "optimize"):
                    raise RuntimeError("Installed dldb does not expose session.optimize(...)")
                try:
                    self.session.optimize(
                        table_name,
                        partition=partition,
                        cleanup_older_than=cleanup_older_than,
                        delete_unverified=delete_unverified,
                        retrain=retrain,
                    )
                    summary["optimized_partitions"].append(partition)
                    self._log_dldb_timing(
                        "optimize",
                        self._extract_dldb_last_call(),
                        table_name=table_name,
                        extra={
                            "partition": partition,
                            "api": "maintain_table_indexes",
                            "table_role": table_role,
                        },
                    )
                except Exception as exc:
                    error = {
                        "partition": partition,
                        "action": "optimize",
                        "error": str(exc),
                    }
                    summary["errors"].append(error)
                    logger.warning(f"Failed to optimize during maintenance: {error}")

        logger.info(
            f"Table index maintenance complete: role={table_role}, table={table_name}, "
            f"partitions={len(target_partitions)}, "
            f"indexes_created={len(summary['indexes_created'])}, "
            f"optimized={len(summary['optimized_partitions'])}, "
            f"errors={len(summary['errors'])}"
        )
        return summary

    def create_scalar_index(
        self,
        table: str = "landing",
        column: str = "id",
        index_type: str = "BTREE"
    ) -> None:
        info = self._get_table_info(table)
        self.session.create_scalar_index(info["table_name"], column, index_type=index_type)
        self._log_dldb_timing(
            "create_scalar_index",
            self._extract_dldb_last_call(),
            table_name=info["table_name"],
            extra={"column": column, "index_type": index_type},
        )
        logger.info(f"Created scalar index on {table}.{column}")

    # ==================== Lifecycle Management ====================

    def close(self) -> Optional[Dict[str, Any]]:
        summary = self.session.shutdown()
        if isinstance(summary, dict):
            self._log_dldb_metrics_summary(summary)
        logger.info("WTGatewayClient closed")
        return summary

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _ = exc_type, exc_val, exc_tb
        self.close()
