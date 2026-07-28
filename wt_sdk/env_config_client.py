import json
import time
from typing import Dict, Any, List, Union, Optional
from loguru import logger
import dldb
import pandas as pd
from wt_sdk.dldb_timing import (
    append_dldb_metrics_log,
    build_dldb_timing_payload,
    extract_dldb_last_call,
    extract_dldb_timing_from_df,
    format_dldb_metrics_summary,
    format_dldb_timing_log,
    resolve_dldb_metrics_log_path,
    resolve_dldb_model,
    resolve_enable_dldb_timing_logs,
)


class EnvConfigManager:

    def __init__(
        self,
        table_name: str = "evaluation_env_config",
        db_uri: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        dldb_model: Optional[str] = None,
        enable_dldb_timing_logs: bool = False,
        log_dldb_metrics_summary_on_close: bool = True,
        dldb_metrics_log_path: Optional[str] = None,
    ):
        from wt_sdk.config import S3Config, resolve_env_config_db_uri

        self.table_name = table_name
        self.db_uri = resolve_env_config_db_uri(db_uri)
        self.storage_options = storage_options or S3Config().to_storage_options()
        self._dldb_model = resolve_dldb_model(dldb_model)
        self._enable_dldb_timing_logs = resolve_enable_dldb_timing_logs(enable_dldb_timing_logs)
        self._log_dldb_metrics_summary_on_close = log_dldb_metrics_summary_on_close
        self._dldb_metrics_log_path = resolve_dldb_metrics_log_path(dldb_metrics_log_path)

        self.session = dldb.connect(
            self.db_uri,
            storage_options=self.storage_options,
            model=self._dldb_model,
        )

        logger.debug(f"EnvConfigManager initialized: table={self.table_name}, db={self.db_uri}")

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

        if not self._log_dldb_metrics_summary_on_close:
            return

        log_line = format_dldb_metrics_summary(summary)
        if log_line:
            logger.info(log_line)

    def _filter_table(
        self,
        *,
        query: str,
        limit: Optional[int] = None,
        columns: Optional[List[str]] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        effective_query = query if query and query.strip() else "id IS NOT NULL"
        df = self.session.filter(
            self.table_name,
            query=effective_query,
            limit=limit,
            columns=columns,
        )
        timing = self._extract_dldb_timing_from_df(df) or self._extract_dldb_last_call()
        log_extra = {
            "limit": limit,
            "columns_count": len(columns) if columns else None,
        }
        if extra:
            log_extra.update(extra)
        if effective_query != query:
            log_extra["normalized_empty_query"] = True
        self._log_dldb_timing("filter", timing, table_name=self.table_name, extra=log_extra)
        return df

    def _add_table(self, df: pd.DataFrame) -> None:
        self.session.add(self.table_name, df)
        self._log_dldb_timing(
            "add",
            self._extract_dldb_last_call(),
            table_name=self.table_name,
            extra={"input_rows": len(df)},
        )

    def _delete_where(self, where: str) -> None:
        self.session.delete(self.table_name, where)
        self._log_dldb_timing(
            "delete",
            self._extract_dldb_last_call(),
            table_name=self.table_name,
        )

    def _update_where(self, where: str, values: Dict[str, Any]) -> None:
        self.session.update(self.table_name, where, values)
        self._log_dldb_timing(
            "update",
            self._extract_dldb_last_call(),
            table_name=self.table_name,
            extra={"updated_fields": len(values)},
        )

    def _get_next_id(self) -> int:
        """
        Get the next auto-increment ID. Returns the maximum existing ID + 1, or 1 if table is empty.
        """
        try:
            # Query to get max ID
            df = self._filter_table(
                query="",  # No filter
                limit=None,
            )
            if len(df) > 0:
                return int(df["id"].max() + 1)
            return 1
        except Exception:
            return 1

    def save_config(
        self,
        config: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> List[int]:
        """
        Example:
            >>> manager = EnvConfigManager()
            >>> # Single config
            >>> config = {
            ...     "env_name": "CartPole-v1",
            ...     "env_id": "env-001",
            ...     "job_id": "job-session-001",
            ...     "group_id": "group-1",
            ...     "env_params": {"gravity": 9.8},
            ...     "image": "cartpole:latest"
            ... }
            >>> ids = manager.save_config(config)

            >>> # Multiple configs (batch write - more efficient)
            >>> configs = [config1, config2, config3]
            >>> ids = manager.save_config(configs)
        """
        # Normalize to list
        if isinstance(config, dict):
            configs = [config]
        else:
            configs = config

        if not configs:
            raise ValueError("Config cannot be empty")

        # Get next ID
        next_id = self._get_next_id()

        # Validate all configs first before writing
        records = []
        for idx, cfg in enumerate(configs):
            if not isinstance(cfg, dict):
                raise ValueError(f"Config must be a dict, got {type(cfg)}")

            if "env_name" not in cfg:
                raise ValueError(f"Config must contain 'env_name' field: {cfg}")

            # Build record with all fields
            record = {
                "id": next_id + idx,
                "env_id": cfg.get("env_id", f"auto-{next_id + idx}-{int(time.time()*1000)}"),
                "job_id": cfg.get("job_id", ""),
                "group_id": cfg.get("group_id"),
                "finished": cfg.get("finished", False),
                "env_name": cfg["env_name"],
                "env_params": json.dumps(cfg.get("env_params", {})),
                "image": cfg.get("image"),
                "created_at": cfg.get("created_at", int(time.time())),
            }
            records.append(record)

        # Batch write all records at once
        try:
            logger.info(f"Saving {len(records)} env_configs (batch write)")
            df = pd.DataFrame(records)
            self._add_table(df)
            saved_ids = [r["id"] for r in records]
            logger.info(f"Successfully saved {len(saved_ids)} env_configs")
            return saved_ids

        except Exception as e:
            logger.error(f"Failed to save env_configs: {e}")
            raise

    def get_env_configs(
        self,
        limit: int,
        offset: int = 0,
        filter_query: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Fetch environment configs with pagination.

        Mimics the SQLite pattern:
            SELECT id, env_name, env_id, env_params, image, group_id, ...
            FROM evaluation_env_config
            WHERE {filter_query}
            ORDER BY id ASC
            LIMIT ? OFFSET ?

        Args:
            limit: Maximum number of configs to return
            offset: Number of configs to skip (for pagination)
            filter_query: Optional SQL WHERE clause (e.g., "finished = false")

        Returns:
            List of config dicts with all fields

        Example:
            >>> manager = EnvConfigManager()
            >>> # Fetch first 10 configs
            >>> configs = manager.get_env_configs(limit=10, offset=0)
            >>> # Fetch next 10 configs
            >>> configs = manager.get_env_configs(limit=10, offset=10)
            >>> # Fetch with filter
            >>> configs = manager.get_env_configs(limit=10, offset=0, filter_query="finished = false")
        """
        try:
            logger.debug(f"Querying env_configs: limit={limit}, offset={offset}, filter={filter_query}")

            # Query LanceDB
            df = self._filter_table(
                query=filter_query,
                limit=None,  # Get all, then paginate in memory
            )

            # Sort by id
            df = df.sort_values("id")

            # Apply pagination
            paginated_df = df.iloc[offset:offset + limit]

            # Convert to list of dicts
            configs = []
            for _, row in paginated_df.iterrows():
                config = {
                    "id": int(row["id"]),
                    "env_id": row["env_id"],
                    "job_id": row.get("job_id", ""),
                    "group_id": row.get("group_id"),
                    "finished": bool(row["finished"]),
                    "env_name": row["env_name"],
                    "env_params": json.loads(row["env_params"]) if row.get("env_params") else {},
                    "image": row.get("image"),
                    "created_at": int(row["created_at"]),
                }
                configs.append(config)

            logger.info(f"Retrieved {len(configs)} env_configs (limit={limit}, offset={offset})")
            return configs

        except Exception as e:
            logger.error(f"Failed to fetch env_configs: {e}")
            raise

    def get_all_env_configs(self) -> List[Dict[str, Any]]:
        """
        Example:
            >>> manager = EnvConfigManager()
            >>> configs = manager.get_all_env_configs()
            >>> print(f"Total configs: {len(configs)}")
        """
        try:
            logger.debug("Querying all env_configs")

            # Query LanceDB - get all rows
            df = self._filter_table(
                query="",  # No filter
                limit=None,  # Get all
            )

            # Sort by id
            df = df.sort_values("id")

            # Convert to list of dicts
            configs = []
            for _, row in df.iterrows():
                config = {
                    "id": int(row["id"]),
                    "env_id": row["env_id"],
                    "job_id": row.get("job_id", ""),
                    "group_id": row.get("group_id"),
                    "finished": bool(row["finished"]),
                    "env_name": row["env_name"],
                    "env_params": json.loads(row["env_params"]) if row.get("env_params") else {},
                    "image": row.get("image"),
                    "created_at": int(row["created_at"]),
                }
                configs.append(config)

            logger.info(f"Retrieved all {len(configs)} env_configs")
            return configs

        except Exception as e:
            logger.error(f"Failed to fetch all env_configs: {e}")
            raise

    def clean_all_configs(self) -> int:
        """
        Example:
            >>> manager = EnvConfigManager()
            >>> count = manager.clean_all_configs()
            >>> print(f"Deleted {count} configs")
        """
        try:
            # First, count how many configs exist
            total_count = self.count()

            if total_count == 0:
                logger.info("No configs to delete")
                return 0

            # Delete all configs (use a tautology to match all rows)
            self._delete_where("id IS NOT NULL")  # Matches all rows since id is always present

            logger.info(f"Deleted all {total_count} env_configs from table")
            return total_count

        except Exception as e:
            logger.error(f"Failed to clean all configs: {e}")
            raise

    def get_env_image_map(self) -> Dict[str, Optional[str]]:
        """
        Get mapping of env_name -> image.
        Example:
            >>> manager = EnvConfigManager()
            >>> image_map = manager.get_env_image_map()
            >>> print(image_map)
            {'CartPole-v1': 'cartpole:latest', 'LunarLander': 'lunar:latest'}
        """
        try:
            logger.debug("Querying env_name -> image mapping")

            # Query all rows, sorted by id
            df = self._filter_table(
                query="",
                limit=None,
            )

            # Sort by id
            df = df.sort_values("id")

            # Build mapping
            result: Dict[str, Optional[str]] = {}
            for _, row in df.iterrows():
                env_name = row.get("env_name")
                image = row.get("image")

                if env_name is None:
                    continue

                # Use this image if provided, otherwise keep existing
                if image:
                    result[env_name] = image
                elif env_name not in result:
                    result[env_name] = None

            logger.info(f"Built env_image map: {len(result)} environments")
            return result

        except Exception as e:
            logger.error(f"Failed to get env_image map: {e}")
            raise

    def get_all_image(self) -> Dict[str, str]:
        """
        Get mapping of image -> env_name.
        Example:
            >>> manager = EnvConfigManager()
            >>> image_map = manager.get_all_image()
            >>> print(image_map)
            {'cartpole:latest': 'CartPole-v1', 'lunar:latest': 'LunarLander'}
        """
        try:
            logger.debug("Querying image -> env_name mapping")

            # Query rows with non-empty images (TRIM not supported in Lance SQL)
            df = self._filter_table(
                query="image IS NOT NULL AND env_name IS NOT NULL",
                limit=None,
            )

            # Sort by id
            df = df.sort_values("id")

            # Build mapping (first env_name for each image wins)
            result: Dict[str, str] = {}
            for _, row in df.iterrows():
                image = (row.get("image") or "").strip()
                env_name = (row.get("env_name") or "").strip()

                if not image or not env_name:
                    continue

                # Only add if image not already in mapping
                if image not in result:
                    result[image] = env_name

            logger.info(f"Built image map: {len(result)} unique images")
            return result

        except Exception as e:
            logger.error(f"Failed to get all images: {e}")
            raise

    def delete_config(self, env_id: str) -> bool:
        """
        Example:
            >>> manager = EnvConfigManager()
            >>> manager.delete_config("env-001")
            True
        """
        try:
            # Delete by env_id filter
            self._delete_where(f"env_id = '{env_id}'")
            logger.info(f"Deleted env_config: {env_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete env_config {env_id}: {e}")
            return False

    def update_config(self, env_id: str, updates: Dict[str, Any]) -> bool:
        """
        Example:
            >>> manager = EnvConfigManager()
            >>> manager.update_config("env-001", {"finished": True, "image": "new-image"})
            True
        """
        try:
            # Convert env_params to JSON if present
            if "env_params" in updates and isinstance(updates["env_params"], dict):
                updates = updates.copy()
                updates["env_params"] = json.dumps(updates["env_params"])

            # Update by env_id (positional args)
            self._update_where(f"env_id = '{env_id}'", updates)
            logger.info(f"Updated env_config: {env_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update env_config {env_id}: {e}")
            return False

    def count(self, filter_query: str = "") -> int:
        """
        Example:
            >>> manager = EnvConfigManager()
            >>> total = manager.count()
            >>> unfinished = manager.count("finished = false")
        """
        try:
            # Use filter and count results .count_rows doesn't work for unpartitioned tables
            df = self._filter_table(
                query=filter_query,
                limit=None,
            )
            return len(df)
        except Exception as e:
            logger.error(f"Failed to count env_configs: {e}")
            return 0

    # ==================== Lifecycle Management ====================

    def close(self) -> Optional[Dict[str, Any]]:
        summary = self.session.shutdown()
        if isinstance(summary, dict):
            self._log_dldb_metrics_summary(summary)
        logger.info("EnvConfigManager closed")
        return summary

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _ = exc_type, exc_val, exc_tb
        self.close()
