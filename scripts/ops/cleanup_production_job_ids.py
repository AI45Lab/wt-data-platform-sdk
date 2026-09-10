#!/usr/bin/env python3
"""Delete matching job IDs from one environment's env, landing, and serving tables.

The default command has a production scope:

* ``evaluation_env_config`` in ``WT_SDK_ENV_CONFIG_DB_URI``;
* ``wind_tunnel_landing`` in ``WT_SDK_DB_URI`` (or the SDK default);
* ``wind_tunnel_serving`` in ``WT_SDK_DB_URI`` (or the SDK default).

For safe integration validation, ``--profile test`` targets the corresponding
test tables instead.  No arbitrary table names are accepted.

Job IDs can be supplied repeatedly with ``--job-id`` or one per line with
``--job-id-file``.  The default is a read-only preview.  A destructive run
requires both ``--execute`` and ``--confirm-delete``.

Examples::

    python scripts/ops/cleanup_production_job_ids.py \
        --job-id gateway \
        --job-id 'cyberrange#20260901#001'

    python scripts/ops/cleanup_production_job_ids.py \
        --job-id-file ./job_ids.txt --dry-run

    python scripts/ops/cleanup_production_job_ids.py \
        --job-id-file ./job_ids.txt --execute --confirm-delete

    python scripts/ops/cleanup_production_job_ids.py \
        --profile test --job-id test-cleanup --execute --confirm-delete
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_ENV_CONFIG_TABLE,
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_ENV_CONFIG_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
    TableConfig,
    default_config,
)
from wt_sdk.env_config_client import EnvConfigManager


PREVIEW_COLUMNS = ["id", "job_id", "env_id", "env_name", "session_id", "created_at"]
MISSING_PARTITION_MARKERS = ("partition", "does not exist")
PROFILE_TABLES = {
    "production": {
        "env": DEFAULT_ENV_CONFIG_TABLE,
        "landing": DEFAULT_LANDING_TABLE,
        "serving": DEFAULT_SERVING_TABLE,
    },
    "test": {
        "env": TEST_ENV_CONFIG_TABLE,
        "landing": TEST_LANDING_TABLE,
        "serving": TEST_SERVING_TABLE,
    },
}


@dataclass
class TableResult:
    """Rows found for one production logical table."""

    table_name: str
    rows: list[dict[str, Any]]
    error: str | None = None

    @property
    def count(self) -> int:
        return len(self.rows)


def quote_sql_literal(value: str) -> str:
    """Quote a SQL string literal using SQL's standard single-quote escape."""
    return "'" + value.replace("'", "''") + "'"


def build_job_id_predicate(job_ids: Sequence[str]) -> str:
    """Build an exact-match predicate for a non-empty job-id sequence."""
    if not job_ids:
        raise ValueError("at least one job_id is required")
    return "job_id IN (" + ", ".join(quote_sql_literal(job_id) for job_id in job_ids) + ")"


def normalize_job_ids(values: Iterable[str]) -> list[str]:
    """Strip, reject blank IDs, and deduplicate while preserving input order."""
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        job_id = value.strip()
        if not job_id:
            raise ValueError("job_id values must not be blank")
        if job_id not in seen:
            normalized.append(job_id)
            seen.add(job_id)
    if not normalized:
        raise ValueError("at least one job_id is required")
    return normalized


def _records_from_frame(frame: Any) -> list[dict[str, Any]]:
    """Convert dldb's DataFrame result into ordinary records."""
    if frame is None:
        return []
    if isinstance(frame, pd.DataFrame):
        return frame.to_dict("records")
    if hasattr(frame, "to_dict"):
        return frame.to_dict("records")
    return list(frame)


def _is_missing_partition_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return all(marker in message for marker in MISSING_PARTITION_MARKERS)


def _normalize_profile(profile: str) -> str:
    """Normalize the accepted profile aliases."""
    normalized = profile.strip().lower()
    if normalized == "prod":
        normalized = "production"
    if normalized not in PROFILE_TABLES:
        raise ValueError("profile must be one of: production, prod, test")
    return normalized


def _build_client(profile: str) -> WTGatewayClient:
    """Create a client pinned to the selected landing and serving tables."""
    profile = _normalize_profile(profile)
    profile_tables = PROFILE_TABLES[profile]
    tables = TableConfig(
        db_uri=default_config.tables.db_uri,
        landing_table=profile_tables["landing"],
        serving_table=profile_tables["serving"],
        profile=profile,
    )
    return WTGatewayClient(
        GatewayConfig(
            s3=default_config.s3,
            tables=tables,
            dldb_model=default_config.dldb_model,
            enable_dldb_timing_logs=default_config.enable_dldb_timing_logs,
            log_dldb_metrics_summary_on_close=default_config.log_dldb_metrics_summary_on_close,
            dldb_metrics_log_path=default_config.dldb_metrics_log_path,
        )
    )


def _table_role(table_name: str, profile: str) -> str:
    """Return the role for a profile-specific active table."""
    profile_tables = PROFILE_TABLES[_normalize_profile(profile)]
    for role in ("landing", "serving"):
        if table_name == profile_tables[role]:
            return role
    raise ValueError(f"unsupported active table for profile {profile}: {table_name}")


def _query_active_job(
    client: WTGatewayClient,
    *,
    table_name: str,
    job_id: str,
) -> list[dict[str, Any]]:
    """Read one exact job ID from a HASH table.

    A job whose HASH bucket has not been physically created is equivalent to
    zero matching rows.  Querying IDs individually prevents one empty bucket
    from hiding matches for other requested IDs.
    """
    try:
        return client.query_data(
            filter_query=build_job_id_predicate([job_id]),
            limit=None,
            columns=["id", "job_id"],
            table=table_name,
            exclude_none=False,
            deserialize_json=False,
            checkout_latest=True,
        )
    except ValueError as exc:
        if _is_missing_partition_error(exc):
            return []
        raise


def _query_env_job(
    manager: EnvConfigManager,
    *,
    job_id: str,
) -> list[dict[str, Any]]:
    """Read one exact job ID from the production SimpleTable."""
    frame = manager.session.filter(
        manager.table_name,
        build_job_id_predicate([job_id]),
        columns=["id", "job_id"],
        checkout_latest=True,
    )
    return _records_from_frame(frame)


def inspect_table(
    *,
    table_name: str,
    job_ids: Sequence[str],
    client: WTGatewayClient | None = None,
    env_manager: EnvConfigManager | None = None,
) -> TableResult:
    """Inspect exact IDs in one table and return all lightweight matches."""
    rows: list[dict[str, Any]] = []
    try:
        for job_id in job_ids:
            if env_manager is not None:
                rows.extend(_query_env_job(env_manager, job_id=job_id))
            elif client is not None:
                rows.extend(_query_active_job(client, table_name=table_name, job_id=job_id))
            else:
                raise ValueError("either client or env_manager is required")
        return TableResult(table_name=table_name, rows=rows)
    except Exception as exc:
        return TableResult(table_name=table_name, rows=rows, error=str(exc))


def _print_result(result: TableResult) -> None:
    if result.error:
        print(f"[ERROR] {result.table_name}: {result.error}")
        return
    if result.count == 0:
        print(f"[SKIP] {result.table_name}: no matching rows")
        return

    counts: dict[str, int] = {}
    for row in result.rows:
        job_id = str(row.get("job_id", ""))
        counts[job_id] = counts.get(job_id, 0) + 1
    print(f"[MATCH] {result.table_name}: {result.count} rows")
    for job_id, count in counts.items():
        print(f"  {count:>8}  {job_id}")

    preview = pd.DataFrame(result.rows[:5])
    columns = [column for column in PREVIEW_COLUMNS if column in preview.columns]
    if columns:
        print(preview[columns].to_string(index=False))


def _delete_table(
    *,
    table_name: str,
    job_ids: Sequence[str],
    profile: str,
    client: WTGatewayClient | None = None,
    env_manager: EnvConfigManager | None = None,
) -> tuple[int, list[str]]:
    """Delete exact IDs from one table and return requested count/errors."""
    deleted_rows = 0
    errors: list[str] = []
    for job_id in job_ids:
        predicate = build_job_id_predicate([job_id])
        try:
            if env_manager is not None:
                rows = _query_env_job(env_manager, job_id=job_id)
                if not rows:
                    print(f"[SKIP] {table_name}: {job_id} has no matching rows")
                    continue
                env_manager.session.delete(env_manager.table_name, predicate)
                deleted_rows += len(rows)
            elif client is not None:
                rows = _query_active_job(client, table_name=table_name, job_id=job_id)
                if not rows:
                    print(f"[SKIP] {table_name}: {job_id} has no matching rows")
                    continue
                reported = (
                    client.delete_landing(predicate)
                    if _table_role(table_name, profile) == "landing"
                    else client.delete_serving(predicate)
                )
                deleted_rows += reported if isinstance(reported, int) else len(rows)
            else:
                raise ValueError("either client or env_manager is required")
            print(f"[DELETE] {table_name}: {job_id} ({len(rows)} matched)")
        except Exception as exc:
            errors.append(f"{job_id}: {exc}")
            print(f"[ERROR] {table_name}: failed for {job_id}: {exc}")
    return deleted_rows, errors


def _verify_table(
    *,
    table_name: str,
    job_ids: Sequence[str],
    client: WTGatewayClient | None = None,
    env_manager: EnvConfigManager | None = None,
) -> TableResult:
    """Re-read the table after deletion and report any remaining rows."""
    return inspect_table(
        table_name=table_name,
        job_ids=job_ids,
        client=client,
        env_manager=env_manager,
    )


def run(
    *,
    job_ids: Sequence[str],
    execute: bool = False,
    profile: str = "production",
    client: WTGatewayClient | None = None,
    env_manager: EnvConfigManager | None = None,
) -> int:
    """Preview or delete exact IDs across the selected environment's tables."""
    job_ids = normalize_job_ids(job_ids)
    profile = _normalize_profile(profile)
    profile_tables = PROFILE_TABLES[profile]
    owns_client = client is None
    owns_env_manager = env_manager is None
    if client is None:
        client = _build_client(profile)
    try:
        if env_manager is None:
            env_manager = EnvConfigManager(profile=profile)

        table_results = [
            inspect_table(
                table_name=profile_tables["env"],
                job_ids=job_ids,
                env_manager=env_manager,
            ),
            inspect_table(
                table_name=profile_tables["landing"],
                job_ids=job_ids,
                client=client,
            ),
            inspect_table(
                table_name=profile_tables["serving"],
                job_ids=job_ids,
                client=client,
            ),
        ]
        for result in table_results:
            _print_result(result)

        errors = [result for result in table_results if result.error]
        matched = sum(result.count for result in table_results)
        if not execute:
            print(
                f"Preview complete: {matched} matching rows across "
                f"{len(table_results)} {profile} tables."
            )
            return 1 if errors else 0
        if errors:
            print("Deletion aborted because at least one table could not be inspected.")
            return 1
        if matched == 0:
            print("No matching rows found; nothing to delete.")
            return 0

        print(f"Executing deletion for {len(job_ids)} exact {profile} job_id values.")
        deletion_errors: list[str] = []
        for table_name in (
            profile_tables["env"],
            profile_tables["landing"],
            profile_tables["serving"],
        ):
            if table_name == profile_tables["env"]:
                _, table_errors = _delete_table(
                    table_name=table_name,
                    job_ids=job_ids,
                    profile=profile,
                    env_manager=env_manager,
                )
                remaining = _verify_table(
                    table_name=table_name,
                    job_ids=job_ids,
                    env_manager=env_manager,
                )
            else:
                _, table_errors = _delete_table(
                    table_name=table_name,
                    job_ids=job_ids,
                    profile=profile,
                    client=client,
                )
                remaining = _verify_table(
                    table_name=table_name,
                    job_ids=job_ids,
                    client=client,
                )
            deletion_errors.extend(f"{table_name}: {error}" for error in table_errors)
            if remaining.error:
                deletion_errors.append(f"{table_name}: verification failed: {remaining.error}")
            elif remaining.count:
                deletion_errors.append(
                    f"{table_name}: {remaining.count} matching rows remain after delete"
                )
                print(f"[WARN] {table_name}: {remaining.count} matching rows remain after delete")
            else:
                print(f"[VERIFY] {table_name}: no requested job_id remains")

        if deletion_errors:
            print("Completed with errors:")
            for error in deletion_errors:
                print(f"  - {error}")
            return 1
        print(f"{profile.capitalize()} job-id cleanup completed successfully.")
        return 0
    finally:
        if owns_client and client is not None:
            client.close()
        if owns_env_manager and env_manager is not None:
            env_manager.close()


def _load_job_ids(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[str]:
    values = list(args.job_id or [])
    if args.job_id_file:
        path = Path(args.job_id_file)
        try:
            values.extend(
                line.strip()
                for line in path.read_text().splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            )
        except OSError as exc:
            parser.error(f"cannot read --job-id-file {path}: {exc}")
    try:
        return normalize_job_ids(values)
    except ValueError as exc:
        parser.error(str(exc))
        raise AssertionError("argparse.error does not return")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preview/delete exact job IDs in an environment's "
            "env-config, landing, and serving tables"
        )
    )
    parser.add_argument(
        "--job-id",
        action="append",
        help="Exact job_id in the selected profile; repeat for multiple IDs.",
    )
    parser.add_argument(
        "--job-id-file",
        help="Text file containing one exact job_id per line; blank lines and # comments are ignored.",
    )
    parser.add_argument(
        "--profile",
        choices=("production", "prod", "test"),
        default="production",
        help=(
            "Environment to clean: production (default) or test. "
            "Test uses env_config_test, v2_landing_test, and serving_test."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Perform deletion after inspection; requires --confirm-delete.",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only; this is also the default.",
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Required together with --execute; no interactive prompt is used.",
    )
    args = parser.parse_args()

    if args.confirm_delete and not args.execute:
        parser.error("--confirm-delete is only valid with --execute")
    if args.execute and not args.confirm_delete:
        parser.error("--execute requires --confirm-delete")
    job_ids = _load_job_ids(args, parser)

    return run(job_ids=job_ids, execute=args.execute, profile=args.profile)


if __name__ == "__main__":
    raise SystemExit(main())
