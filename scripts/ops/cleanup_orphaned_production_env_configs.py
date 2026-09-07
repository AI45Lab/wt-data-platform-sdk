#!/usr/bin/env python3
"""Remove production env-config rows with no matching production landing job.

The production environment-config table is a control-plane table, while the
production landing table is the source of truth for active SAfactory jobs.
This command deliberately compares only those two tables.  The historical
``wind_tunnel_landing_legacy`` table and all test tables are out of scope.

The default mode is a read-only dry run.  A destructive run requires both
``--execute`` and ``--confirm-delete``.  The production landing job-id set and
the env-config candidate set are rescanned immediately before deletion so a
long preview cannot silently turn into a stale delete plan.

Examples::

    # Preview candidates (the default; --dry-run is optional)
    python scripts/ops/cleanup_orphaned_production_env_configs.py

    # Explicit preview
    python scripts/ops/cleanup_orphaned_production_env_configs.py --dry-run

    # Delete only the revalidated orphan rows
    python scripts/ops/cleanup_orphaned_production_env_configs.py \
        --execute --confirm-delete
"""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Iterable

import pandas as pd

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_ENV_CONFIG_TABLE,
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    GatewayConfig,
    TableConfig,
    default_config,
)
from wt_sdk.env_config_client import EnvConfigManager


DEFAULT_BATCH_SIZE = 500
ENV_COLUMNS = ["id", "job_id", "env_id", "env_name"]


def _normalise_job_id(value: Any) -> str | None:
    """Return a comparable job id, treating null/blank values as missing."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        # ``pd.isna`` is not scalar-safe for arbitrary extension values.
        pass
    value = str(value).strip()
    return value or None


def collect_production_landing_job_ids(
    client: WTGatewayClient,
    *,
    table_name: str = DEFAULT_LANDING_TABLE,
) -> set[str]:
    """Collect production landing job_ids with bounded per-bucket reads.

    A failure in any existing bucket is propagated.  Silently skipping a
    bucket would make the anti-join unsafe and could delete valid env rows.
    """
    job_ids: set[str] = set()
    partitions = client.list_table_partitions(table=table_name)
    for partition in partitions:
        rows = client.query_data(
            filter_query="job_id IS NOT NULL",
            columns=["job_id"],
            partition=partition,
            table=table_name,
            exclude_none=False,
            deserialize_json=False,
            checkout_latest=True,
        )
        for row in rows:
            job_id = _normalise_job_id(row.get("job_id"))
            if job_id is not None:
                job_ids.add(job_id)
    return job_ids


def load_production_env_rows(
    manager: EnvConfigManager,
) -> pd.DataFrame:
    """Read the minimum env-config columns from the latest production view."""
    frame = manager._filter_table(  # operational script needs narrow raw rows
        query="id IS NOT NULL",
        columns=ENV_COLUMNS,
        checkout_latest=True,
        extra={"api": "cleanup_orphaned_production_env_configs"},
    )
    missing = [column for column in ENV_COLUMNS if column not in frame.columns]
    if missing:
        raise RuntimeError(
            f"{DEFAULT_ENV_CONFIG_TABLE} is missing required columns: {missing}"
        )
    return frame


def find_orphan_rows(
    env_rows: pd.DataFrame,
    production_job_ids: set[str],
) -> tuple[pd.DataFrame, int]:
    """Return deletable env rows and count blank/NULL job_id rows.

    Rows without a usable job_id are intentionally never deletion candidates.
    They are reported separately for manual review.
    """
    if env_rows.empty:
        return env_rows.copy(), 0

    normalised = env_rows["job_id"].map(_normalise_job_id)
    blank_count = int(normalised.isna().sum())
    orphan_mask = normalised.notna() & ~normalised.isin(production_job_ids)
    return env_rows.loc[orphan_mask].copy(), blank_count


def _validated_integer_ids(rows: pd.DataFrame) -> list[int]:
    """Validate env row ids before interpolating a numeric SQL IN predicate."""
    result: list[int] = []
    for value in rows["id"].tolist():
        try:
            integer = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Cannot safely delete env row with id={value!r}") from exc
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"Cannot safely delete env row with non-integer id={value!r}")
        result.append(integer)
    return result


def _chunks(values: list[int], size: int) -> Iterable[list[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def delete_env_rows_by_id(
    manager: EnvConfigManager,
    row_ids: list[int],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Submit explicit id-based deletes and return the number requested."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    for batch in _chunks(row_ids, batch_size):
        predicate = "id IN (" + ", ".join(str(row_id) for row_id in batch) + ")"
        manager._delete_where(predicate)
    return len(row_ids)


def _build_production_client() -> WTGatewayClient:
    tables = TableConfig(
        db_uri=default_config.tables.db_uri,
        landing_table=DEFAULT_LANDING_TABLE,
        serving_table=DEFAULT_SERVING_TABLE,
        profile="production",
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


def _print_report(
    *,
    production_job_ids: set[str],
    env_rows: pd.DataFrame,
    orphan_rows: pd.DataFrame,
    blank_job_id_count: int,
    sample_size: int,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "landing_table": DEFAULT_LANDING_TABLE,
        "env_table": DEFAULT_ENV_CONFIG_TABLE,
        "production_landing_job_id_count": len(production_job_ids),
        "env_row_count": len(env_rows),
        "orphan_env_row_count": len(orphan_rows),
        "orphan_job_id_count": (
            int(orphan_rows["job_id"].map(_normalise_job_id).nunique())
            if not orphan_rows.empty
            else 0
        ),
        "blank_or_null_job_id_count": blank_job_id_count,
        "sample": [],
    }
    if not orphan_rows.empty:
        preview_columns = [column for column in ["id", "job_id", "env_id", "env_name"] if column in orphan_rows]
        report["sample"] = orphan_rows[preview_columns].head(sample_size).to_dict("records")

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return report


def _refuse_empty_landing_source(
    landing_job_ids: set[str],
    env_rows: pd.DataFrame,
    *,
    allow_empty_landing: bool,
) -> None:
    """Prevent a transient/failed source scan from becoming delete-all."""
    if landing_job_ids or env_rows.empty or allow_empty_landing:
        return
    raise RuntimeError(
        "Production landing returned zero non-blank job_id values while "
        f"{len(env_rows)} env-config rows exist; refusing to classify every "
        "row as orphaned. Recheck the landing table or pass "
        "--allow-empty-landing only when an intentionally empty production "
        "landing table has been verified."
    )


def run(
    *,
    execute: bool,
    batch_size: int,
    sample_size: int,
    allow_empty_landing: bool,
) -> int:
    client = _build_production_client()
    manager = EnvConfigManager(profile="production")
    try:
        landing_job_ids = collect_production_landing_job_ids(client)
        env_rows = load_production_env_rows(manager)
        _refuse_empty_landing_source(
            landing_job_ids,
            env_rows,
            allow_empty_landing=allow_empty_landing,
        )
        orphan_rows, blank_count = find_orphan_rows(env_rows, landing_job_ids)
        print("Initial candidate report:")
        _print_report(
            production_job_ids=landing_job_ids,
            env_rows=env_rows,
            orphan_rows=orphan_rows,
            blank_job_id_count=blank_count,
            sample_size=sample_size,
        )

        if not execute:
            print("[DRY RUN] No rows were deleted.")
            return 0

        # Re-read both sides immediately before mutation.  This avoids using a
        # stale preview after a long scan or a concurrent env-config write.
        print("Revalidating production landing and env-config snapshots before delete...")
        landing_job_ids = collect_production_landing_job_ids(client)
        env_rows = load_production_env_rows(manager)
        _refuse_empty_landing_source(
            landing_job_ids,
            env_rows,
            allow_empty_landing=allow_empty_landing,
        )
        orphan_rows, blank_count = find_orphan_rows(env_rows, landing_job_ids)
        _print_report(
            production_job_ids=landing_job_ids,
            env_rows=env_rows,
            orphan_rows=orphan_rows,
            blank_job_id_count=blank_count,
            sample_size=sample_size,
        )

        row_ids = _validated_integer_ids(orphan_rows)
        if len(row_ids) != len(set(row_ids)):
            raise RuntimeError("Production env-config contains duplicate row IDs; refusing delete")
        requested = delete_env_rows_by_id(manager, row_ids, batch_size=batch_size)

        # Verify against a latest read.  The check is intentionally based on
        # the exact candidate IDs, not a broad NOT-IN predicate.
        remaining_rows = load_production_env_rows(manager)
        remaining_ids = set(_validated_integer_ids(remaining_rows)) & set(row_ids)
        print(
            json.dumps(
                {
                    "delete_requested": requested,
                    "candidate_ids_still_present": len(remaining_ids),
                    "blank_or_null_job_id_count_after_delete": int(
                        remaining_rows["job_id"].map(_normalise_job_id).isna().sum()
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if remaining_ids:
            raise RuntimeError(
                f"{len(remaining_ids)} requested env rows are still present after delete"
            )
        return 0
    finally:
        client.close()
        manager.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preview/delete production env-config rows whose job_id is absent "
            "from production wind_tunnel_landing"
        )
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Perform the revalidated deletion (requires --confirm-delete).",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only; this is the default.",
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Required together with --execute; no interactive prompt is used.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per env-table delete predicate (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=20,
        help="Number of candidate rows to print (default: 20).",
    )
    parser.add_argument(
        "--allow-empty-landing",
        action="store_true",
        help=(
            "Allow deletion when production landing has zero non-blank job_ids. "
            "Use only after independently verifying that the table is intentionally empty."
        ),
    )
    args = parser.parse_args()

    if args.batch_size <= 0 or args.sample_size < 0:
        parser.error("--batch-size must be positive and --sample-size must be non-negative")
    if args.confirm_delete and not args.execute:
        parser.error("--confirm-delete is only valid with --execute")
    if args.execute and not args.confirm_delete:
        parser.error("--execute requires --confirm-delete")

    return run(
        execute=args.execute,
        batch_size=args.batch_size,
        sample_size=args.sample_size,
        allow_empty_landing=args.allow_empty_landing,
    )


if __name__ == "__main__":
    raise SystemExit(main())
