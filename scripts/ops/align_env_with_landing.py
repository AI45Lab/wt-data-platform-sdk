#!/usr/bin/env python3
"""Align the env-config table with wt_landing by job_id.

Keeps only env rows whose job_id exists in the landing table and deletes
everything else from the env table. The landing table is opened READ-ONLY:
this script never issues a write call against it -- landing data is used
solely to build the set of valid job_ids.

Env table selection follows --profile (wt_env_config for production, env_test
for test) in the env-config database resolved by wt_sdk. The landing reference
is always read from the landing database, table wt_landing.

Notes:
  - Reading the landing job_id column opens every HASH bucket and may take a
    while. When targeting a remote (S3) database, export
    WT_SDK_S3_REQUEST_TIMEOUT=120 first.
  - Lance deletes are logical: deleted rows stay recoverable in old table
    versions until cleanup_old_versions runs. In addition, this script always
    dumps every to-be-deleted row to a local JSONL backup before deleting.
  - Deletes are batched (one transaction per --batch-size job_ids) to avoid
    re-creating the version-explosion problem. Run
    maintain_env_config_indexes.py afterwards to compact, but only run its
    cleanup after you have verified the result.

Examples:
  # inspect only: counts, sample job_ids, writes the JSONL backup
  python scripts/ops/align_env_with_landing.py --profile production --dry-run

  # actually delete (still writes the backup first)
  python scripts/ops/align_env_with_landing.py --profile production --yes
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import dldb
import pandas as pd

from wt_sdk.config import (
    DEFAULT_LANDING_DB_URI,
    DEFAULT_LANDING_TABLE,
    resolve_env_config_db_uri,
    resolve_env_config_table_name,
    storage_options_for,
)


def _connect(db_uri: str):
    """Open a dldb session with storage options only for remote (S3) URIs."""
    connect_kwargs = {}
    storage_options = storage_options_for(db_uri)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
    return dldb.connect(db_uri, **connect_kwargs)


MATCH_ALL = "id IS NOT NULL"  # id is non-nullable in both schemas; SDK's own match-all


def sql_literal(value: str) -> str:
    """Escape a string into a single-quoted SQL literal."""
    return "'" + value.replace("'", "''") + "'"


def read_landing_job_ids(session, table_name: str) -> set:
    print(f"[1/4] Reading job_id column from landing table {table_name!r} (read-only)...", flush=True)
    print("      (opens all HASH buckets; expect a slow full-table read)", flush=True)
    frame = session.filter(table_name, MATCH_ALL, columns=["job_id"])
    job_ids = set(frame["job_id"].dropna().astype(str).unique())
    print(f"      landing distinct job_ids: {len(job_ids)}", flush=True)
    return job_ids


def read_env_rows(session, table_name: str) -> pd.DataFrame:
    print(f"[2/4] Reading env table {table_name!r}...", flush=True)
    frame = session.filter(table_name, MATCH_ALL)
    print(f"      env rows: {len(frame)}, distinct job_ids: {frame['job_id'].nunique()}", flush=True)
    return frame


def plan_alignment(env_rows: pd.DataFrame, landing_job_ids: set) -> pd.DataFrame:
    keep_mask = env_rows["job_id"].astype(str).isin(landing_job_ids)
    to_delete = env_rows.loc[~keep_mask]
    keep = env_rows.loc[keep_mask]
    print(f"[3/4] Alignment plan:", flush=True)
    print(f"      keep:    {len(keep):8d} rows ({keep['job_id'].nunique()} job_ids present in landing)", flush=True)
    print(f"      delete:  {len(to_delete):8d} rows ({to_delete['job_id'].nunique()} job_ids NOT in landing)", flush=True)
    sample = sorted(to_delete["job_id"].astype(str).unique())[:10]
    if sample:
        print(f"      sample job_ids to delete: {json.dumps(sample, ensure_ascii=False, indent=8)}", flush=True)
    return to_delete


def write_backup(to_delete: pd.DataFrame, backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with backup_path.open("w", encoding="utf-8") as fh:
        for record in to_delete.to_dict(orient="records"):
            fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    print(f"      backup written: {backup_path} ({len(to_delete)} rows)", flush=True)


def delete_in_batches(session, table_name: str, job_ids, batch_size: int) -> int:
    total = len(job_ids)
    print(f"[4/4] Deleting {total} job_ids in batches of {batch_size}...", flush=True)
    deleted = 0
    for start in range(0, total, batch_size):
        batch = job_ids[start : start + batch_size]
        where = "job_id IN (" + ", ".join(sql_literal(job_id) for job_id in batch) + ")"
        session.delete(table_name, where)
        deleted += len(batch)
        print(f"      deleted batch {start // batch_size + 1}: cumulative {deleted}/{total} job_ids", flush=True)
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Delete env-config rows whose job_id is absent from the landing "
            "table. Landing is read-only; only the env table is modified."
        )
    )
    parser.add_argument(
        "--profile",
        choices=("test", "prod", "production"),
        default="production",
        help="env table to clean: wt_env_config (default) or env_test.",
    )
    parser.add_argument("--db-uri", default=None, help="Env-config DB URI override.")
    parser.add_argument(
        "--landing-db-uri",
        default=None,
        help=f"Landing DB URI (default: {DEFAULT_LANDING_DB_URI}).",
    )
    parser.add_argument(
        "--landing-table",
        default=DEFAULT_LANDING_TABLE,
        help=f"Landing reference table (default: {DEFAULT_LANDING_TABLE}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        metavar="N",
        help="job_ids per DELETE transaction (default: 50).",
    )
    parser.add_argument(
        "--backup-path",
        default=None,
        metavar="PATH",
        help="JSONL backup of deleted rows (default: env_align_backup_<ts>.jsonl in cwd).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan and write the backup, but delete nothing.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required to actually delete. Without it the script is a dry run.",
    )
    args = parser.parse_args()

    # New-generation stack: env_test / wt_env_config in the env-config
    # database, landing reference wt_landing in the landing database.
    env_db_uri = args.db_uri or resolve_env_config_db_uri()
    env_table = resolve_env_config_table_name(profile=args.profile)
    landing_db_uri = args.landing_db_uri or DEFAULT_LANDING_DB_URI

    print(f"Env DB:      {env_db_uri}")
    print(f"Env table:   {env_table}")
    print(f"Landing DB:  {landing_db_uri} (READ-ONLY reference)")
    print(f"Landing table: {args.landing_table}", flush=True)

    # Each session gets storage options only when its database is remote
    # (S3); the GPFS hot-layer paths must not receive S3 credentials.
    env_session = _connect(env_db_uri)
    landing_session = _connect(landing_db_uri)
    try:
        # Landing side: filter() only. This script must never call a write API
        # on landing_session.
        landing_job_ids = read_landing_job_ids(landing_session, args.landing_table)
        env_rows = read_env_rows(env_session, env_table)
        to_delete = plan_alignment(env_rows, landing_job_ids)

        backup_path = Path(
            args.backup_path
            or f"env_align_backup_{env_table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        )
        if len(to_delete):
            write_backup(to_delete, backup_path)

        if args.dry_run or not args.yes:
            print("DRY RUN - nothing deleted. Re-run with --yes to delete.", flush=True)
            return 0

        if not len(to_delete):
            print("Nothing to delete.", flush=True)
            return 0

        job_ids = sorted(to_delete["job_id"].astype(str).unique())
        delete_in_batches(env_session, env_table, job_ids, args.batch_size)

        # Verify: re-read env job_ids, none of the deleted set may remain.
        remaining = set(
            env_session.filter(env_table, MATCH_ALL, columns=["job_id"])["job_id"]
            .dropna()
            .astype(str)
            .unique()
        )
        leftover = remaining & set(job_ids)
        print(f"Verify: env rows now {len(remaining)} distinct job_ids; leftover deleted job_ids: {len(leftover)}", flush=True)
        if leftover:
            print(f"ERROR: {sorted(leftover)[:10]} still present!", flush=True)
            return 1
        print("Done. Consider running maintain_env_config_indexes.py to compact.", flush=True)
        return 0
    finally:
        env_session.shutdown()
        landing_session.shutdown()


if __name__ == "__main__":
    sys.exit(main())
