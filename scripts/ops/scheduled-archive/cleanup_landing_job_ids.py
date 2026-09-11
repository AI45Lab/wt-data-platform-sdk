#!/usr/bin/env python3
"""Delete exact job IDs from production landing, one job at a time.

This script only touches ``wind_tunnel_landing`` and processes each job_id
independently.  It reuses the optimized landing-table path from
``scripts/ops/cleanup_data.py`` so a large table is never materialized in one
operation.

The default mode is a read-only preview.  A destructive run requires both
``--execute`` and ``--confirm-delete``.

Examples::

    python scripts/ops/scheduled-archive/cleanup_landing_job_ids.py \
        --job-id-file ./archive_job_ids.txt --dry-run

    python scripts/ops/scheduled-archive/cleanup_landing_job_ids.py \
        --job-id-file ./archive_job_ids.txt \
        --execute --confirm-delete
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Sequence

# The shared cleanup implementation remains in scripts/ops. Add that directory
# explicitly because this script now lives under scheduled-archive/.
OPS_DIR = Path(__file__).resolve().parents[1]
if str(OPS_DIR) not in sys.path:
    sys.path.insert(0, str(OPS_DIR))
from cleanup_data import _cleanup_trajectory_table
from wt_sdk.config import DEFAULT_LANDING_TABLE, default_config


def normalize_job_ids(values: Iterable[str]) -> list[str]:
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


def quote_sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def exact_job_predicate(job_id: str) -> str:
    return f"job_id = {quote_sql_literal(job_id)}"


def load_job_ids(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[str]:
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


def run(
    *,
    job_ids: Sequence[str],
    execute: bool,
) -> int:
    job_ids = normalize_job_ids(job_ids)
    errors: list[str] = []
    print(f"Landing table: {DEFAULT_LANDING_TABLE}")
    print(f"Database: {default_config.tables.db_uri}")
    print(f"Job IDs: {len(job_ids)}")
    print(
        "Mode: execute (one job_id at a time)"
        if execute
        else "Mode: dry-run (one job_id at a time)"
    )
    print("=" * 80)

    for index, job_id in enumerate(job_ids, start=1):
        predicate = exact_job_predicate(job_id)
        print(f"\n[{index}/{len(job_ids)}] job_id={job_id}")
        try:
            result = _cleanup_trajectory_table(
                db_name=default_config.tables.db_uri,
                table_name=DEFAULT_LANDING_TABLE,
                query=predicate,
                dry_run=not execute,
                confirm=execute,
            )
            if result != 0:
                errors.append(f"{job_id}: cleanup_data.py returned {result}")
        except Exception as exc:
            errors.append(f"{job_id}: {exc}")
            print(f"[ERROR] {job_id}: {exc}")
            continue

    print("\n" + "=" * 80)
    if errors:
        print(f"Completed with {len(errors)} error(s):")
        for error in errors:
            print(f"  - {error}")
        return 1
    print(
        "Preview complete; no rows were changed."
        if not execute
        else "Landing job-id cleanup completed successfully."
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preview/delete exact job IDs from production wind_tunnel_landing, "
            "processing one job_id at a time."
        )
    )
    parser.add_argument(
        "--job-id",
        action="append",
        help="Exact job_id; repeat for multiple IDs.",
    )
    parser.add_argument(
        "--job-id-file",
        help="Text file containing one exact job_id per line; blank lines and # comments are ignored.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--execute",
        action="store_true",
        help="Delete matching rows; requires --confirm-delete.",
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
    job_ids = load_job_ids(args, parser)
    return run(job_ids=job_ids, execute=args.execute)


if __name__ == "__main__":
    raise SystemExit(main())
