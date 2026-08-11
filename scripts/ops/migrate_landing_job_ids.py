#!/usr/bin/env python3
"""Migrate landing rows from incorrect job_id values to corrected job_id values.

This script does not update job_id in place. job_id is the HASH partition key,
so a safe fix is copy-to-new-job-id and verify. It intentionally never deletes
old rows; run a separate cleanup after reviewing the migration result.

Fill JOB_ID_MAPPING before production use, or pass --mapping-json/--mapping-file
for one-off validation.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, Set

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
    TableConfig,
    default_config,
)
from wt_sdk.models import LandingRecord
from wt_sdk.utils import dataframe_to_landing_records


# Fill this mapping for the real production migration: k is incorrect job_id, v is fixed job_id
#
# JOB_ID_MAPPING = {
#     "old_job_id": "dataset#harness#model#task_type#date#owner#extra",
# }
JOB_ID_MAPPING: Dict[str, str] = {}

DEFAULT_BATCH_SIZE = 500


def escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def quote_sql_string(value: str) -> str:
    return f"'{escape_sql_string(value)}'"


def exact_job_filter(job_id: str) -> str:
    return f"job_id = {quote_sql_string(job_id)}"


def normalize_profile(profile: str) -> str:
    value = profile.strip().lower()
    if value in {"prod", "production"}:
        return "production"
    if value == "test":
        return "test"
    raise ValueError("profile must be one of: test, prod, production")


def landing_table_for_profile(profile: str) -> str:
    return TEST_LANDING_TABLE if normalize_profile(profile) == "test" else DEFAULT_LANDING_TABLE


def serving_table_for_profile(profile: str) -> str:
    return TEST_SERVING_TABLE if normalize_profile(profile) == "test" else DEFAULT_SERVING_TABLE


def build_config(profile: str, db_uri: str | None = None) -> GatewayConfig:
    """Build a profile-pinned config that ignores table-name env overrides."""
    canonical_profile = normalize_profile(profile)
    tables = TableConfig(
        profile=canonical_profile,
        db_uri=db_uri or default_config.tables.db_uri,
        landing_table=landing_table_for_profile(canonical_profile),
        serving_table=serving_table_for_profile(canonical_profile),
    )
    return GatewayConfig(
        s3=default_config.s3,
        tables=tables,
        dldb_model=default_config.dldb_model,
        enable_dldb_timing_logs=default_config.enable_dldb_timing_logs,
        log_dldb_metrics_summary_on_close=default_config.log_dldb_metrics_summary_on_close,
        dldb_metrics_log_path=default_config.dldb_metrics_log_path,
    )


def parse_mapping_json(raw_mapping: str) -> Dict[str, str]:
    try:
        parsed = json.loads(raw_mapping)
    except json.JSONDecodeError as exc:
        raise ValueError(f"mapping JSON is invalid: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("mapping JSON must be an object of old_job_id -> new_job_id")
    return {str(old): str(new) for old, new in parsed.items()}


def load_mapping(args: argparse.Namespace) -> Dict[str, str]:
    mapping = dict(JOB_ID_MAPPING)
    if args.mapping_file:
        path = Path(args.mapping_file).expanduser()
        mapping.update(parse_mapping_json(path.read_text(encoding="utf-8")))
    if args.mapping_json:
        mapping.update(parse_mapping_json(args.mapping_json))
    return mapping


def validate_new_job_id(job_id: str) -> None:
    parts = job_id.split("#")
    if len(parts) < 5 or any(not part.strip() for part in parts[:5]):
        raise ValueError(
            "new job_id must include at least 5 non-empty parts: "
            "dataset#harness#model#task_type#date; "
            "owner and extra suffix parts are optional; "
            f"got {job_id!r}"
        )


def validate_mapping(mapping: Mapping[str, str]) -> None:
    if not mapping:
        raise ValueError(
            "JOB_ID_MAPPING is empty. Fill it in the script or pass "
            "--mapping-json/--mapping-file."
        )

    for old_job_id, new_job_id in mapping.items():
        if not old_job_id or not old_job_id.strip():
            raise ValueError("old job_id values must be non-empty strings")
        if not new_job_id or not new_job_id.strip():
            raise ValueError(f"new job_id for {old_job_id!r} must be non-empty")
        if old_job_id == new_job_id:
            raise ValueError(f"old and new job_id are identical: {old_job_id!r}")
        validate_new_job_id(new_job_id)


def fetch_ids_for_job(
    client: WTGatewayClient,
    table_name: str,
    job_id: str,
) -> List[str]:
    try:
        rows = client.query_data(
            filter_query=exact_job_filter(job_id),
            columns=["id"],
            partition=job_id,
            table=table_name,
            exclude_none=False,
            deserialize_json=False,
            checkout_latest=True,
        )
    except ValueError as exc:
        # A corrected job_id may hash to a bucket that has no physical table yet.
        # For read-before-copy and post-delete verification this means "0 rows".
        if "partition" in str(exc) and "does not exist" in str(exc):
            return []
        raise
    ids: List[str] = []
    for row in rows:
        record_id = row.get("id")
        if not isinstance(record_id, str) or not record_id:
            raise RuntimeError(f"row under job_id {job_id!r} has invalid id: {record_id!r}")
        ids.append(record_id)
    return ids


def assert_unique_ids(ids: Sequence[str], *, label: str) -> None:
    unique_ids = set(ids)
    if len(unique_ids) == len(ids):
        return
    seen: Set[str] = set()
    duplicates = []
    for record_id in ids:
        if record_id in seen:
            duplicates.append(record_id)
        seen.add(record_id)
    sample = ", ".join(sorted(set(duplicates))[:10])
    raise RuntimeError(f"{label} contains duplicate ids; sample: {sample}")


def migrate_batch(
    records: Iterable[LandingRecord],
    *,
    old_job_id: str,
    new_job_id: str,
    already_present_ids: Set[str],
) -> List[LandingRecord]:
    migrated: List[LandingRecord] = []
    for record in records:
        if record.job_id != old_job_id:
            raise RuntimeError(
                f"export returned row {record.id!r} with unexpected job_id "
                f"{record.job_id!r}; expected {old_job_id!r}"
            )
        if record.id in already_present_ids:
            continue
        migrated.append(record.model_copy(update={"job_id": new_job_id}))
    return migrated


def run_one_mapping(
    client: WTGatewayClient,
    *,
    table_name: str,
    old_job_id: str,
    new_job_id: str,
    batch_size: int,
    execute: bool,
) -> Dict[str, int | str]:
    print("=" * 80)
    print(f"Old job_id: {old_job_id}")
    print(f"New job_id: {new_job_id}")

    old_ids = fetch_ids_for_job(client, table_name, old_job_id)
    assert_unique_ids(old_ids, label=f"old job_id {old_job_id!r}")
    old_id_set = set(old_ids)

    new_ids_before = fetch_ids_for_job(client, table_name, new_job_id)
    assert_unique_ids(new_ids_before, label=f"new job_id {new_job_id!r} before migration")
    already_present_ids = set(new_ids_before).intersection(old_id_set)

    print(f"Rows under old job_id: {len(old_ids)}")
    print(f"Rows already present under new job_id with same ids: {len(already_present_ids)}")
    estimated_to_copy = len(old_id_set) - len(already_present_ids)
    print(f"Rows to copy: {estimated_to_copy}")

    if not execute:
        print("Dry run only; no rows were copied.")
        return {
            "old_job_id": old_job_id,
            "new_job_id": new_job_id,
            "old_count": len(old_ids),
            "already_present": len(already_present_ids),
            "copied": 0,
        }

    copied = 0
    for batch_index, frame in enumerate(
        client.export_data_batches(
            table=table_name,
            filter_query=exact_job_filter(old_job_id),
            batch_size=batch_size,
            deserialize_json=False,
        ),
        start=1,
    ):
        source_records = dataframe_to_landing_records(frame)
        migrated_records = migrate_batch(
            source_records,
            old_job_id=old_job_id,
            new_job_id=new_job_id,
            already_present_ids=already_present_ids,
        )
        if migrated_records:
            client.ingest_landing_batch(migrated_records)
            copied += len(migrated_records)
            already_present_ids.update(record.id for record in migrated_records)
        print(
            f"Batch {batch_index}: read={len(source_records)}, "
            f"copied={len(migrated_records)}, total_copied={copied}"
        )

    new_ids_after = fetch_ids_for_job(client, table_name, new_job_id)
    assert_unique_ids(new_ids_after, label=f"new job_id {new_job_id!r} after migration")
    missing_after_copy = sorted(old_id_set.difference(new_ids_after))
    if missing_after_copy:
        raise RuntimeError(
            "new job_id does not contain every old id after copy; "
            f"missing sample: {missing_after_copy[:10]}"
        )
    print("Copy verification passed: every old id exists under the new job_id.")
    print("Old rows were not deleted by this script. Clean them up separately after review.")

    return {
        "old_job_id": old_job_id,
        "new_job_id": new_job_id,
        "old_count": len(old_ids),
        "already_present": len(set(new_ids_before).intersection(old_id_set)),
        "copied": copied,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Safely migrate landing rows from old job_id values to corrected "
            "job_id values by copy and verification. This script never deletes "
            "old rows."
        )
    )
    parser.add_argument(
        "--profile",
        default="test",
        choices=["test", "prod", "production"],
        help="Table profile to use. Defaults to test.",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI. Defaults to configured WT_SDK_DB_URI.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per full-data copy batch. Defaults to {DEFAULT_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--mapping-json",
        default=None,
        help='JSON object of old_job_id to new_job_id, e.g. \'{"old":"new"}\'.',
    )
    parser.add_argument(
        "--mapping-file",
        default=None,
        help="Path to a JSON object of old_job_id to new_job_id.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually copy rows. Without this flag the script is dry-run only.",
    )

    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("--batch-size must be a positive integer")

    try:
        mapping = load_mapping(args)
        validate_mapping(mapping)
    except Exception as exc:
        print(f"Invalid mapping: {exc}", file=sys.stderr)
        return 2

    table_name = landing_table_for_profile(args.profile)
    print(f"Profile: {normalize_profile(args.profile)}")
    print(f"Landing table: {table_name}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")
    print("Delete old rows: false (not supported by this script)")

    client = WTGatewayClient(build_config(args.profile, args.db_uri))
    summaries = []
    try:
        for old_job_id, new_job_id in mapping.items():
            summaries.append(
                run_one_mapping(
                    client,
                    table_name=table_name,
                    old_job_id=old_job_id,
                    new_job_id=new_job_id,
                    batch_size=args.batch_size,
                    execute=args.execute,
                )
            )
    finally:
        client.close()

    print("=" * 80)
    print("Summary:")
    print(json.dumps(summaries, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
