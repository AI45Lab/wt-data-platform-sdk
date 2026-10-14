#!/usr/bin/env python3
"""Migrate serving rows from incorrect job_id values to corrected job_id values.

job_id is the HASH partition key of the serving table and cannot be updated in
place. This script implements the safe copy-verify-delete workflow:

1. read every full row under the old job_id;
2. rewrite ``job_id``, recompute ``tags`` from the new job_id, and rebuild
   ``search_text`` with the same join algorithm as the ETL
   ``BuildSearchTextStage`` (tags are appended line-by-line into search_text,
   so a job_id fix must also rebuild search_text);
3. ``upsert_serving_batch`` the migrated rows into the new job's HASH bucket;
4. verify the new job_id contains exactly the old id set;
5. optionally delete the old rows (only with --delete-old --confirm-delete).

Before writing any batch, the rebuilt search_text of the unchanged source
fields is compared against the stored value; a mismatch aborts the run so a
nonstandard search_text is never silently replaced.

``--seed-test-from-production`` copies the old job rows verbatim from
production serving into ``serving_test`` first so the whole workflow can be
rehearsed against the disposable test table.
"""

from __future__ import annotations

import argparse
import json
import sys
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
from wt_sdk.models import ServingRecord
from wt_sdk.utils import dataframe_to_serving_records


# Fill this mapping for the real production migration: k is incorrect job_id,
# v is the fixed job_id.
JOB_ID_MAPPING: Dict[str, str] = {}

DEFAULT_BATCH_SIZE = 100

# Mirrors wt_sdk/etl/stages/search_text.py SEARCH_TEXT_SOURCE_FIELDS ordering.
SEARCH_TEXT_SOURCE_FIELDS = (
    "chosen_trace",
    "rejected_trace",
    "agent_model",
    "meta_json",
    "dataset_type",
)


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


def serving_table_for_profile(profile: str) -> str:
    return TEST_SERVING_TABLE if normalize_profile(profile) == "test" else DEFAULT_SERVING_TABLE


def landing_table_for_profile(profile: str) -> str:
    return TEST_LANDING_TABLE if normalize_profile(profile) == "test" else DEFAULT_LANDING_TABLE


def build_config(profile: str) -> GatewayConfig:
    """Build a profile-pinned config that ignores table-name env overrides."""
    canonical_profile = normalize_profile(profile)
    tables = TableConfig(
        profile=canonical_profile,
        db_uri=default_config.tables.db_uri,
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
        from pathlib import Path

        path = Path(args.mapping_file).expanduser()
        mapping.update(parse_mapping_json(path.read_text(encoding="utf-8")))
    if args.mapping_json:
        mapping.update(parse_mapping_json(args.mapping_json))
    return mapping


def derive_tags(job_id: str) -> List[str] | None:
    """Same best-effort rule as the ETL DeriveJobTagsStage."""
    parts = [part.strip() for part in job_id.split("#")]
    return parts[:4] if len(parts) >= 4 and all(parts[:4]) else None


def build_search_text(record: ServingRecord, tags: Sequence[str] | None) -> str | None:
    """Rebuild search_text exactly like the ETL BuildSearchTextStage."""
    parts: List[str] = []
    for field_name in SEARCH_TEXT_SOURCE_FIELDS:
        value = getattr(record, field_name, None)
        if value is None:
            continue
        if not isinstance(value, str):
            raise RuntimeError(
                f"record {getattr(record, 'id', '?')!r}: field {field_name} is "
                f"{type(value).__name__}, expected str or None"
            )
        if value.strip():
            parts.append(value)
    for tag in tags or []:
        if isinstance(tag, str) and tag.strip():
            parts.append(tag)
    return "\n".join(parts) if parts else None


def validate_new_job_id(job_id: str) -> None:
    parts = job_id.split("#")
    if len(parts) < 5 or any(not part.strip() for part in parts[:5]):
        raise ValueError(
            "new job_id must include at least 5 non-empty parts: "
            "dataset#harness#model#task_type#date; "
            f"got {job_id!r}"
        )
    if derive_tags(job_id) is None:
        raise ValueError(f"new job_id {job_id!r} cannot derive tags")


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
    job_id: str,
) -> List[str]:
    try:
        rows = client.query_data(
            filter_query=exact_job_filter(job_id),
            columns=["id"],
            partition=job_id,
            table=client.config.tables.serving_table,
            exclude_none=False,
            deserialize_json=False,
        )
    except ValueError as exc:
        # The job may hash to a bucket that has no physical table yet.
        if "partition" in str(exc) and "does not exist" in str(exc):
            return []
        raise
    ids: List[str] = []
    for row in rows:
        record_id = row.get("id") if isinstance(row, dict) else getattr(row, "id", None)
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


def iter_source_batches(
    client: WTGatewayClient,
    table_name: str,
    job_id: str,
    batch_size: int,
):
    yield from client.export_data_batches(
        table=table_name,
        filter_query=exact_job_filter(job_id),
        batch_size=batch_size,
        deserialize_json=False,
    )


def verify_and_migrate_batch(
    records: Iterable[ServingRecord],
    *,
    old_job_id: str,
    new_job_id: str,
    new_tags: List[str] | None,
    already_present_ids: Set[str],
    verify_only: bool,
) -> tuple[List[ServingRecord], int, int]:
    """Validate stored search_text, then build migrated records.

    Returns (migrated_records, skipped_already_present, search_text_matches).
    Raises before returning any migrated row if a stored search_text cannot be
    reproduced from its own source fields.
    """
    migrated: List[ServingRecord] = []
    skipped = 0
    matches = 0
    for record in records:
        if record.job_id != old_job_id:
            raise RuntimeError(
                f"export returned row {record.id!r} with unexpected job_id "
                f"{record.job_id!r}; expected {old_job_id!r}"
            )
        # Trust gate: the stored search_text must be reproducible from the
        # row's own fields before this script replaces it.
        rebuilt_old = build_search_text(record, getattr(record, "tags", None))
        if rebuilt_old != getattr(record, "search_text", None):
            raise RuntimeError(
                f"record {record.id!r}: stored search_text is not reproducible "
                "from its source fields; refusing to replace it"
            )
        matches += 1
        if verify_only:
            continue
        if record.id in already_present_ids:
            skipped += 1
            continue
        new_search_text = build_search_text(record, new_tags)
        migrated.append(
            record.model_copy(
                update={
                    "job_id": new_job_id,
                    "tags": list(new_tags) if new_tags is not None else None,
                    "search_text": new_search_text,
                }
            )
        )
    return migrated, skipped, matches


def scan_job(
    client: WTGatewayClient,
    table_name: str,
    old_job_id: str,
    new_job_id: str,
    new_tags: List[str] | None,
    batch_size: int,
    already_present_ids: Set[str],
    execute: bool,
) -> Dict[str, int]:
    copied = 0
    verified = 0
    for batch_index, frame in enumerate(
        iter_source_batches(client, table_name, old_job_id, batch_size),
        start=1,
    ):
        source_records = dataframe_to_serving_records(frame)
        migrated_records, skipped, matches = verify_and_migrate_batch(
            source_records,
            old_job_id=old_job_id,
            new_job_id=new_job_id,
            new_tags=new_tags,
            already_present_ids=already_present_ids,
            verify_only=not execute,
        )
        verified += matches
        if execute and migrated_records:
            client.upsert_serving_batch(migrated_records)
            copied += len(migrated_records)
            already_present_ids.update(record.id for record in migrated_records)
        print(
            f"  Batch {batch_index}: read={len(source_records)}, "
            f"verified={matches}, copied={len(migrated_records) if execute else 0}"
            f"{f', skipped(already present)={skipped}' if skipped else ''}"
        )
    return {"verified": verified, "copied": copied}


def run_one_mapping(
    client: WTGatewayClient,
    *,
    old_job_id: str,
    new_job_id: str,
    batch_size: int,
    execute: bool,
    delete_old: bool,
) -> Dict[str, object]:
    table_name = client.config.tables.serving_table
    print("=" * 80)
    print(f"Serving table: {table_name}")
    print(f"Old job_id: {old_job_id}")
    print(f"New job_id: {new_job_id}")

    old_ids = fetch_ids_for_job(client, old_job_id)
    assert_unique_ids(old_ids, label=f"old job_id {old_job_id!r}")
    old_id_set = set(old_ids)

    new_ids_before = fetch_ids_for_job(client, new_job_id)
    assert_unique_ids(new_ids_before, label=f"new job_id {new_job_id!r} before migration")
    foreign_ids = set(new_ids_before).difference(old_id_set)
    if foreign_ids:
        raise RuntimeError(
            f"new job_id {new_job_id!r} already contains ids that do not belong "
            f"to the old job; sample: {sorted(foreign_ids)[:10]}"
        )
    already_present_ids = set(new_ids_before).intersection(old_id_set)

    new_tags = derive_tags(new_job_id)
    print(f"Rows under old job_id: {len(old_ids)}")
    print(f"Rows already present under new job_id with same ids: {len(already_present_ids)}")
    print(f"New tags: {new_tags}")

    print(f"Phase 1 ({'EXECUTE copy' if execute else 'DRY RUN verify'}):")
    counts = scan_job(
        client,
        table_name,
        old_job_id,
        new_job_id,
        new_tags,
        batch_size,
        already_present_ids,
        execute,
    )
    if counts["verified"] != len(old_ids):
        raise RuntimeError(
            f"scan verified {counts['verified']} rows but the old job_id has "
            f"{len(old_ids)} rows; aborting before any conclusion"
        )
    if not execute:
        print(
            "Dry run only: stored search_text verified reproducible for all "
            f"{counts['verified']} rows; no rows were copied or deleted."
        )
        return {
            "old_job_id": old_job_id,
            "new_job_id": new_job_id,
            "old_count": len(old_ids),
            "already_present": len(already_present_ids),
            "copied": 0,
            "deleted_old": 0,
        }

    new_ids_after = fetch_ids_for_job(client, new_job_id)
    assert_unique_ids(new_ids_after, label=f"new job_id {new_job_id!r} after migration")
    missing_after_copy = sorted(old_id_set.difference(new_ids_after))
    if missing_after_copy:
        raise RuntimeError(
            "new job_id does not contain every old id after copy; "
            f"missing sample: {missing_after_copy[:10]}"
        )
    print(
        f"Copy verification passed: all {len(old_ids)} old ids exist under the "
        f"new job_id (copied this run: {counts['copied']})."
    )

    deleted_old = 0
    if delete_old:
        remaining = fetch_ids_for_job(client, old_job_id)
        if remaining:
            print(f"Deleting {len(remaining)} old rows under {old_job_id!r}...")
            deleted_old = client.delete_serving(exact_job_filter(old_job_id))
            leftovers = fetch_ids_for_job(client, old_job_id)
            if leftovers:
                raise RuntimeError(
                    f"old job_id still has {len(leftovers)} rows after deletion"
                )
            print("Old rows deleted and re-verified as absent.")
        else:
            print("Old rows already absent; nothing to delete.")
    else:
        print("Old rows were NOT deleted (pass --delete-old --confirm-delete).")

    return {
        "old_job_id": old_job_id,
        "new_job_id": new_job_id,
        "old_count": len(old_ids),
        "already_present": len(already_present_ids),
        "copied": counts["copied"],
        "deleted_old": deleted_old,
    }


def seed_test_from_production(
    mapping: Mapping[str, str],
    batch_size: int,
    execute: bool,
) -> None:
    """Copy the old job rows verbatim from production serving into serving_test."""
    prod_client = WTGatewayClient(build_config("production"))
    test_client = WTGatewayClient(build_config("test"))
    prod_table = prod_client.config.tables.serving_table
    test_table = test_client.config.tables.serving_table
    print(f"Seeding {test_table} from {prod_table}")
    print(f"Old job_ids: {sorted(mapping)}")
    try:
        for old_job_id in mapping:
            expected = fetch_ids_for_job(prod_client, old_job_id)
            print(f"  {old_job_id}: {len(expected)} rows in production")
            if not execute:
                continue
            copied = 0
            for frame in iter_source_batches(prod_client, prod_table, old_job_id, batch_size):
                records = dataframe_to_serving_records(frame)
                test_client.upsert_serving_batch(records)
                copied += len(records)
            seeded = fetch_ids_for_job(test_client, old_job_id)
            if set(seeded) != set(expected):
                raise RuntimeError(
                    f"seed verification failed for {old_job_id!r}: "
                    f"expected {len(expected)} ids, found {len(seeded)}"
                )
            print(f"  seeded and verified {copied} rows into {test_table}")
        if not execute:
            print("Dry run only; no rows were seeded.")
    finally:
        prod_client.close()
        test_client.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Safely fix wrong serving job_id values via copy-verify-delete: "
            "rewrite job_id, recompute tags, and rebuild search_text."
        )
    )
    parser.add_argument(
        "--profile",
        default="test",
        choices=["test", "prod", "production"],
        help="Serving table profile. Defaults to test.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per copy batch. Defaults to {DEFAULT_BATCH_SIZE}.",
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
    parser.add_argument(
        "--delete-old",
        action="store_true",
        help="After a verified copy, also delete the rows under the old job_id.",
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Required together with --delete-old to actually delete.",
    )
    parser.add_argument(
        "--seed-test-from-production",
        action="store_true",
        help=(
            "Rehearsal helper: copy the old job rows verbatim from production "
            "serving into serving_test, then exit."
        ),
    )
    args = parser.parse_args()
    if args.batch_size <= 0:
        parser.error("--batch-size must be a positive integer")
    if args.delete_old and not args.confirm_delete:
        parser.error("--delete-old also requires --confirm-delete")
    if args.delete_old and not args.execute:
        parser.error("--delete-old also requires --execute")

    try:
        mapping = load_mapping(args)
        validate_mapping(mapping)
    except Exception as exc:
        print(f"Invalid mapping: {exc}", file=sys.stderr)
        return 2

    if args.seed_test_from_production:
        seed_test_from_production(mapping, args.batch_size, args.execute)
        return 0

    print(f"Profile: {normalize_profile(args.profile)}")
    print(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}"
          + (" + DELETE OLD" if args.delete_old else ""))

    client = WTGatewayClient(build_config(args.profile))
    summaries = []
    try:
        for old_job_id, new_job_id in mapping.items():
            summaries.append(
                run_one_mapping(
                    client,
                    old_job_id=old_job_id,
                    new_job_id=new_job_id,
                    batch_size=args.batch_size,
                    execute=args.execute,
                    delete_old=args.delete_old,
                )
            )
    finally:
        client.close()

    print("=" * 80)
    print("Summary:")
    print(json.dumps(summaries, ensure_ascii=False, indent=2, sort_keys=True))
    print(
        "Index maintenance for the affected buckets (run separately):\n"
        "  python scripts/ops/maintain_table_indexes.py \\\n"
        f"    --table {serving_table_for_profile(args.profile)} --partition '<new job_id>'"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
