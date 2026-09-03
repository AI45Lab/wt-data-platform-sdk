#!/usr/bin/env python3
"""Safely patch filtered rows in the configured landing or serving table.

Examples:
  python scripts/ops/update_table_rows.py \
    --profile test \
    --table landing \
    --query "job_id = 'job-123' AND session_id = 'session-1'" \
    --updates '{"is_session_completed": true}' \
    --dry-run

  python scripts/ops/update_table_rows.py \
    --profile prod \
    --table serving \
    --query "job_id = 'job-123' AND id = 'record-1'" \
    --updates '{"is_session_completed": true}'

The script reads matching rows first, skips rows that already contain the
requested values, updates the remaining IDs in batches, and verifies the
requested values with checkout_latest=True. Landing updates refresh
source_updated_at; serving updates refresh serving_updated_at.
"""

import argparse
import json
import math
import sys
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import numpy as np

import wt_sdk._time as sdk_time
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
from wt_sdk.core.schemas import LANDING_SCHEMA


JSON_COLUMNS = {
    "messages",
    "response",
    "chosen_trace",
    "rejected_trace",
    "meta_json",
}
PROTECTED_COLUMNS = {
    "id",
    "job_id",
    "created_at",
    "source_updated_at",
    "serving_updated_at",
}
BATCH_SIZE = 500


def normalize_profile(profile: str) -> str:
    """Return the canonical profile name accepted by TableConfig."""
    if profile in {"prod", "production"}:
        return "production"
    if profile == "test":
        return "test"
    raise ValueError("profile must be one of: test, prod, production")


def profile_table_name(profile: str, table_role: str) -> str:
    """Resolve only the four supported active tables from profile and role."""
    canonical_profile = normalize_profile(profile)
    if table_role == "landing":
        return TEST_LANDING_TABLE if canonical_profile == "test" else DEFAULT_LANDING_TABLE
    if table_role == "serving":
        return TEST_SERVING_TABLE if canonical_profile == "test" else DEFAULT_SERVING_TABLE
    raise ValueError("table role must be 'landing' or 'serving'")


def build_config(profile: str, db_uri: str | None = None) -> GatewayConfig:
    """Build a profile-pinned config that ignores explicit table-name env overrides."""
    canonical_profile = normalize_profile(profile)
    tables = TableConfig(
        profile=canonical_profile,
        db_uri=db_uri or default_config.tables.db_uri,
        landing_table=profile_table_name(canonical_profile, "landing"),
        serving_table=profile_table_name(canonical_profile, "serving"),
    )
    return GatewayConfig(
        s3=default_config.s3,
        tables=tables,
        dldb_model=default_config.dldb_model,
        enable_dldb_timing_logs=default_config.enable_dldb_timing_logs,
        log_dldb_metrics_summary_on_close=default_config.log_dldb_metrics_summary_on_close,
        dldb_metrics_log_path=default_config.dldb_metrics_log_path,
    )


def parse_updates(raw_updates: str) -> Dict[str, Any]:
    """Parse and validate the user-provided JSON patch."""
    try:
        updates = json.loads(raw_updates)
    except json.JSONDecodeError as exc:
        raise ValueError(f"--updates must be valid JSON: {exc.msg}") from exc

    if not isinstance(updates, dict) or not updates:
        raise ValueError("--updates must be a non-empty JSON object")

    schema_fields = {field.name: field for field in LANDING_SCHEMA}
    unknown = set(updates).difference(schema_fields)
    if unknown:
        raise ValueError(f"unknown update columns: {', '.join(sorted(unknown))}")

    protected = set(updates).intersection(PROTECTED_COLUMNS)
    if protected:
        raise ValueError(
            "SDK-managed or immutable columns cannot be updated: "
            f"{', '.join(sorted(protected))}"
        )

    normalized: Dict[str, Any] = {}
    for column, value in updates.items():
        field = schema_fields[column]
        if value is None and not field.nullable:
            raise ValueError(f"column {column!r} is not nullable")
        if column in JSON_COLUMNS and value is not None and not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        normalized[column] = value
    return normalized


def values_equal(current: Any, desired: Any) -> bool:
    """Compare values returned through Pandas with JSON/CLI patch values."""
    if isinstance(current, np.ndarray):
        current = current.tolist()
    if isinstance(desired, np.ndarray):
        desired = desired.tolist()

    if current is None or desired is None:
        return current is None and desired is None

    if isinstance(current, float) and math.isnan(current):
        return desired is None or (isinstance(desired, float) and math.isnan(desired))
    if isinstance(desired, float) and math.isnan(desired):
        return isinstance(current, float) and math.isnan(current)

    if isinstance(current, (list, tuple)) or isinstance(desired, (list, tuple)):
        if not isinstance(current, (list, tuple)) or not isinstance(desired, (list, tuple)):
            return False
        return list(current) == list(desired)
    return bool(current == desired)


def rows_requiring_update(
    rows: Iterable[Mapping[str, Any]],
    updates: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Return matching rows whose requested business values differ."""
    return [
        dict(row)
        for row in rows
        if any(not values_equal(row.get(column), value) for column, value in updates.items())
    ]


def escape_sql_string(value: str) -> str:
    return value.replace("'", "''")


def chunked(
    rows: Sequence[Dict[str, Any]],
    size: int = BATCH_SIZE,
) -> Iterable[Sequence[Dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def group_rows_by_job(rows: Iterable[Dict[str, Any]]) -> Dict[Any, List[Dict[str, Any]]]:
    grouped: Dict[Any, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row.get("job_id"), []).append(row)
    return grouped


def ids_filter(rows: Sequence[Mapping[str, Any]], job_id: Any) -> str:
    ids = [row.get("id") for row in rows]
    if any(not isinstance(record_id, str) or not record_id for record_id in ids):
        raise ValueError("every matched row must have a non-empty string id")
    quoted_ids = ", ".join(f"'{escape_sql_string(record_id)}'" for record_id in ids)
    predicate = f"id IN ({quoted_ids})"
    if job_id is not None:
        predicate = f"job_id = '{escape_sql_string(str(job_id))}' AND {predicate}"
    return predicate


def query_rows(
    client: WTGatewayClient,
    table_name: str,
    query: str,
    columns: List[str],
    *,
    partition: Any = None,
) -> List[Dict[str, Any]]:
    return client.query_data(
        filter_query=query,
        columns=columns,
        partition=partition,
        table=table_name,
        exclude_none=False,
        deserialize_json=False,
        checkout_latest=True,
    )


def update_and_verify(
    client: WTGatewayClient,
    table_role: str,
    table_name: str,
    rows: Sequence[Dict[str, Any]],
    updates: Mapping[str, Any],
    source_query: str,
) -> int:
    """Update selected IDs and return the number verified at requested values."""
    timestamp_column = "source_updated_at" if table_role == "landing" else "serving_updated_at"
    effective_updates = dict(updates)
    effective_updates[timestamp_column] = sdk_time.now_ms()
    verified = 0

    for job_id, job_rows in group_rows_by_job(rows).items():
        for batch in chunked(job_rows):
            id_predicate = ids_filter(batch, job_id)
            update_predicate = f"({source_query}) AND ({id_predicate})"
            if table_role == "landing":
                client.update_landing(
                    filter_query=update_predicate,
                    updates=dict(updates),
                    partition=job_id,
                )
            else:
                resolved_partition = (
                    client._resolve_explicit_partition_for_table(
                        table_name,
                        job_id,
                        client.SERVING_PARTITION_KEY,
                    )
                    if job_id is not None
                    else None
                )
                client.session.update(
                    table_name,
                    update_predicate,
                    effective_updates,
                    partition=resolved_partition,
                )

            current_rows = query_rows(
                client,
                table_name,
                id_predicate,
                ["id", *updates.keys()],
                partition=job_id,
            )
            verified += sum(
                all(values_equal(row.get(column), value) for column, value in updates.items())
                for row in current_rows
            )

    return verified


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Patch filtered rows in a profile-selected landing or serving table."
    )
    parser.add_argument(
        "--profile",
        required=True,
        choices=("test", "prod", "production"),
        help="Select v2_landing_test/serving_test or the production pair.",
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=("landing", "serving"),
        help="Logical table role within the selected profile.",
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Required dldb WHERE predicate selecting rows to update.",
    )
    parser.add_argument(
        "--updates",
        required=True,
        help='Non-empty JSON object of column/value updates, e.g. \'{"is_session_completed": true}\'.',
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: configured WT_SDK_DB_URI).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows requiring changes without writing anything.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive exact-table confirmation.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not args.query.strip():
        parser.error("--query must not be empty")

    try:
        updates = parse_updates(args.updates)
    except ValueError as exc:
        parser.error(str(exc))

    profile = normalize_profile(args.profile)
    table_name = profile_table_name(profile, args.table)
    config = build_config(profile, args.db_uri)
    client = WTGatewayClient(config)
    try:
        columns = list(dict.fromkeys(["id", "job_id", *updates.keys()]))
        matched_rows = query_rows(client, table_name, args.query, columns)
        changed_rows = rows_requiring_update(matched_rows, updates)

        preview = {
            "profile": profile,
            "table_role": args.table,
            "table_name": table_name,
            "filter_query": args.query,
            "matched_rows": len(matched_rows),
            "rows_requiring_update": len(changed_rows),
            "updates": updates,
            "dry_run": args.dry_run,
        }
        print(json.dumps(preview, ensure_ascii=False, indent=2, sort_keys=True))

        if args.dry_run or not changed_rows:
            print(f"Modified rows: 0")
            return 0

        if not args.yes:
            try:
                confirmation = input(
                    f"Type the exact table name {table_name!r} to update "
                    f"{len(changed_rows)} rows: "
                )
            except (EOFError, KeyboardInterrupt):
                print("\nAborted.")
                return 1
            if confirmation != table_name:
                print("Aborted: table name did not match.")
                return 1

        verified_rows = update_and_verify(
            client,
            args.table,
            table_name,
            changed_rows,
            updates,
            args.query,
        )
        result = {
            "table_name": table_name,
            "requested_rows": len(changed_rows),
            "modified_rows": verified_rows,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        print(f"Modified rows: {verified_rows}")
        if verified_rows != len(changed_rows):
            print(
                "Error: post-update verification did not find every requested row at the target values.",
                file=sys.stderr,
            )
            return 1
        return 0
    except Exception as exc:
        print(f"Update failed; some dldb buckets may already have been changed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
