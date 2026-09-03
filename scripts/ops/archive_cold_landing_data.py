#!/usr/bin/env python3
"""Archive cold landing jobs without replacing the active table.

The caller supplies one inclusive calendar cutoff date. The script discovers
complete jobs whose every source row is older than that cutoff, verifies that
landing enrichment and serving publication have completed, copies each job to
the profile-specific ``archived_YYYYMMDD_<landing-table>`` table, verifies the
copy, and conditionally deletes the unchanged source rows. The default profile
is test; production must be selected explicitly.

The active landing table remains available throughout the operation. This is
an online, job-at-a-time workflow; it is separate from the full-table quiesced
archive/rebuild scripts.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Sequence
from zoneinfo import ZoneInfo

import dldb
import numpy as np
import pandas as pd
import pyarrow as pa
from dldb.utils import stable_hash

from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    default_config,
)
from wt_sdk.core.schemas import (
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_PARTITIONS,
    LANDING_SCHEMA,
)


ARCHIVE_TIMEZONE = "Asia/Shanghai"
DEFAULT_BATCH_SIZE = 500
DISCOVERY_COLUMNS = ["job_id"]
ELIGIBILITY_COLUMNS = [
    "id",
    "job_id",
    "session_id",
    "source_updated_at",
    "is_session_completed",
    "is_trainable",
]
SERVING_VERIFICATION_COLUMNS = ["id", "source_updated_at"]


@dataclass(frozen=True)
class ColdJob:
    job_id: str
    bucket: int
    row_count: int
    max_source_updated_at: int
    source_updated_at_by_id: Mapping[str, int]
    trainable_ids: frozenset[str]


@dataclass(frozen=True)
class ArchiveTarget:
    profile: str
    source_table: str
    serving_table: str
    archive_table: str


def resolve_archive_target(
    profile: str,
    *,
    now: datetime | None = None,
) -> ArchiveTarget:
    normalized = profile.strip().lower()
    if normalized == "prod":
        normalized = "production"
    if normalized not in {"test", "production"}:
        raise ValueError("profile must be one of: test, production, prod")

    if normalized == "test":
        source_table = TEST_LANDING_TABLE
        serving_table = TEST_SERVING_TABLE
    else:
        source_table = DEFAULT_LANDING_TABLE
        serving_table = DEFAULT_SERVING_TABLE
    return ArchiveTarget(
        profile=normalized,
        source_table=source_table,
        serving_table=serving_table,
        archive_table=default_archive_table(source_table=source_table, now=now),
    )


def parse_cutoff_date(value: str) -> tuple[date, int]:
    """Return the inclusive date and next-midnight exclusive epoch-ms bound."""
    try:
        cutoff_date = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("cutoff date must use YYYY-MM-DD") from exc
    timezone = ZoneInfo(ARCHIVE_TIMEZONE)
    next_midnight = datetime.combine(
        cutoff_date + timedelta(days=1),
        time.min,
        tzinfo=timezone,
    )
    return cutoff_date, int(next_midnight.timestamp() * 1000)


def default_archive_table(
    *,
    source_table: str = DEFAULT_LANDING_TABLE,
    now: datetime | None = None,
) -> str:
    timezone = ZoneInfo(ARCHIVE_TIMEZONE)
    current = now.astimezone(timezone) if now is not None else datetime.now(timezone)
    return f"archived_{current.strftime('%Y%m%d')}_{source_table}"


def quote_sql_string(value: str) -> str:
    return f"'{value.replace(chr(39), chr(39) * 2)}'"


def exact_job_filter(job_id: str) -> str:
    return f"job_id = {quote_sql_string(job_id)}"


def ids_filter(record_ids: Sequence[str]) -> str:
    if not record_ids:
        raise ValueError("record_ids must not be empty")
    return "id IN (" + ", ".join(quote_sql_string(record_id) for record_id in record_ids) + ")"


def partition_metadata(record) -> Dict[str, Any]:
    return {
        "partition_column": record.partition_column,
        "partition_type": record.partition_type,
        "partitions": record.partitions,
    }


def expected_landing_metadata() -> Dict[str, Any]:
    return {
        "partition_column": LANDING_PARTITION_COLUMN,
        "partition_type": LANDING_PARTITION_TYPE,
        "partitions": LANDING_PARTITIONS,
    }


def verify_landing_layout(session, table_name: str) -> None:
    if not session.table_exists(table_name):
        raise ValueError(f"Required table {table_name!r} does not exist")
    record = session.schema_table.get(table_name)
    if partition_metadata(record) != expected_landing_metadata():
        raise ValueError(
            f"Table {table_name!r} has unexpected partition metadata: "
            f"{partition_metadata(record)}"
        )
    if session.get_schema(table_name) != LANDING_SCHEMA:
        raise ValueError(f"Table {table_name!r} schema differs from LANDING_SCHEMA")


def list_partitions(session, table_name: str) -> List[int]:
    table = session._get_table(table_name)
    return sorted(int(value) for value in table.list_partitions())


def query_partition(
    session,
    *,
    table_name: str,
    partition: int,
    query: str,
    columns: Sequence[str],
    existing_partitions: set[int] | None = None,
) -> pd.DataFrame:
    if existing_partitions is not None and partition not in existing_partitions:
        return pd.DataFrame(columns=list(columns))
    frame = session.filter(
        table_name,
        query=query,
        limit=None,
        columns=list(columns),
        partitions=[partition],
        checkout_latest=True,
    )
    if frame is None:
        return pd.DataFrame(columns=list(columns))
    return frame


def _required_string(value: Any, *, field: str) -> str:
    if value is None or pd.isna(value):
        raise ValueError(f"{field} must be non-null")
    text = str(value)
    if not text:
        raise ValueError(f"{field} must be non-empty")
    return text


def _required_int(value: Any, *, field: str) -> int:
    if value is None or pd.isna(value) or isinstance(value, bool):
        raise ValueError(f"{field} must be a non-null integer")
    return int(value)


def _is_true(value: Any) -> bool:
    return value is not None and not pd.isna(value) and bool(value)


def evaluate_cold_job(
    frame: pd.DataFrame,
    *,
    job_id: str,
    bucket: int,
    cutoff_exclusive_ms: int,
) -> tuple[ColdJob | None, str | None]:
    """Classify one complete latest-snapshot job frame."""
    if frame is None or frame.empty:
        return None, "job disappeared during discovery"
    missing_columns = sorted(set(ELIGIBILITY_COLUMNS) - set(frame.columns))
    if missing_columns:
        return None, f"missing eligibility columns: {missing_columns}"

    source_updated_at_by_id: Dict[str, int] = {}
    trainable_ids: set[str] = set()
    completed_by_session: Dict[str, bool] = {}

    try:
        for row in frame.to_dict(orient="records"):
            row_job_id = _required_string(row.get("job_id"), field="job_id")
            if row_job_id != job_id:
                return None, f"query returned another job_id: {row_job_id!r}"
            record_id = _required_string(row.get("id"), field="id")
            if record_id in source_updated_at_by_id:
                return None, f"duplicate id: {record_id!r}"
            session_id = _required_string(row.get("session_id"), field="session_id")
            updated_at = _required_int(
                row.get("source_updated_at"),
                field="source_updated_at",
            )
            source_updated_at_by_id[record_id] = updated_at
            completed_by_session[session_id] = (
                completed_by_session.get(session_id, False)
                or _is_true(row.get("is_session_completed"))
            )
            if row.get("is_trainable") is None or pd.isna(row.get("is_trainable")):
                return None, "landing enrichment has not populated is_trainable for every row"
            if _is_true(row.get("is_trainable")):
                trainable_ids.add(record_id)
    except (TypeError, ValueError) as exc:
        return None, str(exc)

    incomplete_sessions = sorted(
        session_id
        for session_id, completed in completed_by_session.items()
        if not completed
    )
    if incomplete_sessions:
        return None, f"incomplete sessions: {incomplete_sessions[:5]}"

    max_updated_at = max(source_updated_at_by_id.values())
    if max_updated_at >= cutoff_exclusive_ms:
        return None, "job contains rows newer than the inclusive cutoff date"

    return (
        ColdJob(
            job_id=job_id,
            bucket=bucket,
            row_count=len(source_updated_at_by_id),
            max_source_updated_at=max_updated_at,
            source_updated_at_by_id=source_updated_at_by_id,
            trainable_ids=frozenset(trainable_ids),
        ),
        None,
    )


def verify_serving_publication(
    session,
    *,
    job: ColdJob,
    serving_table: str,
    serving_partitions: set[int],
) -> tuple[bool, str | None]:
    if not job.trainable_ids:
        return True, None
    frame = query_partition(
        session,
        table_name=serving_table,
        partition=job.bucket,
        query=exact_job_filter(job.job_id),
        columns=SERVING_VERIFICATION_COLUMNS,
        existing_partitions=serving_partitions,
    )
    serving_by_id: Dict[str, int] = {}
    try:
        for row in frame.to_dict(orient="records"):
            record_id = _required_string(row.get("id"), field="serving.id")
            if record_id in serving_by_id:
                return False, f"serving contains duplicate id {record_id!r}"
            serving_by_id[record_id] = _required_int(
                row.get("source_updated_at"),
                field="serving.source_updated_at",
            )
    except (TypeError, ValueError) as exc:
        return False, str(exc)

    missing = sorted(job.trainable_ids - set(serving_by_id))
    if missing:
        return False, f"trainable rows missing from serving: {missing[:5]}"
    stale = sorted(
        record_id
        for record_id in job.trainable_ids
        if serving_by_id[record_id] != job.source_updated_at_by_id[record_id]
    )
    if stale:
        return False, f"serving rows have stale source_updated_at: {stale[:5]}"
    return True, None


def discover_cold_jobs(
    session,
    *,
    source_table: str,
    serving_table: str,
    cutoff_exclusive_ms: int,
    max_jobs: int | None,
    only_job_ids: Sequence[str] | None = None,
) -> tuple[List[ColdJob], Dict[str, int]]:
    source_partitions = list_partitions(session, source_table)
    serving_partitions = set(list_partitions(session, serving_table))
    if only_job_ids:
        candidate_job_ids = {str(value) for value in only_job_ids if str(value)}
    else:
        candidate_job_ids = set()
        cutoff_query = (
            f"source_updated_at < {cutoff_exclusive_ms} AND job_id IS NOT NULL"
        )

        for bucket in source_partitions:
            frame = query_partition(
                session,
                table_name=source_table,
                partition=bucket,
                query=cutoff_query,
                columns=DISCOVERY_COLUMNS,
            )
            for value in frame.get("job_id", pd.Series(dtype="object")).tolist():
                if value is not None and not pd.isna(value) and str(value):
                    candidate_job_ids.add(str(value))

    reasons: Dict[str, int] = {}
    eligible: List[ColdJob] = []
    for job_id in sorted(candidate_job_ids):
        bucket = stable_hash(job_id) % LANDING_PARTITIONS
        frame = query_partition(
            session,
            table_name=source_table,
            partition=bucket,
            query=exact_job_filter(job_id),
            columns=ELIGIBILITY_COLUMNS,
        )
        job, reason = evaluate_cold_job(
            frame,
            job_id=job_id,
            bucket=bucket,
            cutoff_exclusive_ms=cutoff_exclusive_ms,
        )
        if job is None:
            reasons[reason or "unknown eligibility failure"] = (
                reasons.get(reason or "unknown eligibility failure", 0) + 1
            )
            continue
        published, reason = verify_serving_publication(
            session,
            job=job,
            serving_table=serving_table,
            serving_partitions=serving_partitions,
        )
        if not published:
            reasons[reason or "serving verification failed"] = (
                reasons.get(reason or "serving verification failed", 0) + 1
            )
            continue
        eligible.append(job)
        if max_jobs is not None and len(eligible) >= max_jobs:
            break
    return eligible, reasons


def ensure_archive_table(session, archive_table: str) -> None:
    if session.table_exists(archive_table):
        verify_landing_layout(session, archive_table)
        return
    session.create_table(
        archive_table,
        LANDING_SCHEMA,
        partition_column=LANDING_PARTITION_COLUMN,
        partition_type=LANDING_PARTITION_TYPE,
        partitions=LANDING_PARTITIONS,
    )
    verify_landing_layout(session, archive_table)
    print(f"Created archive table: {archive_table}")


def _pythonize_nested_value(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return [_pythonize_nested_value(item) for item in value.tolist()]
    if isinstance(value, dict):
        return {key: _pythonize_nested_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_pythonize_nested_value(item) for item in value]
    return value


def to_dldb_write_frame(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in LANDING_SCHEMA.names if column not in frame.columns]
    if missing:
        raise ValueError(f"Source batch is missing schema columns: {missing}")
    normalized = frame.loc[:, LANDING_SCHEMA.names].copy()
    for field in LANDING_SCHEMA:
        if (
            pa.types.is_list(field.type)
            or pa.types.is_large_list(field.type)
            or pa.types.is_struct(field.type)
        ):
            normalized[field.name] = normalized[field.name].map(_pythonize_nested_value)
    table = pa.Table.from_pandas(normalized, schema=LANDING_SCHEMA, preserve_index=False)
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def batched(values: Sequence[str], batch_size: int) -> Iterable[List[str]]:
    for start in range(0, len(values), batch_size):
        yield list(values[start : start + batch_size])


def _id_timestamp_map(frame: pd.DataFrame, *, label: str) -> Dict[str, int]:
    result: Dict[str, int] = {}
    for row in frame.to_dict(orient="records"):
        record_id = _required_string(row.get("id"), field=f"{label}.id")
        if record_id in result:
            raise RuntimeError(f"{label} contains duplicate id {record_id!r}")
        result[record_id] = _required_int(
            row.get("source_updated_at"),
            field=f"{label}.source_updated_at",
        )
    return result


def query_job_id_timestamps(
    session,
    *,
    table_name: str,
    job: ColdJob,
    existing_partitions: set[int] | None = None,
) -> Dict[str, int]:
    frame = query_partition(
        session,
        table_name=table_name,
        partition=job.bucket,
        query=exact_job_filter(job.job_id),
        columns=["id", "source_updated_at"],
        existing_partitions=existing_partitions,
    )
    return _id_timestamp_map(frame, label=table_name)


def copy_job_to_archive(
    session,
    *,
    job: ColdJob,
    source_table: str,
    archive_table: str,
    batch_size: int,
) -> int:
    archive_partitions = set(list_partitions(session, archive_table))
    archived = query_job_id_timestamps(
        session,
        table_name=archive_table,
        job=job,
        existing_partitions=archive_partitions,
    )
    unexpected = sorted(set(archived) - set(job.source_updated_at_by_id))
    if unexpected:
        raise RuntimeError(
            f"Archive already contains unexpected ids for {job.job_id!r}: {unexpected[:5]}"
        )
    stale = sorted(
        record_id
        for record_id, updated_at in archived.items()
        if updated_at != job.source_updated_at_by_id[record_id]
    )
    if stale:
        raise RuntimeError(
            f"Archive contains stale rows for {job.job_id!r}: {stale[:5]}"
        )

    missing_ids = sorted(set(job.source_updated_at_by_id) - set(archived))
    copied = 0
    for record_ids in batched(missing_ids, batch_size):
        query = f"({exact_job_filter(job.job_id)}) AND {ids_filter(record_ids)}"
        frame = query_partition(
            session,
            table_name=source_table,
            partition=job.bucket,
            query=query,
            columns=LANDING_SCHEMA.names,
        )
        returned = _id_timestamp_map(frame, label=source_table)
        if set(returned) != set(record_ids):
            missing = sorted(set(record_ids) - set(returned))
            raise RuntimeError(
                f"Source changed while copying {job.job_id!r}; missing ids: {missing[:5]}"
            )
        changed = sorted(
            record_id
            for record_id, updated_at in returned.items()
            if updated_at != job.source_updated_at_by_id[record_id]
        )
        if changed:
            raise RuntimeError(
                f"Source changed while copying {job.job_id!r}: {changed[:5]}"
            )
        session.add(
            archive_table,
            to_dldb_write_frame(frame),
            partition=job.bucket,
        )
        copied += len(frame)
        print(
            f"  {job.job_id}: copied {copied}/{len(missing_ids)} new rows "
            f"({len(archived) + copied}/{job.row_count} archived)",
            flush=True,
        )

    archived_after = query_job_id_timestamps(
        session,
        table_name=archive_table,
        job=job,
    )
    if archived_after != dict(job.source_updated_at_by_id):
        raise RuntimeError(f"Archive verification failed for job {job.job_id!r}")
    return copied


def revalidate_job(
    session,
    job: ColdJob,
    cutoff_exclusive_ms: int,
    *,
    source_table: str,
) -> None:
    frame = query_partition(
        session,
        table_name=source_table,
        partition=job.bucket,
        query=exact_job_filter(job.job_id),
        columns=ELIGIBILITY_COLUMNS,
    )
    latest, reason = evaluate_cold_job(
        frame,
        job_id=job.job_id,
        bucket=job.bucket,
        cutoff_exclusive_ms=cutoff_exclusive_ms,
    )
    if latest is None:
        raise RuntimeError(f"Job became ineligible before deletion: {reason}")
    if dict(latest.source_updated_at_by_id) != dict(job.source_updated_at_by_id):
        raise RuntimeError("Job changed after discovery; source rows were not deleted")


def delete_verified_job(
    session,
    *,
    job: ColdJob,
    source_table: str,
    cutoff_exclusive_ms: int,
) -> None:
    revalidate_job(
        session,
        job,
        cutoff_exclusive_ms,
        source_table=source_table,
    )
    delete_query = (
        f"({exact_job_filter(job.job_id)}) AND "
        f"source_updated_at < {cutoff_exclusive_ms}"
    )
    session.delete(
        source_table,
        delete_query,
        partition=job.bucket,
    )
    remaining = query_job_id_timestamps(
        session,
        table_name=source_table,
        job=job,
    )
    if remaining:
        raise RuntimeError(
            f"Conditional deletion left {len(remaining)} rows for {job.job_id!r}; "
            "the archive remains intact and the job requires reconciliation"
        )


def rollback_archive_job_if_source_intact(
    session,
    *,
    job: ColdJob,
    source_table: str,
    archive_table: str,
) -> bool:
    """Remove a staged archive copy only while every source ID still exists."""
    source = query_job_id_timestamps(
        session,
        table_name=source_table,
        job=job,
    )
    if set(source) != set(job.source_updated_at_by_id):
        return False
    archive_partitions = set(list_partitions(session, archive_table))
    if job.bucket not in archive_partitions:
        return True
    session.delete(
        archive_table,
        exact_job_filter(job.job_id),
        partition=job.bucket,
    )
    remaining = query_job_id_timestamps(
        session,
        table_name=archive_table,
        job=job,
    )
    if remaining:
        raise RuntimeError(
            f"Failed to roll back staged archive rows for {job.job_id!r}"
        )
    return True


def run_archive(
    *,
    cutoff_date_value: str,
    batch_size: int,
    execute: bool,
    confirm_delete: bool,
    max_jobs: int | None,
    db_uri: str | None,
    profile: str = "test",
    job_ids: Sequence[str] | None = None,
    archive_table_override: str | None = None,
) -> Dict[str, Any]:
    cutoff_date, cutoff_exclusive_ms = parse_cutoff_date(cutoff_date_value)
    target = resolve_archive_target(profile)
    if archive_table_override is not None:
        if target.profile != "test":
            raise ValueError("archive_table_override is restricted to test profile")
        valid_test_archive_name = (
            archive_table_override.startswith("archived_")
            and archive_table_override.endswith("_v2_landing_test")
        )
        if not valid_test_archive_name:
            raise ValueError(
                "test archive override must match archived_*_v2_landing_test"
            )
        target = ArchiveTarget(
            profile=target.profile,
            source_table=target.source_table,
            serving_table=target.serving_table,
            archive_table=archive_table_override,
        )
    if execute and not confirm_delete:
        raise ValueError("--execute requires --confirm-delete")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if max_jobs is not None and max_jobs <= 0:
        raise ValueError("max_jobs must be positive")

    session = dldb.connect(
        db_uri or default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )
    try:
        verify_landing_layout(session, target.source_table)
        verify_landing_layout(session, target.serving_table)
        jobs, excluded_reasons = discover_cold_jobs(
            session,
            source_table=target.source_table,
            serving_table=target.serving_table,
            cutoff_exclusive_ms=cutoff_exclusive_ms,
            max_jobs=max_jobs,
            only_job_ids=job_ids,
        )
        summary: Dict[str, Any] = {
            "mode": "execute" if execute else "dry-run",
            "database": db_uri or default_config.tables.db_uri,
            "profile": target.profile,
            "source_table": target.source_table,
            "serving_table": target.serving_table,
            "archive_table": target.archive_table,
            "cutoff_date_inclusive": cutoff_date.isoformat(),
            "cutoff_timezone": ARCHIVE_TIMEZONE,
            "eligible_jobs": len(jobs),
            "eligible_rows": sum(job.row_count for job in jobs),
            "excluded_reasons": excluded_reasons,
            "requested_job_ids": sorted(set(job_ids or [])),
            "archived_jobs": [],
            "affected_buckets": sorted({job.bucket for job in jobs}),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        if not execute or not jobs:
            return summary

        ensure_archive_table(session, target.archive_table)
        completed: List[Dict[str, Any]] = []
        affected_buckets: set[int] = set()
        for index, job in enumerate(jobs, start=1):
            print(
                f"[{index}/{len(jobs)}] Archiving {job.job_id!r}: "
                f"rows={job.row_count}, bucket={job.bucket}"
            )
            try:
                copied = copy_job_to_archive(
                    session,
                    job=job,
                    source_table=target.source_table,
                    archive_table=target.archive_table,
                    batch_size=batch_size,
                )
                delete_verified_job(
                    session,
                    job=job,
                    source_table=target.source_table,
                    cutoff_exclusive_ms=cutoff_exclusive_ms,
                )
            except Exception:
                rolled_back = rollback_archive_job_if_source_intact(
                    session,
                    job=job,
                    source_table=target.source_table,
                    archive_table=target.archive_table,
                )
                if rolled_back:
                    print(
                        f"  rolled back staged archive rows for {job.job_id!r}; "
                        "the complete source job remains in landing",
                        file=sys.stderr,
                    )
                else:
                    print(
                        f"  source job {job.job_id!r} is no longer complete; "
                        "archive rows were retained for reconciliation",
                        file=sys.stderr,
                    )
                raise
            affected_buckets.add(job.bucket)
            completed.append(
                {
                    "job_id": job.job_id,
                    "bucket": job.bucket,
                    "rows": job.row_count,
                    "new_rows_copied": copied,
                }
            )
            print(f"  verified and deleted {job.row_count} source rows")

        summary["archived_jobs"] = completed
        summary["affected_buckets"] = sorted(affected_buckets)
        print("=" * 80)
        print("Online cold landing archive complete")
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        if affected_buckets:
            args = " ".join(
                f"--partition {bucket}" for bucket in sorted(affected_buckets)
            )
            print("Suggested later low-traffic maintenance command:")
            print(
                "python scripts/ops/maintain_table_indexes.py "
                f"--table {target.source_table} {args}"
            )
        return summary
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Discover and archive complete landing jobs whose latest "
            "source update falls on or before an inclusive YYYY-MM-DD cutoff."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--job-id",
        action="append",
        default=None,
        help=(
            "Optional exact job_id canary scope; repeat for multiple jobs. "
            "Without it, the script discovers eligible jobs from the cutoff."
        ),
    )
    parser.add_argument(
        "--profile",
        choices=("test", "production", "prod"),
        default="test",
        help=(
            "Select v2_landing_test/serving_test or the production pair. "
            "The default is deliberately test-safe."
        ),
    )
    parser.add_argument(
        "--cutoff-date",
        required=True,
        help=(
            "Inclusive cutoff date in YYYY-MM-DD, interpreted in Asia/Shanghai. "
            "Only whole cold jobs are eligible."
        ),
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=None,
        help="Optional canary limit on the number of eligible jobs processed.",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI; defaults to configured WT_SDK_DB_URI.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Create/write the archive and delete verified source jobs.",
    )
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Required with --execute because source rows are deleted after verification.",
    )
    args = parser.parse_args()

    try:
        run_archive(
            cutoff_date_value=args.cutoff_date,
            batch_size=args.batch_size,
            execute=args.execute,
            confirm_delete=args.confirm_delete,
            max_jobs=args.max_jobs,
            db_uri=args.db_uri,
            profile=args.profile,
            job_ids=args.job_id,
        )
    except Exception as exc:
        print(f"Cold landing archive failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
