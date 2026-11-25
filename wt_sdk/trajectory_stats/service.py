"""Production new-stack orchestration for trajectory session/category metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import wt_sdk._time as sdk_time

from .aggregation import aggregate_category_metrics
from .schemas import (
    CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS,
    CLASSIFICATION_STATUS_VALID,
)
from .session_metrics import compute_session_metrics
from .store import CategoryKey, TrajectoryStatsStore


SessionIdentity = tuple[str, str]
LANDING_METRIC_COLUMNS = [
    "id",
    "job_id",
    "session_id",
    "sub_job_id",
    "step_id",
    "meta_json",
    "response",
    "messages",
]


@dataclass(frozen=True)
class TrajectoryStatsUpdate:
    sessions_updated: int = 0
    categories_upserted: int = 0
    categories_deleted: int = 0


class TrajectoryStatsService:
    """Refresh delivered sessions and recompute affected category aggregates."""

    def __init__(self, client, store: TrajectoryStatsStore) -> None:
        profile = getattr(getattr(client, "config", None), "tables", None)
        if getattr(profile, "profile", None) != "production":
            raise ValueError("trajectory statistics writes require the production profile")
        self.client = client
        self.store = store

    def discover_delivered_sessions(
        self,
        job_ids: Optional[Iterable[str]] = None,
    ) -> list[SessionIdentity]:
        """Discover distinct delivered parent sessions from production serving."""
        normalized_jobs = (
            sorted({_required_string("job_id", value) for value in job_ids})
            if job_ids is not None
            else None
        )
        if normalized_jobs == []:
            return []
        if normalized_jobs is None:
            query = "job_id IS NOT NULL AND session_id IS NOT NULL"
        else:
            jobs = ", ".join(f"'{_escape_sql(job_id)}'" for job_id in normalized_jobs)
            query = f"job_id IN ({jobs}) AND session_id IS NOT NULL"
        rows = self.client.query_data(
            filter_query=query,
            columns=["job_id", "session_id"],
            checkout_latest=True,
            table=self.client.config.tables.serving_table,
        )
        return sorted(
            {
                (row["job_id"], row["session_id"])
                for row in rows
                if isinstance(row.get("job_id"), str)
                and row.get("job_id")
                and isinstance(row.get("session_id"), str)
                and row.get("session_id")
            }
        )

    def refresh_delivered_sessions(
        self,
        job_ids: Optional[Iterable[str]] = None,
        *,
        updated_at_ms: Optional[int] = None,
    ) -> TrajectoryStatsUpdate:
        return self.update_sessions(
            self.discover_delivered_sessions(job_ids),
            updated_at_ms=updated_at_ms,
        )

    def update_sessions(
        self,
        session_keys: Iterable[SessionIdentity],
        *,
        updated_at_ms: Optional[int] = None,
    ) -> TrajectoryStatsUpdate:
        """Idempotently recompute complete parent sessions from all landing shards."""
        keys = sorted({_session_identity(key) for key in session_keys})
        if not keys:
            return TrajectoryStatsUpdate()
        timestamp = sdk_time.now_ms() if updated_at_ms is None else updated_at_ms
        existing_rows = self.store.list_session_rows()
        sessions_by_key = {
            (row["job_id"], row["session_id"]): row for row in existing_rows
        }
        landing_by_key = self._load_landing_rows(keys)

        computed_rows = [
            compute_session_metrics(
                job_id,
                session_id,
                landing_by_key.get((job_id, session_id), []),
                updated_at_ms=timestamp,
            )
            for job_id, session_id in keys
        ]
        # Only old/new categories for changed sessions can be affected.
        changed_keys = set(keys)
        affected_categories = {
            category
            for row in [*existing_rows, *computed_rows]
            if (row.get("job_id"), row.get("session_id")) in changed_keys
            and (category := _valid_category_key(row)) is not None
        }

        self.store.upsert_session_rows(computed_rows)
        for row in computed_rows:
            sessions_by_key[(row["job_id"], row["session_id"])] = row
        aggregates = aggregate_category_metrics(
            sessions_by_key.values(),
            updated_at_ms=timestamp,
        )
        category_rows = [
            row
            for row in aggregates
            if tuple(row[column] for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS)
            in affected_categories
        ]
        written_categories = {
            tuple(row[column] for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS)
            for row in category_rows
        }
        deleted_categories = affected_categories - written_categories

        categories_upserted = (
            self.store.upsert_category_rows(category_rows) if category_rows else 0
        )
        categories_deleted = (
            self.store.delete_category_rows(deleted_categories)
            if deleted_categories
            else 0
        )
        return TrajectoryStatsUpdate(
            sessions_updated=len(computed_rows),
            categories_upserted=categories_upserted,
            categories_deleted=categories_deleted,
        )

    def rebuild_all_categories(
        self,
        *,
        updated_at_ms: Optional[int] = None,
    ) -> TrajectoryStatsUpdate:
        """Rebuild every category from the complete new session table."""
        return rebuild_category_table(self.store, updated_at_ms=updated_at_ms)

    def _load_landing_rows(
        self,
        keys: list[SessionIdentity],
    ) -> dict[SessionIdentity, list[dict[str, object]]]:
        sessions_by_job: dict[str, set[str]] = {}
        for job_id, session_id in keys:
            sessions_by_job.setdefault(job_id, set()).add(session_id)

        result: dict[SessionIdentity, list[dict[str, object]]] = {key: [] for key in keys}
        for job_id, session_ids in sorted(sessions_by_job.items()):
            # Resolve explicitly so env read failures abort before any writes. The
            # subsequent public query uses the same SDK shard-resolution contract.
            shards = set(self.client.resolve_sub_jobs(job_id))
            session_literals = ", ".join(
                f"'{_escape_sql(session_id)}'" for session_id in sorted(session_ids)
            )
            rows = self.client.query_data(
                filter_query=(
                    f"job_id = '{_escape_sql(job_id)}' "
                    f"AND session_id IN ({session_literals})"
                ),
                columns=LANDING_METRIC_COLUMNS,
                checkout_latest=True,
                table=self.client.config.tables.landing_table,
            )
            for row in rows:
                if row.get("job_id") != job_id or row.get("session_id") not in session_ids:
                    raise ValueError("landing query returned a row outside the requested sessions")
                sub_job_id = row.get("sub_job_id")
                if len(shards) > 1 and sub_job_id not in shards:
                    raise ValueError(
                        f"split job {job_id!r} returned landing row with unknown sub_job_id"
                    )
                result[(job_id, row["session_id"])].append(row)
        return result


def rebuild_category_table(
    store: TrajectoryStatsStore,
    *,
    updated_at_ms: Optional[int] = None,
) -> TrajectoryStatsUpdate:
    """Rebuild all categories, including legacy and new-stack session rows."""
    timestamp = sdk_time.now_ms() if updated_at_ms is None else updated_at_ms
    aggregates = aggregate_category_metrics(
        store.list_session_rows(),
        updated_at_ms=timestamp,
    )
    desired = {
        tuple(row[column] for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS)
        for row in aggregates
    }
    existing = {
        tuple(row[column] for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS)
        for row in store.list_category_rows()
    }
    stale = existing - desired
    return TrajectoryStatsUpdate(
        categories_upserted=(
            store.upsert_category_rows(aggregates) if aggregates else 0
        ),
        categories_deleted=store.delete_category_rows(stale) if stale else 0,
    )


def _session_identity(value) -> SessionIdentity:
    try:
        job_id, session_id = value
    except (TypeError, ValueError) as exc:
        raise ValueError("session identity must be a (job_id, session_id) pair") from exc
    return _required_string("job_id", job_id), _required_string("session_id", session_id)


def _required_string(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _valid_category_key(row) -> Optional[CategoryKey]:
    if row.get("classification_status") != CLASSIFICATION_STATUS_VALID:
        return None
    key = tuple(row.get(column) for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS)
    if not all(isinstance(value, str) for value in key):
        raise ValueError("valid-classification session has incomplete category fields")
    return key


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")
