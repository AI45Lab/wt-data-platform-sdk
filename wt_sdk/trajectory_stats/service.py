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
from .session_metrics import SessionMetricsAccumulator
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
        landing_page_size: int = 100,
    ) -> TrajectoryStatsUpdate:
        return self.update_sessions(
            self.discover_delivered_sessions(job_ids),
            updated_at_ms=updated_at_ms,
            landing_page_size=landing_page_size,
        )

    def update_sessions(
        self,
        session_keys: Iterable[SessionIdentity],
        *,
        updated_at_ms: Optional[int] = None,
        landing_page_size: int = 100,
    ) -> TrajectoryStatsUpdate:
        """Idempotently recompute complete parent sessions from all landing shards."""
        if landing_page_size <= 0:
            raise ValueError("landing_page_size must be positive")
        keys = sorted({_session_identity(key) for key in session_keys})
        if not keys:
            return TrajectoryStatsUpdate()
        timestamp = sdk_time.now_ms() if updated_at_ms is None else updated_at_ms
        existing_rows = self.store.list_session_rows()
        sessions_by_key = {
            (row["job_id"], row["session_id"]): row for row in existing_rows
        }
        computed_rows = self._compute_session_rows(
            keys,
            updated_at_ms=timestamp,
            landing_page_size=landing_page_size,
        )
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

    def _compute_session_rows(
        self,
        keys: list[SessionIdentity],
        *,
        updated_at_ms: int,
        landing_page_size: int,
    ) -> list[dict[str, object]]:
        accumulators = {key: SessionMetricsAccumulator(*key) for key in keys}
        self._accumulate_landing_rows(accumulators, landing_page_size)
        # Finalize only after every landing read and validation has succeeded.
        return [accumulators[key].finalize(updated_at_ms=updated_at_ms) for key in keys]

    def _accumulate_landing_rows(
        self,
        accumulators: dict[SessionIdentity, SessionMetricsAccumulator],
        landing_page_size: int,
    ) -> None:
        sessions_by_job: dict[str, set[str]] = {}
        for job_id, session_id in accumulators:
            sessions_by_job.setdefault(job_id, set()).add(session_id)

        checkout_latest = True
        for job_id, session_ids in sorted(sessions_by_job.items()):
            # Resolve once per parent job, then paginate each HASH shard separately.
            shards = sorted(set(self.client.resolve_sub_jobs(job_id)))
            session_literals = ", ".join(
                f"'{_escape_sql(session_id)}'" for session_id in sorted(session_ids)
            )
            base_query = (
                f"job_id = '{_escape_sql(job_id)}' "
                f"AND session_id IN ({session_literals}) AND id IS NOT NULL"
            )
            for sub_job_id in shards:
                cursor = None
                while True:
                    id_query = base_query
                    if cursor is not None:
                        id_query += f" AND id > '{_escape_sql(cursor)}'"
                    # Sorting a JSON-projected Lance query can materialize every
                    # matching payload before applying its limit. Sort only ids.
                    id_rows = self.client.query_data(
                        filter_query=id_query,
                        columns=["id"],
                        limit=landing_page_size,
                        partition=sub_job_id,
                        order_by="id",
                        ascending=True,
                        checkout_latest=checkout_latest,
                        table=self.client.config.tables.landing_table,
                    )
                    checkout_latest = False
                    if not id_rows:
                        break

                    page_ids = []
                    for row in id_rows:
                        row_id = row.get("id")
                        if not isinstance(row_id, str) or not row_id:
                            raise ValueError("landing pagination requires non-empty string ids")
                        if cursor is not None and row_id <= cursor:
                            raise ValueError(
                                f"landing ids are not strictly increasing: "
                                f"{row_id!r} after {cursor!r}"
                            )
                        cursor = row_id
                        page_ids.append(row_id)

                    id_literals = ", ".join(f"'{_escape_sql(row_id)}'" for row_id in page_ids)
                    rows = self.client.query_data(
                        filter_query=f"{base_query} AND id IN ({id_literals})",
                        columns=LANDING_METRIC_COLUMNS,
                        limit=landing_page_size,
                        partition=sub_job_id,
                        checkout_latest=False,
                        table=self.client.config.tables.landing_table,
                    )
                    rows.sort(key=lambda row: row.get("id") or "")
                    if [row.get("id") for row in rows] != page_ids:
                        raise ValueError("landing payload page does not match its id page")
                    for row in rows:
                        session_id = row.get("session_id")
                        if row.get("job_id") != job_id or session_id not in session_ids:
                            raise ValueError(
                                "landing query returned a row outside the requested sessions"
                            )
                        row_shard = row.get("sub_job_id")
                        if row_shard not in shards:
                            raise ValueError(
                                f"split job {job_id!r} returned landing row with "
                                "unknown sub_job_id"
                            )
                        if row_shard != sub_job_id:
                            raise ValueError(
                                f"landing row belongs to shard {row_shard!r}, "
                                f"not queried shard {sub_job_id!r}"
                            )
                        accumulators[(job_id, session_id)].add_row(row)
                    if len(page_ids) < landing_page_size:
                        break


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
