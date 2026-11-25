"""Pure category-level aggregation over distinct session metric rows."""

import statistics
from typing import Iterable, Mapping, Optional

from .schemas import (
    CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS,
    CLASSIFICATION_STATUS_VALID,
    METRIC_NAMES,
    METRIC_STATUS_VALID,
)


def aggregate_category_metrics(
    session_rows: Iterable[Mapping[str, object]],
    *,
    updated_at_ms: int,
) -> list[dict[str, object]]:
    """Aggregate each metric independently over distinct valid-classification sessions."""
    unique_sessions: dict[tuple[object, object], Mapping[str, object]] = {}
    for row in session_rows:
        job_id = row.get("job_id")
        session_id = row.get("session_id")
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("every session metric row requires job_id")
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("every session metric row requires session_id")
        unique_sessions[(job_id, session_id)] = row

    groups: dict[tuple[str, str, str, str, str], list[Mapping[str, object]]] = {}
    for row in unique_sessions.values():
        if row.get("classification_status") != CLASSIFICATION_STATUS_VALID:
            continue
        key = tuple(row.get(column) for column in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS[:-1])
        if not all(isinstance(value, str) and value for value in key):
            raise ValueError("valid-classification rows require all category dimensions")
        level = row.get("level") or ""
        if not isinstance(level, str):
            raise ValueError("category level must be a string")
        groups.setdefault((*key, level), []).append(row)

    return [
        _aggregate_group(key, rows, updated_at_ms=updated_at_ms)
        for key, rows in sorted(groups.items())
    ]


def _aggregate_group(
    key: tuple[str, str, str, str, str],
    rows: list[Mapping[str, object]],
    *,
    updated_at_ms: int,
) -> dict[str, object]:
    delivered_session_count = len(rows)
    result: dict[str, object] = dict(zip(CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS, key))
    result["delivered_session_count"] = delivered_session_count

    for metric in METRIC_NAMES:
        values = [
            int(row[metric])
            for row in rows
            if row.get(f"{metric}_status") == METRIC_STATUS_VALID
            and row.get(metric) is not None
        ]
        valid_count = len(values)
        missing_count = delivered_session_count - valid_count
        result.update(_metric_aggregates(metric, values))
        result[f"{metric}_valid_session_count"] = valid_count
        result[f"{metric}_missing_session_count"] = missing_count
        result[f"{metric}_coverage"] = (
            valid_count / delivered_session_count if delivered_session_count else 0.0
        )

    result["updated_at_ms"] = updated_at_ms
    return result


def _metric_aggregates(metric: str, values: list[int]) -> dict[str, Optional[float]]:
    if not values:
        return {
            f"{metric}_mean": None,
            f"{metric}_median": None,
            f"{metric}_max": None,
            f"{metric}_min": None,
        }
    return {
        f"{metric}_mean": float(sum(values) / len(values)),
        f"{metric}_median": float(statistics.median(values)),
        f"{metric}_max": max(values),
        f"{metric}_min": min(values),
    }
