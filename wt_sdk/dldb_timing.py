import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


def _is_truthy(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def resolve_dldb_model(configured: Optional[str]) -> Optional[str]:
    raw = os.getenv("WT_SDK_DLDB_MODEL")
    if raw is not None:
        raw = raw.strip()
        return raw or None

    if configured is None:
        return None

    configured = configured.strip()
    return configured or None


def resolve_enable_dldb_timing_logs(configured: bool) -> bool:
    raw = os.getenv("WT_SDK_LOG_DLDB_TIMING")
    if raw is not None:
        return _is_truthy(raw)
    return bool(configured)


def resolve_dldb_metrics_log_path(configured: Optional[str]) -> Optional[str]:
    raw = os.getenv("WT_SDK_DLDB_METRICS_LOG")
    if raw is not None:
        raw = raw.strip()
        return raw or None

    if configured is None:
        return None

    configured = configured.strip()
    return configured or None


def extract_dldb_timing_from_df(df: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(df, pd.DataFrame):
        return None

    attrs = getattr(df, "attrs", None) or {}
    timing = attrs.get("dldb")
    return timing if isinstance(timing, dict) else None


def extract_dldb_last_call(session: Any) -> Optional[Dict[str, Any]]:
    timing = getattr(session, "last_call", None)
    return timing if isinstance(timing, dict) else None


def build_dldb_timing_payload(
    api_name: str,
    timing: Optional[Dict[str, Any]],
    *,
    table_name: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(timing, dict):
        return None

    payload: Dict[str, Any] = {
        "api": api_name or timing.get("api"),
        "elapsed_ms": timing.get("elapsed_ms"),
        "rows": timing.get("rows"),
        "rows_per_s": timing.get("rows_per_s"),
        "mb_per_s": timing.get("mb_per_s"),
        "table_name": table_name or timing.get("table_name"),
    }

    for key, value in timing.items():
        if key not in payload and value is not None:
            payload[key] = value

    if extra:
        for key, value in extra.items():
            if value is not None:
                payload[key] = value

    return {key: value for key, value in payload.items() if value is not None}


def format_dldb_timing_log(
    api_name: str,
    timing: Optional[Dict[str, Any]],
    *,
    table_name: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    payload = build_dldb_timing_payload(
        api_name,
        timing,
        table_name=table_name,
        extra=extra,
    )
    if not payload:
        return None

    parts = [
        f"{key}={value}"
        for key, value in payload.items()
        if value is not None
    ]
    if not parts:
        return None

    return "dldb_timing " + " ".join(parts)


def format_dldb_metrics_summary(summary: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(summary, dict):
        return None

    by_api = summary.get("by_api", {})
    api_names = ",".join(sorted(by_api.keys())) if isinstance(by_api, dict) else None

    payload = {
        "model": summary.get("model"),
        "total_calls": summary.get("total_calls"),
        "total_errors": summary.get("total_errors"),
        "total_latency_seconds_sum": summary.get("total_latency_seconds_sum"),
        "total_rows": summary.get("total_rows"),
        "total_bytes": summary.get("total_bytes"),
        "apis": api_names,
    }
    parts = [
        f"{key}={value}"
        for key, value in payload.items()
        if value is not None
    ]
    if not parts:
        return None

    return "dldb_metrics_summary " + " ".join(parts)


def append_dldb_metrics_log(
    log_path: Optional[str],
    event_type: str,
    payload: Optional[Dict[str, Any]],
) -> None:
    """Append a single JSONL metrics event. Logging failures are intentionally ignored."""
    if not log_path or not isinstance(payload, dict):
        return

    try:
        path = Path(log_path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        event = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event_type,
            "payload": payload,
        }
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, default=str))
            f.write("\n")
    except Exception:
        return
