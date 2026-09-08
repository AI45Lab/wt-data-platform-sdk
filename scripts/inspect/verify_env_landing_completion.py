#!/usr/bin/env python3
"""Compare env completion state with landing session completion markers.

This command is read-only. By default it checks the three production jobs
selected during the 2026-09-07 investigation. Pass ``--job-id`` to reuse the
same validation for other jobs.

Examples:
  python scripts/inspect/verify_env_landing_completion.py

  python scripts/inspect/verify_env_landing_completion.py \
    --job-id job-a --job-id job-b --json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from wt_sdk import GatewayConfig, TableConfig, WTGatewayClient
from wt_sdk.env_config_client import EnvConfigManager


DEFAULT_JOB_IDS = (
    "cybergym#opencode#kimi-k3#find#20260904#cxq#level1#fortest",
    "test#vulhub#opencode#glm-5.3#exploit#20260904180649#lml",
    "test#vulhub#codex#glm-5.3#exploit#20260904180129#lml",
)


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _nonempty_string(value: object) -> str:
    return str(value or "").strip()


def _is_true(value: object) -> bool:
    """Treat the bool values returned by Arrow/Pandas as completion markers."""

    if value is True:
        return True
    if value is None:
        return False
    try:
        return bool(value == True)  # noqa: E712
    except (TypeError, ValueError):
        return False


def compare_job_rows(
    job_id: str,
    env_rows: Sequence[Mapping[str, object]],
    landing_rows: Sequence[Mapping[str, object]],
) -> dict[str, Any]:
    """Return a deterministic, JSON-serializable comparison for one job."""

    env_ids_in_order = [_nonempty_string(row.get("env_id")) for row in env_rows]
    env_ids = {env_id for env_id in env_ids_in_order if env_id}
    env_id_counts = Counter(env_id for env_id in env_ids_in_order if env_id)
    duplicate_env_ids = sorted(
        env_id for env_id, count in env_id_counts.items() if count > 1
    )

    rows_by_session: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    empty_landing_session_rows = 0
    for row in landing_rows:
        session_id = _nonempty_string(row.get("session_id"))
        if not session_id:
            empty_landing_session_rows += 1
            continue
        rows_by_session[session_id].append(row)

    landing_session_ids = set(rows_by_session)
    missing_in_landing = sorted(env_ids - landing_session_ids)
    extra_in_landing = sorted(landing_session_ids - env_ids)
    completion_marker_counts = {
        session_id: sum(
            _is_true(row.get("is_session_completed"))
            for row in rows_by_session[session_id]
        )
        for session_id in sorted(landing_session_ids)
    }
    sessions_without_completion_marker = sorted(
        session_id
        for session_id, count in completion_marker_counts.items()
        if count == 0
    )
    sessions_with_multiple_completion_markers = sorted(
        session_id
        for session_id, count in completion_marker_counts.items()
        if count > 1
    )

    env_rows_exist = bool(env_rows)
    all_env_finished = env_rows_exist and all(
        _is_true(row.get("finished")) for row in env_rows
    )
    empty_env_id_rows = sum(not env_id for env_id in env_ids_in_order)

    issues: list[str] = []
    if not env_rows_exist:
        issues.append("env_rows_missing")
    if env_rows_exist and not all_env_finished:
        issues.append("env_not_all_finished")
    if empty_env_id_rows:
        issues.append("empty_env_id")
    if duplicate_env_ids:
        issues.append("duplicate_env_id")
    if not landing_rows:
        issues.append("landing_rows_missing")
    if empty_landing_session_rows:
        issues.append("empty_landing_session_id")
    if missing_in_landing:
        issues.append("env_id_missing_in_landing_sessions")
    if extra_in_landing:
        issues.append("landing_session_missing_in_env")
    if sessions_without_completion_marker:
        issues.append("landing_session_without_completion_marker")
    if sessions_with_multiple_completion_markers:
        issues.append("landing_session_with_multiple_completion_markers")

    return {
        "job_id": job_id,
        "status": "PASS" if not issues else "FAIL",
        "env_rows": len(env_rows),
        "env_ids": len(env_ids),
        "all_env_finished": all_env_finished,
        "empty_env_id_rows": empty_env_id_rows,
        "duplicate_env_ids": duplicate_env_ids,
        "landing_rows": len(landing_rows),
        "landing_sessions": len(landing_session_ids),
        "empty_landing_session_rows": empty_landing_session_rows,
        "env_ids_missing_in_landing": missing_in_landing,
        "landing_sessions_missing_in_env": extra_in_landing,
        "sessions_without_completion_marker": sessions_without_completion_marker,
        "sessions_with_multiple_completion_markers": (
            sessions_with_multiple_completion_markers
        ),
        "issues": issues,
    }


def _read_env_rows(
    manager: EnvConfigManager,
    job_id: str,
) -> list[dict[str, object]]:
    frame = manager._filter_table(
        query=f"job_id = '{_escape_sql(job_id)}'",
        limit=None,
        columns=["job_id", "env_id", "finished"],
        checkout_latest=True,
        extra={"api": "verify_env_landing_completion"},
    )
    return frame.to_dict(orient="records")


def _read_landing_rows(
    client: WTGatewayClient,
    job_id: str,
) -> list[dict[str, object]]:
    return client.query_data(
        filter_query=f"job_id = '{_escape_sql(job_id)}'",
        partition=job_id,
        columns=["session_id", "is_session_completed"],
        table=client.config.tables.landing_table,
        checkout_latest=True,
        exclude_none=False,
        deserialize_json=False,
    )


def inspect_jobs(
    env_manager: EnvConfigManager,
    landing_client: WTGatewayClient,
    job_ids: Iterable[str],
) -> list[dict[str, Any]]:
    reports = []
    for job_id in job_ids:
        print(f"Checking job: {job_id}", file=sys.stderr, flush=True)
        try:
            env_rows = _read_env_rows(env_manager, job_id)
            landing_rows = _read_landing_rows(landing_client, job_id)
            reports.append(compare_job_rows(job_id, env_rows, landing_rows))
        except Exception as exc:
            reports.append(
                {
                    "job_id": job_id,
                    "status": "ERROR",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
            )
    return reports


def _print_job_report(report: Mapping[str, Any]) -> None:
    print("=" * 80)
    print(f"Job: {report['job_id']}")
    print(f"Status: {report['status']}")
    if report["status"] == "ERROR":
        print(f"Error: {report['error_type']}: {report['message']}")
        return

    print(
        "Env: "
        f"rows={report['env_rows']}, ids={report['env_ids']}, "
        f"all_finished={str(report['all_env_finished']).lower()}"
    )
    print(
        "Landing: "
        f"rows={report['landing_rows']}, sessions={report['landing_sessions']}"
    )
    print(
        "Comparison: "
        f"missing_in_landing={len(report['env_ids_missing_in_landing'])}, "
        f"extra_in_landing={len(report['landing_sessions_missing_in_env'])}, "
        f"without_completion_marker="
        f"{len(report['sessions_without_completion_marker'])}, "
        f"multiple_completion_markers="
        f"{len(report['sessions_with_multiple_completion_markers'])}"
    )
    if report["issues"]:
        print(f"Issues: {', '.join(report['issues'])}")
    if report["env_ids_missing_in_landing"]:
        print(
            "Env IDs missing in landing: "
            + ", ".join(report["env_ids_missing_in_landing"])
        )
    if report["landing_sessions_missing_in_env"]:
        print(
            "Landing sessions missing in env: "
            + ", ".join(report["landing_sessions_missing_in_env"])
        )
    if report["sessions_without_completion_marker"]:
        print(
            "Sessions without completion marker: "
            + ", ".join(report["sessions_without_completion_marker"])
        )
    if report["sessions_with_multiple_completion_markers"]:
        print(
            "Sessions with multiple completion markers: "
            + ", ".join(report["sessions_with_multiple_completion_markers"])
        )


def _deduplicate_job_ids(values: Iterable[str]) -> list[str]:
    deduplicated: list[str] = []
    seen: set[str] = set()
    for value in values:
        job_id = value.strip()
        if not job_id or job_id in seen:
            continue
        seen.add(job_id)
        deduplicated.append(job_id)
    return deduplicated


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only comparison of env finished state and landing session "
            "completion markers"
        )
    )
    parser.add_argument(
        "--profile",
        choices=("test", "prod", "production"),
        default="production",
        help=(
            "Select env/landing tables by profile. Defaults to production; "
            "this command is read-only."
        ),
    )
    parser.add_argument(
        "--job-id",
        action="append",
        default=None,
        help=(
            "Exact job ID to compare; repeat for multiple jobs. Defaults to "
            "the three production samples embedded in this script."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print one machine-readable JSON report instead of text output.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    job_ids = _deduplicate_job_ids(args.job_id or DEFAULT_JOB_IDS)
    if not job_ids:
        print("At least one non-empty --job-id is required", file=sys.stderr)
        return 2

    table_config = TableConfig(profile=args.profile)
    env_manager = EnvConfigManager(profile=args.profile)
    try:
        landing_client = WTGatewayClient(GatewayConfig(tables=table_config))
        try:
            reports = inspect_jobs(env_manager, landing_client, job_ids)
        finally:
            landing_client.close()
    finally:
        env_manager.close()

    passed = sum(report["status"] == "PASS" for report in reports)
    failed = sum(report["status"] == "FAIL" for report in reports)
    errors = sum(report["status"] == "ERROR" for report in reports)
    payload = {
        "read_only": True,
        "profile": table_config.profile,
        "env_table": env_manager.table_name,
        "landing_table": table_config.landing_table,
        "jobs_checked": len(reports),
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "status": "PASS" if failed == 0 and errors == 0 else "FAIL",
        "jobs": reports,
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print("Read only: true")
        print(f"Profile: {payload['profile']}")
        print(f"Environment table: {payload['env_table']}")
        print(f"Landing table: {payload['landing_table']}")
        for report in reports:
            _print_job_report(report)
        print("=" * 80)
        print(
            "Overall: "
            f"{payload['status']} "
            f"(passed={passed}, failed={failed}, errors={errors})"
        )

    if errors:
        return 2
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
