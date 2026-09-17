"""Read-only job sampling and structural trainability diagnostics.

Run with ``python -m wt_sdk.etl.tools.trainability_sample --help``.
"""

from __future__ import annotations

import argparse
import json
import os
import random
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from ..policy import (
    TrainabilityPolicy,
    normalize_trainability_policy,
    trainability_policy_from_env_value,
)
from ..stage import SessionKey, StageContext
from ..stages.trainability import (
    UpdateIsTrainableStage,
    _detect_trainable_record_ids,
    _has_non_200_status_code,
    _is_eligible_trainability_record,
    _is_incomplete_stream_request,
    _record_id,
    _step_id,
)


REASONS = {
    "selected_chain_tail": "该链最终保留的链尾，且未被多链 session 的独立单步链规则排除。",
    "superseded_in_chain": "同链后续记录通过严格前缀延伸或回退替代了此记录。",
    "short_side_chain": "多链 session 中链长小于 20 的子链，被 stage 的短子链规则排除；不证明它是真实 subagent。",
    "non_200_status": "meta_json 顶层、env_state 或 telemetry 含显式非 200 状态码，先行排除。",
    "incomplete_stream_request": "流式请求缺少 finish_reason，先行排除。",
    "selected_max_eligible_step": "降级模式选择过滤非 200 后 step_id 最大的一条。",
    "not_max_eligible_step": "降级模式未选中此记录。",
}


def process_session(
    rows: list[dict[str, Any]],
    job_id: str,
    session_id: str,
    *,
    trainability_policy: TrainabilityPolicy = TrainabilityPolicy.NORMAL,
) -> dict[str, Any]:
    """Apply the real stage and attach explanations without modifying input rows."""
    stage = UpdateIsTrainableStage()
    context = StageContext(
        pipeline_name="trainability_sample",
        pipeline_version=stage.version,
        session_key=SessionKey(job_id, session_id),
        stage_name=stage.name,
        trainability_policy=trainability_policy,
    )
    ids = [_record_id(row) for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("session contains duplicate record IDs")
    if any(row.get("job_id") != job_id or row.get("session_id") != session_id for row in rows):
        raise ValueError("query returned rows outside the requested job/session")
    session = tuple(sorted(rows, key=_step_id))
    patches = stage.transform_session(session, context)
    if not patches:
        raise ValueError("sampled session no longer has a completion marker")

    downgrade = context.trainability_policy is TrainabilityPolicy.DOWNGRADE
    diagnostics: dict[str, dict[str, Any]] = {}
    eligible = tuple(
        row for row in session
        if _is_eligible_trainability_record(row)
    )
    if not downgrade:
        explained_ids = _detect_trainable_record_ids(eligible, diagnostics=diagnostics)
        actual_ids = {record_id for record_id, patch in patches.items() if patch["is_trainable"]}
        if explained_ids != actual_ids:
            raise AssertionError("structural diagnostics disagree with stage patches")

    completion = next(row for row in session if row.get("is_session_completed") is True)
    exported_rows = []
    for row in session:
        record_id = _record_id(row)
        patch = patches[record_id]
        if _has_non_200_status_code(row):
            evidence = {"reason_code": "non_200_status"}
        elif _is_incomplete_stream_request(row):
            evidence = {"reason_code": "incomplete_stream_request"}
        elif downgrade:
            evidence = {"reason_code": (
                "selected_max_eligible_step" if patch["is_trainable"] else "not_max_eligible_step"
            )}
        else:
            evidence = diagnostics[record_id]
        exported_rows.append({
            **row,
            **patch,
            "trainability_diagnostics": {
                **evidence,
                "reason": REASONS[evidence["reason_code"]],
                "stored_is_trainable": row.get("is_trainable"),
                "stored_reward": row.get("reward"),
                "stage_patch": patch,
                "is_max_session_step": row["step_id"] == session[-1]["step_id"],
                "reward_source_record_id": _record_id(completion) if "reward" in patch else None,
            },
        })
    return {
        "session_id": session_id,
        "status": "processed",
        "row_count": len(session),
        "eligible_row_count": len(eligible),
        "trainable_step_ids": [row["step_id"] for row in exported_rows if row["is_trainable"]],
        "is_trainable_true_count": sum(row["is_trainable"] for row in exported_rows if row["is_trainable"] is True),
        "warnings": [asdict(warning) for warning in context.emitted_warnings],
        "rows": exported_rows,
    }


def sample_job(
    client: Any,
    *,
    job_id: str,
    session_count: int,
    table: str,
    seed: int = 0,
    trainability_policy: TrainabilityPolicy = TrainabilityPolicy.NORMAL,
) -> dict[str, Any]:
    """Sample completed IDs, then query every row of each selected session."""
    if not job_id.strip() or not table.strip():
        raise ValueError("job_id and table must not be empty")
    if session_count <= 0:
        raise ValueError("session_count must be positive")
    effective_trainability_policy = normalize_trainability_policy(
        trainability_policy
    )
    job_filter = "job_id = '" + job_id.replace("'", "''") + "'"
    query_options = {
        "partition": job_id,
        "table": table,
        "checkout_latest": True,
        "exclude_none": False,
        "deserialize_json": False,
    }
    completed_rows = client.query_data(
        filter_query=job_filter + " AND is_session_completed = true",
        columns=["session_id"],
        **query_options,
    )
    candidates = sorted({
        row["session_id"] for row in completed_rows
        if isinstance(row.get("session_id"), str) and row["session_id"].strip()
    })
    if len(candidates) < session_count:
        raise ValueError(
            f"requested {session_count} completed sessions, found only {len(candidates)}"
        )
    selected = random.Random(seed).sample(candidates, session_count)
    sessions = []
    for index, session_id in enumerate(selected, start=1):
        rows: list[dict[str, Any]] = []
        try:
            session_filter = "session_id = '" + session_id.replace("'", "''") + "'"
            rows = client.query_data(
                filter_query=job_filter + " AND " + session_filter,
                **query_options,
            )
            result = process_session(
                rows,
                job_id,
                session_id,
                trainability_policy=effective_trainability_policy,
            )
        except Exception as exc:
            result = {
                "session_id": session_id,
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "record_id": getattr(exc, "record_id", None),
                "row_count": len(rows),
                "rows": rows,
            }
        if result["status"] == "failed":
            print(
                f"DEBUG session={session_id} rows={len(rows)} "
                f"error_type={result['error_type']} error={result['error']!r} "
                f"record_id={result['record_id']}",
                flush=True,
            )
        else:
            print(
                f"DEBUG session={session_id} rows={result['row_count']} "
                f"eligible={result['eligible_row_count']} "
                f"trainable={result['is_trainable_true_count']}",
                flush=True,
            )
        sessions.append(result)
        print(f"[{index}/{session_count}] {session_id}: {result['status']}", flush=True)

    processed = [session for session in sessions if session["status"] == "processed"]
    reason_counts = Counter(
        row["trainability_diagnostics"]["reason_code"]
        for session in processed for row in session["rows"]
    )
    return {
        "job_id": job_id,
        "source_table": table,
        "stage_name": UpdateIsTrainableStage.name,
        "stage_version": UpdateIsTrainableStage.version,
        "TRAINABILITY_DOWNGRADE_LABEL": (
            effective_trainability_policy is TrainabilityPolicy.DOWNGRADE
        ),
        "sampling": {
            "method": "seeded_random_without_replacement_from_sorted_completed_ids",
            "seed": seed,
            "available_sessions": len(candidates),
            "requested_sessions": session_count,
            "session_ids": selected,
        },
        "sessions_processed": len(processed),
        "sessions_failed": len(sessions) - len(processed),
        "rows_processed": sum(session["row_count"] for session in processed),
        "trainable_row_count": sum(len(session["trainable_step_ids"]) for session in processed),
        "multi_trainable_session_count": sum(
            len(session["trainable_step_ids"]) > 1 for session in processed
        ),
        "reason_counts": dict(reason_counts),
        "diagnostic_scope": (
            "仅根据状态码、规范化消息相等性、严格前缀关系和链位置解释。"
            "relation=strict_prefix_reset 表示前缀回退；不证明发生了业务重试。"
            "system_messages_equal=false 仅表示内容变化，不证明跨日、记忆压缩或 subagent。"
            "failed session 的 rows 是原始数据，未应用 stage 标注。"
        ),
        "sessions": sessions,
    }


def write_report(report: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--session-count", type=int, required=True)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--table", help="defaults to the SDK configured landing table")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.session_count <= 0 or not args.job_id.strip():
        parser.error("--session-count must be positive and --job-id must not be empty")

    trainability_policy = trainability_policy_from_env_value(
        os.getenv("TRAINABILITY_DOWNGRADE_LABEL")
    )

    from wt_sdk import WTGatewayClient

    with WTGatewayClient() as client:
        report = sample_job(
            client,
            job_id=args.job_id,
            session_count=args.session_count,
            seed=args.seed,
            table=args.table or client.config.tables.landing_table,
            trainability_policy=trainability_policy,
        )
    write_report(report, args.output)
    print(f"Exported {report['sessions_processed']} sessions to {args.output.resolve()}; "
          f"failed={report['sessions_failed']}")
    return 1 if report["sessions_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
