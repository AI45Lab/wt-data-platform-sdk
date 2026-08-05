"""Run one or more contributed ETL pipelines serially.

Examples:
  python scripts/etl/run.py \
    --profile test \
    --pipeline-factory wt_sdk.etl.pipelines:build_landing_pipeline \
    --pipeline-factory wt_sdk.etl.pipelines:build_serving_pipeline \
    --start-from 2026-08-01T00:00:00Z

  python scripts/etl/run.py \
    --profile test \
    --pipeline-factory wt_sdk.etl.pipelines:build_serving_pipeline \
    --job-id 'dataset#harness#model#task#date#owner#extra'
"""

import argparse
import importlib
import json
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import wt_sdk._time as sdk_time
from wt_sdk import GatewayConfig, TableConfig, WTGatewayClient
from wt_sdk.config import DEFAULT_LANDING_TABLE, DEFAULT_SERVING_TABLE
from wt_sdk.etl import (
    DldbCheckpointStore,
    ETLEngine,
    ETLRunFailed,
    PipelineDefinition,
    PipelineMode,
    RecordFailure,
    RunSummary,
    SessionKey,
    resolve_etl_state_db_uri,
)


def _load_pipeline(reference: str) -> PipelineDefinition:
    if ":" not in reference:
        raise ValueError("pipeline factory must use module:callable syntax")
    module_name, attribute_name = reference.split(":", 1)
    factory = getattr(importlib.import_module(module_name), attribute_name)
    if not callable(factory):
        raise TypeError(f"pipeline factory is not callable: {reference}")
    pipeline = factory()
    if not isinstance(pipeline, PipelineDefinition):
        raise TypeError(f"pipeline factory did not return PipelineDefinition: {reference}")
    return pipeline


def _parse_time(value: str) -> int:
    stripped = value.strip()
    if stripped.isdigit():
        numeric = int(stripped)
        return numeric if numeric >= 10_000_000_000 else numeric * 1000
    normalized = stripped.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _summary_payload(
    summary,
    *,
    pipeline_run_id: str,
    started_at_ms: int,
    ended_at_ms: int,
) -> dict:
    payload = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": summary.pipeline_name,
        "pipeline_version": summary.pipeline_version,
        "mode": summary.mode.value,
        "status": summary.status,
        "started_at": _iso_time(started_at_ms),
        "ended_at": _iso_time(ended_at_ms),
        "started_at_ms": started_at_ms,
        "ended_at_ms": ended_at_ms,
        "duration_ms": max(0, ended_at_ms - started_at_ms),
        "buckets_scanned": summary.buckets_scanned,
        "discovery_rows": summary.discovery_rows,
        "sessions_processed": summary.sessions_processed,
        "sessions_failed": summary.sessions_failed,
        "source_rows": summary.source_rows,
        "selected_rows": summary.selected_rows,
        "successful_rows": summary.successful_rows,
        "failed_rows": summary.failed_rows,
        "landing_rows_updated": summary.landing_rows_updated,
        "serving_rows_upserted": summary.serving_rows_upserted,
        "failures": [asdict(failure) for failure in summary.failures],
        "dirty_sessions": [
            {"job_id": key.job_id, "session_id": key.session_id}
            for key in sorted(summary.dirty_sessions)
        ],
    }
    payload["failed_row_ids"] = sorted(
        {
            failure.record_id
            for failure in summary.failures
            if failure.record_id is not None
        }
    )
    payload["audit"] = {
        "discovery_rows_read": summary.discovery_rows,
        "source_rows_read": summary.source_rows,
        "rows_selected": summary.selected_rows,
        "rows_succeeded": summary.successful_rows,
        "rows_failed": summary.failed_rows,
        "landing_rows_updated": summary.landing_rows_updated,
        "serving_rows_upserted": summary.serving_rows_upserted,
    }
    return payload


def _iso_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _new_pipeline_run_id(pipeline: PipelineDefinition, started_at_ms: int) -> str:
    timestamp = datetime.fromtimestamp(
        started_at_ms / 1000, tz=timezone.utc
    ).strftime("%Y%m%dT%H%M%S.%fZ")
    identity = _safe_filename_component(f"{pipeline.name}__v{pipeline.version}")
    return f"{identity}__{timestamp}__{uuid4().hex[:12]}"


def _safe_filename_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return normalized.strip("._-") or "etl"


def _write_report(payload: dict, report_dir: str) -> Path:
    directory = Path(report_dir).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{payload['pipeline_run_id']}.json"
    payload["report_path"] = str(path.resolve())
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _validate_pipeline_order(pipelines: list[PipelineDefinition]) -> None:
    seen_serving = False
    identities = set()
    for pipeline in pipelines:
        identity = (pipeline.name, pipeline.version, pipeline.mode)
        if identity in identities:
            raise ValueError(f"duplicate pipeline in one run: {identity}")
        identities.add(identity)
        if pipeline.mode is PipelineMode.SERVING:
            seen_serving = True
        elif seen_serving:
            raise ValueError("landing pipelines must run before serving pipelines in v1")


def _inspection_payload(
    pipelines: list[PipelineDefinition],
    *,
    include_stage_details: bool,
) -> dict:
    if include_stage_details:
        pipeline_payloads = [pipeline.describe_dag() for pipeline in pipelines]
    else:
        pipeline_payloads = [
            {
                "pipeline_name": pipeline.name,
                "pipeline_version": pipeline.version,
                "mode": pipeline.mode.value,
                "stage_count": len(pipeline.ordered_stages),
                "execution_order": [stage.name for stage in pipeline.ordered_stages],
            }
            for pipeline in pipelines
        ]
    return {
        "valid": True,
        "pipeline_count": len(pipelines),
        "pipelines": pipeline_payloads,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run WT landing/serving ETL pipelines")
    parser.add_argument(
        "--pipeline-factory",
        action="append",
        required=True,
        help="Repeatable module:callable returning PipelineDefinition; order is execution order.",
    )
    parser.add_argument(
        "--profile",
        choices=["production", "test"],
        default=None,
        help=(
            "Required for ETL execution; not needed for --list-stages/--validate-only. "
            "Production writes need --confirm-production."
        ),
    )
    parser.add_argument(
        "--list-stages",
        action="store_true",
        help="Print stage metadata, execution order, and dependency edges; do not access DB.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate pipeline/stage DAG configuration; do not access DB or execute stages.",
    )
    parser.add_argument("--landing-table", default=None)
    parser.add_argument("--serving-table", default=None)
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Number of lightweight discovery rows per page.",
    )
    parser.add_argument("--settle-delay-seconds", type=int, default=7200)
    parser.add_argument(
        "--start-from",
        default=None,
        help="First incremental watermark (ISO or epoch).",
    )
    parser.add_argument("--start-time", default=None, help="Manual inclusive range start.")
    parser.add_argument("--end-time", default=None, help="Manual inclusive range end.")
    parser.add_argument("--job-id", action="append", default=None)
    parser.add_argument("--session-id", action="append", default=None)
    parser.add_argument(
        "--source-filter",
        default=None,
        help="Advanced manual dldb WHERE expression; scans all landing HASH buckets.",
    )
    parser.add_argument("--force-unsettled", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-production", action="store_true")
    parser.add_argument("--state-db-uri", default=None)
    parser.add_argument("--checkpoint-table", default="wt_etl_checkpoints")
    parser.add_argument("--report-dir", default="etl_reports")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    try:
        pipelines = [_load_pipeline(reference) for reference in args.pipeline_factory]
        _validate_pipeline_order(pipelines)
    except Exception as exc:
        raise SystemExit(f"pipeline validation failed: {exc}") from exc

    if args.list_stages or args.validate_only:
        payload = _inspection_payload(
            pipelines,
            include_stage_details=args.list_stages,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    if args.profile is None:
        raise SystemExit(
            "--profile is required for ETL execution; it is optional only with "
            "--list-stages or --validate-only"
        )
    if args.session_id and not args.job_id:
        raise SystemExit("--session-id requires --job-id")
    if args.session_id and len(args.job_id) != 1:
        raise SystemExit("--session-id requires exactly one --job-id")
    if args.end_time and not args.start_time:
        raise SystemExit("--end-time requires --start-time")
    if args.source_filter is not None and not args.source_filter.strip():
        raise SystemExit("--source-filter must be a non-empty WHERE expression")
    manual_modes = sum(
        value is not None for value in (args.job_id, args.start_time, args.source_filter)
    )
    if manual_modes > 1:
        raise SystemExit(
            "--job-id, --start-time, and --source-filter are mutually exclusive"
        )
    if args.start_from and manual_modes:
        raise SystemExit("--start-from is only valid for default incremental mode")
    if args.settle_delay_seconds < 0:
        raise SystemExit("--settle-delay-seconds must be non-negative")

    table_config = TableConfig(
        profile=args.profile,
        landing_table=args.landing_table,
        serving_table=args.serving_table,
    )
    targets_production = (
        args.profile == "production"
        or table_config.landing_table == DEFAULT_LANDING_TABLE
        or table_config.serving_table == DEFAULT_SERVING_TABLE
    )
    if targets_production and not args.dry_run and not args.confirm_production:
        raise SystemExit(
            "refusing production ETL writes without --confirm-production; "
            "run --dry-run first"
        )
    client = WTGatewayClient(GatewayConfig(tables=table_config))
    checkpoint_store = None
    outputs = []
    exit_code = 0
    try:
        is_incremental = not args.job_id and not args.start_time and not args.source_filter
        if is_incremental:
            checkpoint_store = DldbCheckpointStore(
                resolve_etl_state_db_uri(args.state_db_uri),
                table_name=args.checkpoint_table,
            )
            checkpoint_store.verify_ready()

        engine = ETLEngine(client, checkpoint_store=checkpoint_store)
        dirty_sessions: set[SessionKey] = set()
        for pipeline in pipelines:
            started_at_ms = sdk_time.now_ms()
            pipeline_run_id = _new_pipeline_run_id(pipeline, started_at_ms)
            unexpected_failure = False
            try:
                if args.job_id and args.session_id:
                    summary = engine.run_sessions(
                        pipeline,
                        [SessionKey(args.job_id[0], session_id) for session_id in args.session_id],
                        dry_run=args.dry_run,
                    )
                elif args.job_id:
                    summary = engine.run_jobs(
                        pipeline,
                        args.job_id,
                        dry_run=args.dry_run,
                    )
                elif args.source_filter:
                    summary = engine.run_filter(
                        pipeline,
                        args.source_filter,
                        page_size=args.page_size,
                        dry_run=args.dry_run,
                    )
                elif args.start_time:
                    end_ms = (
                        _parse_time(args.end_time)
                        if args.end_time
                        else sdk_time.now_ms()
                        - (0 if args.force_unsettled else args.settle_delay_seconds * 1000)
                    )
                    summary = engine.run_range(
                        pipeline,
                        start_ms=_parse_time(args.start_time),
                        end_ms=end_ms,
                        page_size=args.page_size,
                        dry_run=args.dry_run,
                    )
                else:
                    summary = engine.run_incremental(
                        pipeline,
                        settle_delay_ms=(
                            0
                            if args.force_unsettled
                            else args.settle_delay_seconds * 1000
                        ),
                        page_size=args.page_size,
                        start_from_ms=(
                            _parse_time(args.start_from) if args.start_from else None
                        ),
                        dry_run=args.dry_run,
                        run_id=pipeline_run_id,
                    )
            except ETLRunFailed as exc:
                summary = exc.summary
                exit_code = 1
            except Exception as exc:
                summary = RunSummary(
                    pipeline_name=pipeline.name,
                    pipeline_version=pipeline.version,
                    mode=pipeline.mode,
                )
                summary.add_failure(
                    RecordFailure(
                        record_id=None,
                        job_id="",
                        session_id="",
                        stage_name="__pipeline__",
                        error_type=type(exc).__name__,
                        message=str(exc),
                    )
                )
                exit_code = 1
                unexpected_failure = True

            if (
                not unexpected_failure
                and pipeline.mode is PipelineMode.SERVING
                and dirty_sessions
            ):
                try:
                    immediate = engine.run_sessions(
                        pipeline,
                        dirty_sessions,
                        dry_run=args.dry_run,
                    )
                except ETLRunFailed as exc:
                    immediate = exc.summary
                    exit_code = 1
                summary.merge(immediate)
            if pipeline.mode is PipelineMode.LANDING:
                dirty_sessions.update(summary.dirty_sessions)
            ended_at_ms = sdk_time.now_ms()
            payload = _summary_payload(
                summary,
                pipeline_run_id=pipeline_run_id,
                started_at_ms=started_at_ms,
                ended_at_ms=ended_at_ms,
            )
            _write_report(payload, args.report_dir)
            outputs.append(payload)
            if unexpected_failure:
                break
    finally:
        try:
            if checkpoint_store is not None:
                checkpoint_store.close()
        finally:
            client.close()

    print(json.dumps(outputs, ensure_ascii=False, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
