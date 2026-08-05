"""Run one or more contributed ETL pipelines serially.

Examples:
  python scripts/etl/run.py \
    --profile test \
    --pipeline-factory my_package.etl:build_landing_pipeline \
    --pipeline-factory my_package.etl:build_serving_pipeline \
    --start-from 2026-08-01T00:00:00Z

  python scripts/etl/run.py \
    --profile test \
    --pipeline-factory my_package.etl:build_serving_pipeline \
    --job-id 'dataset#harness#model#task#date#owner#extra'
"""

import argparse
import importlib
import json
from dataclasses import asdict
from datetime import datetime, timezone

import wt_sdk._time as sdk_time
from wt_sdk import GatewayConfig, TableConfig, WTGatewayClient
from wt_sdk.config import DEFAULT_LANDING_TABLE, DEFAULT_SERVING_TABLE
from wt_sdk.etl import (
    DldbCheckpointStore,
    ETLEngine,
    PipelineDefinition,
    PipelineMode,
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


def _summary_payload(summary) -> dict:
    payload = asdict(summary)
    payload["mode"] = summary.mode.value
    payload["dirty_sessions"] = [
        {"job_id": key.job_id, "session_id": key.session_id}
        for key in sorted(summary.dirty_sessions)
    ]
    return payload


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
    parser.add_argument("--job-id", default=None)
    parser.add_argument("--session-id", default=None)
    parser.add_argument("--force-unsettled", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-production", action="store_true")
    parser.add_argument("--state-db-uri", default=None)
    parser.add_argument("--checkpoint-table", default="wt_etl_checkpoints")
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
    if args.end_time and not args.start_time:
        raise SystemExit("--end-time requires --start-time")
    if args.job_id and args.start_time:
        raise SystemExit("job/session selection cannot be combined with a time range")
    if args.start_from and (args.job_id or args.start_time):
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
    try:
        is_incremental = not args.job_id and not args.start_time
        if is_incremental:
            checkpoint_store = DldbCheckpointStore(
                resolve_etl_state_db_uri(args.state_db_uri),
                table_name=args.checkpoint_table,
            )
            checkpoint_store.verify_ready()

        engine = ETLEngine(client, checkpoint_store=checkpoint_store)
        dirty_sessions: set[SessionKey] = set()
        for pipeline in pipelines:
            if args.job_id and args.session_id:
                summary = engine.run_sessions(
                    pipeline,
                    [SessionKey(args.job_id, args.session_id)],
                    dry_run=args.dry_run,
                )
            elif args.job_id:
                summary = engine.run_job(pipeline, args.job_id, dry_run=args.dry_run)
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
                        0 if args.force_unsettled else args.settle_delay_seconds * 1000
                    ),
                    page_size=args.page_size,
                    start_from_ms=(
                        _parse_time(args.start_from) if args.start_from else None
                    ),
                    dry_run=args.dry_run,
                )

            if pipeline.mode is PipelineMode.SERVING and dirty_sessions:
                immediate = engine.run_sessions(
                    pipeline,
                    dirty_sessions,
                    dry_run=args.dry_run,
                )
                summary.merge(immediate)
            if pipeline.mode is PipelineMode.LANDING:
                dirty_sessions.update(summary.dirty_sessions)
            outputs.append(_summary_payload(summary))
    finally:
        try:
            if checkpoint_store is not None:
                checkpoint_store.close()
        finally:
            client.close()

    print(json.dumps(outputs, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
