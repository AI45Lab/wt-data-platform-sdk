"""Copy real landing_test rows into an isolated, persistent ETL test fixture.

The source rows are read-only. Copies receive unique identity fields and an
explicit marker in meta_json so they can be inspected and cleaned manually
after the staged ETL mode tests are complete.
"""

import argparse
import json
from pathlib import Path
from uuid import uuid4

from wt_sdk import GatewayConfig, LandingRecord, TableConfig, WTGatewayClient


LANDING_TEST_TABLE = "v2_landing_test"
SERVING_TEST_TABLE = "serving_test"


def _decode_meta(value: object) -> dict[str, object]:
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {"original_meta_json": value}
        if isinstance(decoded, dict):
            return decoded
        return {"original_meta_json_value": decoded}
    return {}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a persistent 20-row v2_landing_test ETL fixture"
    )
    parser.add_argument("--source-job-id", default="gateway")
    parser.add_argument("--row-count", type=int, default=20)
    parser.add_argument("--trainable-count", type=int, default=10)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--manifest-dir", default="etl_reports/fixtures")
    parser.add_argument("--confirm-create", action="store_true")
    args = parser.parse_args()

    if args.row_count <= 0:
        raise SystemExit("--row-count must be positive")
    if not 0 <= args.trainable_count <= args.row_count:
        raise SystemExit("--trainable-count must be between 0 and --row-count")
    if not args.confirm_create:
        raise SystemExit("refusing to write v2_landing_test without --confirm-create")

    batch_id = args.batch_id or uuid4().hex[:12]
    copied_job_id = (
        "gateway-real-copy#wt-etl#kimi-k3#mode-test#20260805#codex#"
        f"{batch_id}_mock_test"
    )
    config = GatewayConfig(
        tables=TableConfig(
            profile="test",
            landing_table=LANDING_TEST_TABLE,
            serving_table=SERVING_TEST_TABLE,
        )
    )

    with WTGatewayClient(config=config) as client:
        source_rows = client.query_data(
            filter_query=(
                f"job_id = '{args.source_job_id}' "
                "AND messages IS NOT NULL AND response IS NOT NULL"
            ),
            limit=args.row_count,
            partition=args.source_job_id,
            checkout_latest=True,
            table=LANDING_TEST_TABLE,
            exclude_none=False,
            deserialize_json=False,
        )
        if len(source_rows) != args.row_count:
            raise RuntimeError(
                f"expected {args.row_count} source rows, found {len(source_rows)}"
            )

        copies: list[LandingRecord] = []
        source_to_copy: list[dict[str, object]] = []
        for index, source in enumerate(source_rows):
            source_id = str(source["id"])
            source_session_id = str(source["session_id"])
            copy_id = f"{source_id}_mock_test_{batch_id}"
            copy_session_id = f"{source_session_id}_mock_test_{batch_id}"
            meta = _decode_meta(source.get("meta_json"))
            meta["wt_etl_test_fixture"] = {
                "batch_id": batch_id,
                "source_id": source_id,
                "source_job_id": args.source_job_id,
                "persistent_until_manual_cleanup": True,
            }

            copied = dict(source)
            copied.update(
                {
                    "id": copy_id,
                    "job_id": copied_job_id,
                    "session_id": copy_session_id,
                    "env_id": (
                        f"{source['env_id']}_mock_test_{batch_id}"
                        if source.get("env_id")
                        else None
                    ),
                    "source_updated_at": None,
                    "serving_updated_at": None,
                    "is_trainable": index < args.trainable_count,
                    "chosen_trace": None,
                    "tags": None,
                    "meta_json": json.dumps(meta, ensure_ascii=False),
                }
            )
            copies.append(LandingRecord(**copied))
            source_to_copy.append(
                {
                    "source_id": source_id,
                    "source_source_updated_at": source.get("source_updated_at"),
                    "copy_id": copy_id,
                    "copy_session_id": copy_session_id,
                    "step_id": source.get("step_id"),
                    "is_trainable": index < args.trainable_count,
                }
            )

        client.ingest_landing_batch(copies)
        persisted = client.query_data(
            filter_query=f"job_id = '{copied_job_id}'",
            partition=copied_job_id,
            checkout_latest=True,
            table=LANDING_TEST_TABLE,
            exclude_none=False,
            deserialize_json=False,
        )
        if len(persisted) != args.row_count:
            raise RuntimeError(
                f"fixture verification expected {args.row_count} rows, found {len(persisted)}"
            )

    baseline_by_id = {
        str(row["id"]): int(row["source_updated_at"])
        for row in persisted
    }
    for item in source_to_copy:
        item["baseline_source_updated_at"] = baseline_by_id[str(item["copy_id"])]

    manifest = {
        "batch_id": batch_id,
        "source_job_id": args.source_job_id,
        "copied_job_id": copied_job_id,
        "landing_table": LANDING_TEST_TABLE,
        "serving_table": SERVING_TEST_TABLE,
        "row_count": args.row_count,
        "trainable_count": args.trainable_count,
        "rows": source_to_copy,
    }
    manifest_dir = Path(args.manifest_dir).expanduser()
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"{batch_id}.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({**manifest, "manifest_path": str(manifest_path.resolve())}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
