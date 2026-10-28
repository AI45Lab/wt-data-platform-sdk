"""Read-only verification for a persistent ETL real-data fixture."""

import argparse
import json
from pathlib import Path

from wt_sdk import GatewayConfig, TableConfig, WTGatewayClient


LANDING_TEST_TABLE = "landing_test"
SERVING_TEST_TABLE = "serving_test"


def _decode(value: object) -> object:
    if not isinstance(value, str):
        return value
    return json.loads(value)


def _expected_trace(row: dict[str, object]) -> object:
    messages = _decode(row.get("messages"))
    response = _decode(row.get("response"))
    if not isinstance(messages, list):
        return None
    trace = list(messages)
    if isinstance(response, list):
        trace.extend(response)
    else:
        trace.append(response)
    return trace


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()

    manifest_path = Path(args.manifest).expanduser()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    job_id = str(manifest["copied_job_id"])
    batch_id = str(manifest["batch_id"])
    baseline = {
        str(row["copy_id"]): int(row["baseline_source_updated_at"])
        for row in manifest["rows"]
    }
    expected_trainable_ids = {
        str(row["copy_id"])
        for row in manifest["rows"]
        if row["is_trainable"] is True
    }

    config = GatewayConfig(
        tables=TableConfig(
            profile="test",
            landing_table=LANDING_TEST_TABLE,
            serving_table=SERVING_TEST_TABLE,
        )
    )
    with WTGatewayClient(config=config) as client:
        query = f"job_id = '{job_id}'"
        landing = client.query_data(
            filter_query=query,
            partition=job_id,
            checkout_latest=True,
            table=LANDING_TEST_TABLE,
            exclude_none=False,
        )
        try:
            serving = client.query_data(
                filter_query=query,
                partition=job_id,
                checkout_latest=True,
                table=SERVING_TEST_TABLE,
                exclude_none=False,
            )
        except ValueError as exc:
            if "partition" not in str(exc) or "does not exist" not in str(exc):
                raise
            serving = []

    landing_by_id = {str(row["id"]): row for row in landing}
    serving_by_id = {str(row["id"]): row for row in serving}
    changed_timestamps = []
    for record_id, baseline_timestamp in baseline.items():
        current = landing_by_id.get(record_id, {}).get("source_updated_at")
        if current != baseline_timestamp:
            changed_timestamps.append(
                {
                    "id": record_id,
                    "baseline": baseline_timestamp,
                    "current": current,
                }
            )

    marker_errors = []
    for record_id, row in landing_by_id.items():
        try:
            marker = _decode(row.get("meta_json"))["wt_etl_test_fixture"]
        except Exception as exc:
            marker_errors.append({"id": record_id, "error": str(exc)})
            continue
        if marker.get("batch_id") != batch_id:
            marker_errors.append({"id": record_id, "error": "batch_id mismatch"})

    chosen_trace_errors = []
    tags_errors = []
    source_timestamp_mismatches = []
    expected_tags = job_id.split("#")[:4]
    for record_id, serving_row in serving_by_id.items():
        landing_row = landing_by_id.get(record_id)
        if landing_row is None:
            chosen_trace_errors.append({"id": record_id, "error": "missing landing row"})
            continue
        try:
            if _decode(serving_row.get("chosen_trace")) != _expected_trace(landing_row):
                chosen_trace_errors.append({"id": record_id, "error": "trace mismatch"})
        except Exception as exc:
            chosen_trace_errors.append({"id": record_id, "error": str(exc)})
        actual_tags = serving_row.get("tags")
        if hasattr(actual_tags, "tolist"):
            actual_tags = actual_tags.tolist()
        if actual_tags != expected_tags:
            tags_errors.append(
                {
                    "id": record_id,
                    "expected": expected_tags,
                    "actual": actual_tags,
                }
            )
        if serving_row.get("source_updated_at") != landing_row.get("source_updated_at"):
            source_timestamp_mismatches.append(record_id)

    serving_ids = set(serving_by_id)
    payload = {
        "manifest": str(manifest_path.resolve()),
        "batch_id": batch_id,
        "job_id": job_id,
        "landing_rows": len(landing),
        "landing_trainable_rows": sum(row.get("is_trainable") is True for row in landing),
        "landing_non_trainable_rows": sum(
            row.get("is_trainable") is not True for row in landing
        ),
        "landing_source_updated_at_unchanged": len(baseline) - len(changed_timestamps),
        "landing_source_updated_at_changed": changed_timestamps,
        "landing_meta_marker_errors": marker_errors,
        "serving_rows": len(serving),
        "serving_missing_expected_trainable_ids": sorted(
            expected_trainable_ids - serving_ids
        ),
        "serving_unexpected_ids": sorted(serving_ids - expected_trainable_ids),
        "serving_chosen_trace_errors": chosen_trace_errors,
        "serving_tags_errors": tags_errors,
        "serving_source_timestamp_mismatches": source_timestamp_mismatches,
        "serving_updated_at_non_null": sum(
            row.get("serving_updated_at") is not None for row in serving
        ),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
