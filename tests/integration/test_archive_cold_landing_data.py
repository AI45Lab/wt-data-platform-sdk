"""Real dldb/S3 validation for the online cold landing archive workflow.

The test is scoped to unique rows in ``landing_test``/``serving_test`` and a
unique temporary archive table. Cleanup runs even when the archive assertion
fails. Production tables are never selected.
"""

import json
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

import dldb

import scripts.ops.archive_cold_landing_data as archive
from wt_sdk import (
    GatewayConfig,
    LandingRecord,
    TableConfig,
    WTGatewayClient,
)


TEST_CONFIG = GatewayConfig(
    tables=TableConfig(
        profile="test",
        landing_table="landing_test",
        serving_table="serving_test",
    )
)


def _record(
    *,
    record_id: str,
    job_id: str,
    session_id: str,
    source_updated_at: int,
    is_trainable: bool,
) -> LandingRecord:
    return LandingRecord(
        dataset_type="ARCHIVE_INTEGRATION_TEST",
        id=record_id,
        session_id=session_id,
        created_at=source_updated_at // 1000,
        source_updated_at=source_updated_at,
        job_id=job_id,
        step_id=0,
        is_terminal=True,
        is_session_completed=True,
        is_trainable=is_trainable,
        messages=json.dumps([{"role": "user", "content": "archive me"}]),
        response=json.dumps({"role": "assistant", "content": "archived"}),
        meta_json=json.dumps({"integration_test": True, "nullable": None}),
        tags=["archive-integration"],
    )


def _drop_temporary_archive(table_name: str) -> None:
    session = dldb.connect(
        TEST_CONFIG.tables.db_uri,
        storage_options=TEST_CONFIG.s3.to_storage_options(),
    )
    try:
        if session.table_exists(table_name):
            session.drop_table(table_name)
        assert not session.table_exists(table_name)
    finally:
        session.shutdown()


def _unique_job_in_existing_landing_bucket() -> tuple[str, int, str]:
    session = dldb.connect(
        TEST_CONFIG.tables.db_uri,
        storage_options=TEST_CONFIG.s3.to_storage_options(),
    )
    try:
        landing_buckets = set(archive.list_partitions(session, "landing_test"))
    finally:
        session.shutdown()

    if not landing_buckets:
        raise RuntimeError("landing_test has no existing HASH bucket")
    target_bucket = sorted(landing_buckets)[0]
    for _ in range(1_000):
        suffix = uuid.uuid4().hex
        job_id = (
            "archive-test#pytest#sdk#cold-landing#20260801#codex#" + suffix
        )
        if archive.stable_hash(job_id) % archive.LANDING_PARTITIONS == target_bucket:
            return job_id, target_bucket, suffix
    raise RuntimeError("failed to generate a test job_id in an existing bucket")


def test_archive_cold_landing_data_real_copy_delete_and_cleanup():
    job_id, target_bucket, suffix = _unique_job_in_existing_landing_bucket()
    source_updated_at = int(
        datetime(2026, 8, 1, 12, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()
        * 1000
    )
    archive_table = f"archived_integration_{suffix}_landing_test"
    record_ids = [f"archive-integration-{suffix}-{index}" for index in range(3)]
    landing_records = [
        _record(
            record_id=record_id,
            job_id=job_id,
            session_id=f"session-{suffix}-{index}",
            source_updated_at=source_updated_at + index,
            is_trainable=False,
        )
        for index, record_id in enumerate(record_ids)
    ]
    filter_query = archive.exact_job_filter(job_id)

    _drop_temporary_archive(archive_table)
    with WTGatewayClient(config=TEST_CONFIG) as client:
        try:
            client.ingest_landing_batch(landing_records)

            result = archive.run_archive(
                cutoff_date_value="2026-08-01",
                batch_size=2,
                execute=True,
                confirm_delete=True,
                max_jobs=None,
                db_uri=TEST_CONFIG.tables.db_uri,
                profile="test",
                job_ids=[job_id],
                archive_table_override=archive_table,
            )
            assert result["profile"] == "test"
            assert result["source_table"] == "landing_test"
            assert result["serving_table"] == "serving_test"
            assert result["archive_table"] == archive_table
            assert result["archived_jobs"] == [
                {
                    "job_id": job_id,
                    "bucket": target_bucket,
                    "rows": 3,
                    "new_rows_copied": 3,
                }
            ]

            source_rows = client.query_data(
                filter_query=filter_query,
                table="landing_test",
                checkout_latest=True,
            )
            assert source_rows == []

            session = dldb.connect(
                TEST_CONFIG.tables.db_uri,
                storage_options=TEST_CONFIG.s3.to_storage_options(),
            )
            try:
                archive.verify_landing_layout(session, archive_table)
                archived = archive.query_partition(
                    session,
                    table_name=archive_table,
                    partition=target_bucket,
                    query=filter_query,
                    columns=archive.LANDING_SCHEMA.names,
                )
                assert set(archived["id"].tolist()) == set(record_ids)
                by_id = {row["id"]: row for row in archived.to_dict("records")}
                assert json.loads(by_id[record_ids[0]]["messages"]) == json.loads(
                    landing_records[0].messages
                )
                assert json.loads(by_id[record_ids[0]]["meta_json"]) == json.loads(
                    landing_records[0].meta_json
                )
            finally:
                session.shutdown()
        finally:
            try:
                client.delete_landing(filter_query)
            finally:
                client.delete_serving(filter_query)
            time.sleep(1)
            _drop_temporary_archive(archive_table)
