"""Real DLDB/S3 coverage for unified read APIs and reliable serving export.

Every operation is pinned to ``landing_test`` or ``serving_test``.  Each test
uses a unique ``job_id`` and verifies cleanup in ``finally`` before exiting.
"""

import json
import time
import uuid
from typing import List, Type

from wt_sdk import (
    ChatMessage,
    ContentItem,
    GatewayConfig,
    LandingRecord,
    ServingRecord,
    TableConfig,
    WTGatewayClient,
)


LANDING_TEST_TABLE = "landing_test"
SERVING_TEST_TABLE = "serving_test"
TEST_TABLE_CONFIG = GatewayConfig(
    tables=TableConfig(
        landing_table=LANDING_TEST_TABLE,
        serving_table=SERVING_TEST_TABLE,
    )
)


def _message(role: str, text: str) -> ChatMessage:
    return ChatMessage(
        role=role,
        content=[ContentItem(type="text", text=text)],
    )


def _make_records(
    record_type: Type[LandingRecord],
    *,
    prefix: str,
    job_id: str,
    dataset_type: str,
    tag: str,
    search_text: str,
    count: int,
) -> List[LandingRecord]:
    base_created_at = int(time.time())
    records = []
    for index in range(count):
        question = _message("user", f"question {index}")
        answer = _message("assistant", f"answer {index}")
        rejected = _message("assistant", f"rejected {index}")
        records.append(
            record_type(
                dataset_type=dataset_type,
                id=f"{prefix}_{uuid.uuid4().hex}_{index:03d}",
                session_id=f"session_{job_id}",
                created_at=base_created_at + index,
                step_id=index,
                is_terminal=index == count - 1,
                env_id=f"env_{job_id}",
                job_id=job_id,
                is_truncated=False,
                step_reward=float(index),
                reward=float(index),
                messages=[question],
                response=answer,
                chosen_trace=[question, answer],
                rejected_trace=[question, rejected],
                search_text=f"{search_text} item {index}",
                agent_model="integration-test-model",
                env_name="integration-test-env",
                is_session_completed=index == count - 1,
                is_trainable=True,
                meta_json=json.dumps(
                    {
                        "source": "integration-test",
                        "job_id": job_id,
                        "index": index,
                    }
                ),
                tags=[tag, f"item-{index}"],
            )
        )
    return records


def _cleanup_and_verify(
    client: WTGatewayClient,
    job_id: str,
    tables: tuple[str, ...] = (LANDING_TEST_TABLE, SERVING_TEST_TABLE),
) -> None:
    """Delete this test's rows from every table it wrote and prove none remain."""
    filter_query = f"job_id = '{job_id}'"
    cleanup_errors = []
    delete_by_table = {
        LANDING_TEST_TABLE: client.delete_landing,
        SERVING_TEST_TABLE: client.delete_serving,
    }

    for table_name in tables:
        try:
            delete_by_table[table_name](filter_query)
        except Exception as exc:  # Continue so the other test table is still cleaned.
            cleanup_errors.append(f"{table_name}: {exc}")

    if cleanup_errors:
        raise AssertionError("Integration cleanup failed: " + "; ".join(cleanup_errors))

    time.sleep(1)
    for table_name in tables:
        remaining = client.query_data(
            filter_query=filter_query,
            table=table_name,
            checkout_latest=True,
        )
        assert remaining == [], f"{table_name} cleanup left {len(remaining)} rows"


def test_unified_read_interfaces_on_landing_and_serving_test_tables():
    suffix = uuid.uuid4().hex
    job_id = f"unified_read_job_{suffix}"
    dataset_type = f"UNIFIED_READ_{suffix}"
    tag = f"unified-read-tag-{suffix}"
    search_text = f"unique dashboard phrase {suffix}"
    filter_query = f"job_id = '{job_id}'"

    landing_records = _make_records(
        LandingRecord,
        prefix="landing_read",
        job_id=job_id,
        dataset_type=dataset_type,
        tag=tag,
        search_text=search_text,
        count=3,
    )
    serving_records = _make_records(
        ServingRecord,
        prefix="serving_read",
        job_id=job_id,
        dataset_type=dataset_type,
        tag=tag,
        search_text=search_text,
        count=3,
    )

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.landing_table == LANDING_TEST_TABLE
        assert client.config.tables.serving_table == SERVING_TEST_TABLE
        try:
            client.ingest_landing_batch(landing_records)
            client.ingest_serving(serving_records[0])
            client.ingest_serving_batch(serving_records[1:])
            time.sleep(1)

            assert client.count_landing(partition=job_id) == 3
            assert client.count_serving(partition=job_id) == 3

            landing_result = client.query_data(
                filter_query=filter_query,
                order_by="created_at",
                table=LANDING_TEST_TABLE,
            )
            serving_result = client.query_data(
                filter_query=filter_query,
                order_by="created_at",
                table=SERVING_TEST_TABLE,
            )
            assert [record["id"] for record in landing_result] == [
                record.id for record in landing_records
            ]
            assert [record["id"] for record in serving_result] == [
                record.id for record in serving_records
            ]
            assert all(len(record.get("chosen_trace", [])) == 2 for record in serving_result)
            assert all(len(record.get("rejected_trace", [])) == 2 for record in serving_result)
            assert all(tag in record.get("tags", []) for record in serving_result)
            assert [json.loads(record["meta_json"])["index"] for record in serving_result] == [
                0,
                1,
                2,
            ]

            assert "ground_truth_answer" not in landing_result[0]
            assert "image_url" not in landing_result[0]["messages"][0]["content"][0]
            landing_with_nulls = client.query_data(
                filter_query=filter_query,
                limit=1,
                table=LANDING_TEST_TABLE,
                exclude_none=False,
            )
            assert landing_with_nulls[0]["ground_truth_answer"] is None
            assert landing_with_nulls[0]["messages"][0]["content"][0]["image_url"] is None

            first_landing_page = client.pull_data(
                dataset_type=dataset_type,
                where_sql=filter_query,
                limit=2,
            )
            assert first_landing_page["id"].tolist() == [
                landing_records[0].id,
                landing_records[1].id,
            ]
            cursor = client.extract_cursor(first_landing_page)
            assert cursor == landing_records[1].created_at
            second_landing_page = client.pull_data(
                dataset_type=dataset_type,
                where_sql=filter_query,
                cursor=cursor,
                limit=2,
            )
            assert second_landing_page["id"].tolist() == [landing_records[2].id]

            serving_page = client.pull_data(
                dataset_type=dataset_type,
                where_sql=filter_query,
                table=SERVING_TEST_TABLE,
                limit=3,
            )
            assert serving_page["id"].tolist() == [record.id for record in serving_records]

            landing_batches = list(
                client.iter_data_batches(
                    dataset_type=dataset_type,
                    where_sql=filter_query,
                    chunk_size=2,
                )
            )
            serving_batches = list(
                client.iter_data_batches(
                    dataset_type=dataset_type,
                    where_sql=filter_query,
                    chunk_size=2,
                    table=SERVING_TEST_TABLE,
                )
            )
            assert [len(batch) for batch in landing_batches] == [2, 1]
            assert [len(batch) for batch in serving_batches] == [2, 1]
            assert [
                record_id
                for batch in serving_batches
                for record_id in batch["id"].tolist()
            ] == [record.id for record in serving_records]

            default_serving_by_id = client.get_by_id(serving_records[1].id)
            named_landing_by_id = client.get_by_id(
                landing_records[1].id,
                table=LANDING_TEST_TABLE,
            )
            assert default_serving_by_id and default_serving_by_id["id"] == serving_records[1].id
            assert named_landing_by_id and named_landing_by_id["id"] == landing_records[1].id

            search_result = client.search(
                f"dashboard phrase {suffix}",
                tags=[tag],
                where_sql=filter_query,
                dataset_type=dataset_type,
                limit=10,
            )
            assert set(search_result["id"].tolist()) == {
                record.id for record in serving_records
            }

            tag_counts = client.get_tags_distribution()
            assert tag_counts[tag] == 3
        finally:
            _cleanup_and_verify(client, job_id)


def test_export_data_batches_from_serving_test_and_cleanup():
    suffix = uuid.uuid4().hex
    job_id = f"export_job_{suffix}"
    dataset_type = f"EXPORT_{suffix}"
    tag = f"export-tag-{suffix}"
    filter_query = f"job_id = '{job_id}'"
    records = _make_records(
        ServingRecord,
        prefix="serving_export",
        job_id=job_id,
        dataset_type=dataset_type,
        tag=tag,
        search_text=f"export phrase {suffix}",
        count=5,
    )

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        assert client.config.tables.serving_table == SERVING_TEST_TABLE
        try:
            client.ingest_serving_batch(records)
            time.sleep(1)

            batches = list(
                client.export_data_batches(
                    filter_query=filter_query,
                    batch_size=2,
                    columns=["id", "created_at", "tags", "meta_json"],
                )
            )

            assert [len(batch) for batch in batches] == [2, 2, 1]
            expected_columns = ["id", "created_at", "tags", "meta_json"]
            assert all(list(batch.columns) == expected_columns for batch in batches)
            assert all(batch.attrs["wt_export"]["table"] == SERVING_TEST_TABLE for batch in batches)
            assert all(batch.attrs["wt_export"]["manifest_rows"] == 5 for batch in batches)

            exported_rows = [row for batch in batches for row in batch.to_dict("records")]
            assert {row["id"] for row in exported_rows} == {record.id for record in records}
            assert {json.loads(row["meta_json"])["job_id"] for row in exported_rows} == {job_id}
            assert all(tag in row["tags"] for row in exported_rows)
        finally:
            _cleanup_and_verify(client, job_id, tables=(SERVING_TEST_TABLE,))
