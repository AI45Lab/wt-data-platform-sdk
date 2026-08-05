"""Reusable safety helpers for contributor-owned ``landing_test`` tests."""

import time

from dldb.utils import stable_hash

from wt_sdk import GatewayConfig, TableConfig, WTGatewayClient
from wt_sdk.core.schemas import LANDING_PARTITIONS, SERVING_PARTITIONS


LANDING_TEST_TABLE = "landing_test"
SERVING_TEST_TABLE = "serving_test"
TEST_TABLE_CONFIG = GatewayConfig(
    tables=TableConfig(
        profile="test",
        landing_table=LANDING_TEST_TABLE,
        serving_table=SERVING_TEST_TABLE,
    )
)


def cleanup_test_trajectory(client: WTGatewayClient, job_id: str) -> None:
    """Delete one unique test job from both tables and verify cleanup."""

    escaped_job_id = job_id.replace("'", "''")
    filter_query = f"job_id = '{escaped_job_id}'"
    errors: list[str] = []
    existing_tables: list[tuple[str, int]] = []
    for table_name, partition_count, delete in (
        (LANDING_TEST_TABLE, LANDING_PARTITIONS, client.delete_landing),
        (SERVING_TEST_TABLE, SERVING_PARTITIONS, client.delete_serving),
    ):
        try:
            bucket = stable_hash(job_id) % partition_count
            existing_buckets = set(client.list_table_partitions(table=table_name))
            if bucket not in existing_buckets:
                continue
            delete(filter_query)
            existing_tables.append((table_name, bucket))
        except Exception as exc:
            errors.append(f"{table_name} delete failed: {exc}")

    time.sleep(1)
    for table_name, bucket in existing_tables:
        try:
            remaining = client.query_data(
                filter_query=filter_query,
                partition=bucket,
                table=table_name,
                checkout_latest=True,
            )
        except Exception as exc:
            errors.append(f"{table_name} cleanup verification failed: {exc}")
        else:
            if remaining:
                errors.append(
                    f"{table_name} cleanup left {len(remaining)} row(s): "
                    f"{[row.get('id') for row in remaining]}"
                )

    if errors:
        raise AssertionError("Integration cleanup failed: " + "; ".join(errors))


__all__ = [
    "LANDING_TEST_TABLE",
    "SERVING_TEST_TABLE",
    "TEST_TABLE_CONFIG",
    "cleanup_test_trajectory",
]
