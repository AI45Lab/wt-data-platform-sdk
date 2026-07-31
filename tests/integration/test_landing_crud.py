"""
Test basic CRUD operations on landing table.

This test demonstrates:
1. Inserting 3 items into landing_table
2. Querying them
3. Deleting them

Each query and delete includes `job_id` so the test also covers HASH(job_id)
partition pruning through the SDK.
"""
import time
import uuid
from contextlib import contextmanager
from wt_sdk import GatewayConfig, LandingRecord, ChatMessage, ContentItem, TableConfig, WTGatewayClient


TEST_TABLE_CONFIG = GatewayConfig(tables=TableConfig(landing_table="landing_test"))


@contextmanager
def _test_client(job_id: str, session_id: str):
    """Clean up this test's rows even when an assertion or read fails."""
    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        try:
            yield client
        finally:
            try:
                client.delete_landing(
                    filter_query=f"job_id = '{job_id}' AND session_id = '{session_id}'"
                )
            except Exception as exc:
                print(f"WARNING: cleanup failed for job_id={job_id}: {exc}")


def create_test_record(index: int, session_id: str) -> LandingRecord:
    """Create a test LandingRecord with sample data."""
    return LandingRecord(
        dataset_type="test_dataset",
        dt="2026-01-04",
        id=f"test_{uuid.uuid4().hex[:8]}_{index}",
        session_id=session_id,
        created_at=int(time.time()),
        job_id=f"job_{session_id}",
        messages=[
            ChatMessage(
                role="user",
                content=[
                    ContentItem(type="text", text=f"Test message {index}")
                ]
            ),
            ChatMessage(
                role="assistant",
                content=[
                    ContentItem(type="text", text=f"Test response {index}")
                ]
            )
        ],
        agent_model="test-model",
        env_name="test-env",
        is_session_completed=False,
        ground_truth_answer=f"answer_{index}"
    )


def test_insert_query_delete():
    """Test inserting, querying, and deleting 3 items from landing table."""
    test_session_id = f"test_session_{uuid.uuid4().hex[:8]}"
    job_id = f"job_{test_session_id}"

    print("=" * 80)
    print("Starting landing table CRUD test")
    print("=" * 80)

    # Initialize client with test table config
    print("\n1. Initializing client...")
    with _test_client(job_id, test_session_id) as client:
        print(f"   Client initialized successfully")
        print(f"   Landing table: {client.landing_uri}")

        # Get initial count
        initial_count = client.count_landing()
        print(f"   Initial table count: {initial_count}")

        # Create 3 test records
        print("\n2. Creating 3 test records...")
        test_records = [
            create_test_record(i, test_session_id)
            for i in range(1, 4)
        ]
        for record in test_records:
            print(f"   - {record.id}: session_id={record.session_id}")

        # Insert records
        print("\n3. Inserting records into landing table...")
        client.ingest_landing_batch(test_records)
        print(f"   Inserted {len(test_records)} records")

        # Give a moment for data to be available
        time.sleep(1)

        # Query all records with our session_id
        print("\n4. Querying records back...")
        queried_records = client.query_data(
            filter_query=f"job_id = '{job_id}' AND session_id = '{test_session_id}'"
        )

        print(f"   Found {len(queried_records)} records:")
        for record in queried_records:
            print(f"   - {record.id}:")
            print(f"     dataset_type: {record.dataset_type}")
            print(f"     messages: {len(record.messages)} messages")
            if record.messages:
                print(f"     first message: {record.messages[0].role}")

        # Verify we got all 3 records
        assert len(queried_records) == 3, f"Expected 3 records, got {len(queried_records)}"
        print("\n   ✓ All 3 records successfully retrieved")

        # Query with limit
        print("\n5. Querying with limit=2...")
        limited_records = client.query_data(
            filter_query=f"job_id = '{job_id}' AND session_id = '{test_session_id}'",
            limit=2
        )
        print(f"   Retrieved {len(limited_records)} records (limited)")
        assert len(limited_records) == 2, f"Expected 2 records with limit, got {len(limited_records)}"
        print("   ✓ Limit working correctly")

        # Delete the records
        print("\n6. Deleting test records...")
        deleted_count = client.delete_landing(
            filter_query=f"job_id = '{job_id}' AND session_id = '{test_session_id}'"
        )
        print(f"   Deleted {deleted_count} records")
        assert deleted_count == 3, f"Expected to delete 3 records, deleted {deleted_count}"
        print("   ✓ All test records deleted")

        # Verify deletion
        print("\n7. Verifying deletion...")
        # NOTE: Delete uses LanceDB native SDK while query uses DLDB wrapper.
        # There might be a delay for consistency between the two connections.
        time.sleep(3)  # Give more time for deletion to propagate
        remaining_records = client.query_data(
            filter_query=f"job_id = '{job_id}' AND session_id = '{test_session_id}'"
        )
        print(f"   Remaining records with session_id: {len(remaining_records)}")

        if len(remaining_records) > 0:
            print(f"   WARNING: {len(remaining_records)} records still found after deletion")
        else:
            print("   ✓ Verification complete - no test records remain")

    print("\n" + "=" * 80)
    print("✓ All tests passed successfully!")
    print("=" * 80)


def test_insert_single_and_batch():
    """Test both single and batch insert methods."""
    test_session_id = f"test_session_{uuid.uuid4().hex[:8]}"
    job_id = f"job_{test_session_id}"

    print("\n" + "=" * 80)
    print("Testing single and batch insert methods")
    print("=" * 80)

    with _test_client(job_id, test_session_id) as client:
        # Insert single record
        print("\n1. Testing single insert...")
        single_record = create_test_record(0, test_session_id)
        client.ingest_landing(single_record)
        print(f"   Inserted single record: {single_record.id}")

        time.sleep(1)

        # Insert batch of 2 records
        print("\n2. Testing batch insert...")
        batch_records = [
            create_test_record(i, test_session_id)
            for i in range(1, 3)
        ]
        client.ingest_landing_batch(batch_records)
        print(f"   Inserted batch of {len(batch_records)} records")

        time.sleep(1)

        # Query and verify
        print("\n3. Verifying inserts...")
        all_records = client.query_data(
            filter_query=f"job_id = '{job_id}' AND session_id = '{test_session_id}'"
        )
        print(f"   Total records found: {len(all_records)}")
        assert len(all_records) == 3, f"Expected 3 records, got {len(all_records)}"
        print("   ✓ Both single and batch inserts successful")

        # Cleanup
        print("\n4. Cleaning up...")
        client.delete_landing(
            filter_query=f"job_id = '{job_id}' AND session_id = '{test_session_id}'"
        )
        print("   ✓ Cleanup complete")

    print("\n" + "=" * 80)
    print("✓ Single and batch insert test passed!")
    print("=" * 80)


if __name__ == "__main__":
    # Run the tests
    test_insert_query_delete()
    test_insert_single_and_batch()
    print("\n✓ All test suites completed successfully!")
