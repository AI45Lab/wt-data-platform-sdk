import time
from wt_sdk import WTGatewayClient
from wt_sdk.models import LandingRecord, ChatMessage, ContentItem
from wt_sdk.config import GatewayConfig, TableConfig


TEST_DATASET_TYPE = "CURSOR_TEST"
CHUNK_SIZE = 10
TOTAL_RECORDS = 25


def create_test_records(count: int) -> list:
    current_time = int(time.time())
    records = []

    for i in range(count):
        record = LandingRecord(
            dataset_type=TEST_DATASET_TYPE,
            dt="2025-01-15",
            id=f"cursor_demo_{current_time}_{i:03d}",
            session_id=f"session_{i // 5}",
            created_at=current_time + i,
            step_id=i,
            is_terminal=(i == count - 1),
            step_reward=0.1 * i,
            reward=0.5,
            messages=[
                ChatMessage(
                    role="user",
                    content=[ContentItem(type="text", text=f"Test message {i}")]
                )
            ],
            response=ChatMessage(
                role="assistant",
                content=[ContentItem(type="text", text=f"Response {i}")]
            ),
            agent_model="demo-model",
            env_name="demo-env",
            is_session_completed=False,
        )
        records.append(record)

    return records


def run_pagination_demo():
    print("=" * 70)
    print("WTGatewayClient Cursor-Based Pagination Demo")
    print("=" * 70)
    print("\nNOTE: This demo uses cursor pagination with created_at")
    print("      - Uses dldb's order_by for efficient server-side sorting")
    print("      - Cursor (created_at) is pushed down to reduce data volume")
    print("      - May skip records with the same created_at as cursor")

    print("\n[1] Initializing WTGatewayClient...")
    # Create custom config to use landing_test table
    test_config = GatewayConfig(tables=TableConfig(landing_table="landing_test"))
    client = WTGatewayClient(test_config)
    print(f"    Client initialized")
    print(f"    Using table: {test_config.tables.landing_table}")

    # Cleanup
    print("\n[2] Cleaning up any existing test data...")
    try:
        deleted = client.session.delete(test_config.tables.landing_table, f"dataset_type = '{TEST_DATASET_TYPE}'")
        print(f"    Deleted {deleted} existing records")
    except Exception as e:
        print(f"    ℹ No existing data to clean up")

    # Create and ingest test records
    print(f"\n[3] Creating {TOTAL_RECORDS} test records...")
    test_records = create_test_records(TOTAL_RECORDS)
    print(f"    Created {len(test_records)} records with dataset_type='{TEST_DATASET_TYPE}'")
    print(f"    - created_at: ranges from {test_records[0].created_at} to {test_records[-1].created_at}")
    print(f"    - id: cursor_demo_xxx_000 to cursor_demo_xxx_024")

    print("\n[4] Ingesting test records...")
    client.ingest_landing_batch(test_records)
    print(f"    Successfully ingested {len(test_records)} records")
    time.sleep(1)

    # Test 1: get_max_created_at
    print("\n" + "=" * 70)
    print("Test 1: get_max_created_at()")
    print("=" * 70)
    max_record = client.get_max_created_at(f"dataset_type = '{TEST_DATASET_TYPE}'")
    print(f"    Max record: id={max_record['id']}, created_at={max_record['created_at']}")
    expected_max = test_records[-1].created_at
    print(f"    Expected max:   {expected_max}")
    if max_record['created_at'] == expected_max:
        print(f"    ✓ get_max_created_at works correctly!")
    else:
        print(f"    ✗ get_max_created_at returned wrong value!")

    # Test 2: cursor-based pagination
    print("\n" + "=" * 70)
    print("Test 2: Cursor-Based Pagination")
    print("=" * 70)
    print("\nHow it works:")
    print("  1. Use dldb's order_by (server-side sorting)")
    print("  2. Cursor filter (created_at > cursor) pushed to DB")
    print("  3. Limit applied after sorting")
    print("  4. Extract cursor (max created_at) for next batch")
    print()

    print(f"[Step 1] Pulling data in batches of {CHUNK_SIZE}...")
    print("-" * 50)

    cursor = None
    all_records = []
    page_num = 0

    while True:
        # Pull data with cursor
        df = client.pull_data(
            dataset_type=TEST_DATASET_TYPE,
            cursor=cursor,
            limit=CHUNK_SIZE
        )

        if df is None or len(df) == 0:
            print(f"\n    No more data to fetch")
            break

        page_num += 1

        # Get cursor for next batch
        cursor = client.extract_cursor(df)

        # Store records
        all_records.extend(df.to_dict('records'))

        print(f"\n    Page {page_num}:")
        print(f"        - Fetched {len(df)} records")
        print(f"        - First record: id={df.iloc[0]['id']}, created_at={df.iloc[0]['created_at']}")
        print(f"        - Last record:  id={df.iloc[-1]['id']}, created_at={df.iloc[-1]['created_at']}")
        print(f"        - Cursor for next page: {cursor}")

        if len(df) < CHUNK_SIZE:
            print(f"\n    Last page reached (fetched {len(df)} < {CHUNK_SIZE})")
            break

    # Summary
    print("\n" + "=" * 70)
    print("Pagination Summary")
    print("=" * 70)
    print(f"    Total pages: {page_num}")
    print(f"    Total records fetched: {len(all_records)}")
    print(f"    Expected records: {TOTAL_RECORDS}")

    # Verify no duplicates
    record_ids = [r['id'] for r in all_records]
    unique_ids = set(record_ids)
    print(f"    Unique record IDs: {len(unique_ids)}")

    # Verify correct ordering
    created_at_values = [r['created_at'] for r in all_records]
    is_sorted = created_at_values == sorted(created_at_values)
    print(f"    Records sorted by created_at: {is_sorted}")

    if len(record_ids) == len(unique_ids) and is_sorted:
        print(f"\n    SUCCESS: All {len(all_records)} records fetched correctly!")
    elif len(record_ids) != len(unique_ids):
        print(f"\n    WARNING: Found duplicates in fetched records")
    else:
        print(f"\n    WARNING: Records not properly sorted")

    # Test 3: iter_data_batches (auto-pagination)
    print("\n" + "=" * 70)
    print("Test 3: iter_data_batches (auto-pagination)")
    print("=" * 70)
    print("\nHow it works:")
    print("  1. Automatically handles pagination internally")
    print("  2. User just iterates over batches")
    print("  3. No cursor management needed")
    print()

    print(f"    Fetching data in chunks of {CHUNK_SIZE}...")
    print("-" * 50)

    all_fetch_records = []
    batch_num = 0

    for batch in client.iter_data_batches(
        dataset_type=TEST_DATASET_TYPE,
        chunk_size=CHUNK_SIZE,
    ):
        batch_num += 1
        records = batch.to_dict('records')
        all_fetch_records.extend(records)

        print(f"\n    Batch {batch_num}:")
        print(f"        - Fetched {len(batch)} records")
        print(f"        - First: id={batch.iloc[0]['id']}, created_at={batch.iloc[0]['created_at']}")
        print(f"        - Last:  id={batch.iloc[-1]['id']}, created_at={batch.iloc[-1]['created_at']}")

        if len(batch) < CHUNK_SIZE:
            print(f"\n    Last batch reached (fetched {len(batch)} < {CHUNK_SIZE})")
            break

    print("\n" + "=" * 70)
    print("iter_data_batches Summary")
    print("=" * 70)
    print(f"    Total batches: {batch_num}")
    print(f"    Total records fetched: {len(all_fetch_records)}")
    print(f"    Expected records: {TOTAL_RECORDS}")

    fetch_ids = [r['id'] for r in all_fetch_records]
    is_sorted = [r['created_at'] for r in all_fetch_records] == sorted([r['created_at'] for r in all_fetch_records])

    if len(fetch_ids) == len(set(fetch_ids)) and is_sorted:
        print(f"    ✓ No duplicates, correct ordering")
        print(f"\n    SUCCESS: iter_data_batches works correctly!")
    else:
        print(f"    ✗ Issues found in iter_data_batches results")

    # Cleanup
    print("\n[Cleanup] Deleting test data...")
    deleted = client.session.delete(test_config.tables.landing_table, f"dataset_type = '{TEST_DATASET_TYPE}'")
    print(f"    Deleted {deleted} test records")

    client.close()
    print("\n    Demo completed successfully!")


if __name__ == "__main__":
    run_pagination_demo()
