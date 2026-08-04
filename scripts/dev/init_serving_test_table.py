"""
Populate the existing serving_test table with sample data for testing.

Use create_serving_test_table.py first when the logical table does not exist.
The records include various tags for testing get_tags_distribution().

Usage:
    python scripts/dev/init_serving_test_table.py
"""
import json
import time
from typing import List
from loguru import logger

from wt_sdk import GatewayConfig, TableConfig, WTGatewayClient
from wt_sdk.models import ServingRecord


TEST_SERVING_CONFIG = GatewayConfig(
    tables=TableConfig(serving_table="serving_test")
)


# Define the 14 tags that exist in the system
ALL_TAGS = [
    "illegal_facilitation",
    "violence_threats",
    "civic_values",
    "ethical_integrity",
    "privacy_datasecurity",
    "hate_harassment",
    "fraud_deception",
    "highrisk_misinformation",
    "science_rationality",
    "sexual_content",
    "human_dignity",
    "extremism",
    "selfharm_suicide",
    "historical_truth_responsibility"
]


def create_test_serving_records(count: int = 20) -> List[ServingRecord]:
    """Create test ServingRecords with diverse tags for distribution testing."""
    records = []
    current_time = int(time.time())

    # Create records with different tag combinations
    tag_distributions = [
        ["violence_threats", "hate_harassment"],
        ["selfharm_suicide", "highrisk_misinformation"],
        ["sexual_content"],
        ["hate_harassment", "civic_values"],
        ["illegal_facilitation", "fraud_deception"],
        ["privacy_datasecurity"],
        ["highrisk_misinformation", "science_rationality"],
        ["extremism", "violence_threats"],
        ["civic_values", "ethical_integrity"],
        ["human_dignity", "hate_harassment"],
        ["historical_truth_responsibility", "highrisk_misinformation"],
        ["science_rationality"],
        ["violence_threats", "illegal_facilitation", "extremism"],
        ["sexual_content", "privacy_datasecurity"],
        ["fraud_deception", "privacy_datasecurity"],
        ["selfharm_suicide", "human_dignity"],
        ["ethical_integrity", "science_rationality", "civic_values"],
        ["hate_harassment", "violence_threats", "extremism"],
        ["highrisk_misinformation", "fraud_deception", "illegal_facilitation"],
        ["violence_threats", "hate_harassment", "sexual_content", "highrisk_misinformation"]
    ]

    for i in range(count):
        tags = tag_distributions[i % len(tag_distributions)]

        record = ServingRecord(
            dataset_type="TEST_TAGS",
            dt="2025-01-15",
            id=f"serving_test_{current_time}_{i}",
            job_id="serving-test-tags",
            session_id=f"session_{i % 5}",
            created_at=current_time + i,
            step_id=i,
            is_terminal=(i == count - 1),
            step_reward=0.1 * (i % 10),
            reward=0.5 + 0.05 * (i % 10),
            messages=json.dumps(
                [
                    {"role": "user", "content": f"Test message {i} with tags: {', '.join(tags)}"},
                    {"role": "assistant", "content": f"Test response {i}"},
                ]
            ),
            response=json.dumps(
                {"role": "assistant", "content": f"Final response {i}"}
            ),
            ground_truth_answer=f"Answer_{i}",
            search_text=f"Test message {i} Test response {i} Final response {i}",
            agent_model="test-model",
            env_name="test-env",
            is_session_completed=(i == count - 1),
            tags=tags,
        )
        records.append(record)

    return records


def init_serving_test_table():
    """Initialize the serving_test table with test data."""
    logger.info("=" * 60)
    logger.info("Initializing serving_test table")
    logger.info("=" * 60)

    # Initialize client with test config
    client = WTGatewayClient(config=TEST_SERVING_CONFIG)
    logger.info(f"Serving table: {TEST_SERVING_CONFIG.tables.serving_table}")

    try:
        # Create test records
        logger.info("\n1. Creating test records...")
        records = create_test_serving_records(count=20)
        logger.info(f"   Created {len(records)} test records")

        # Show tag distribution in test data
        tag_counts = {}
        for record in records:
            if record.tags:
                for tag in record.tags:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1

        logger.info("\n2. Expected tag distribution:")
        for tag, count in sorted(tag_counts.items(), key=lambda x: -x[1]):
            logger.info(f"   - {tag}: {count}")

        # Ingest records
        logger.info("\n3. Ingesting records to serving_test table...")
        client.ingest_serving_batch(records)
        logger.info(f"   Ingested {len(records)} records")

        # Verify count
        time.sleep(1)  # Wait for data to be available
        count = client.count_serving()
        logger.info(f"\n4. Verification: serving_test table has {count} records")

        logger.info("\n" + "=" * 60)
        logger.info("serving_test table initialized successfully!")
        logger.info("=" * 60)

    finally:
        client.close()


def cleanup_serving_test_table():
    """Clean up the serving_test table."""
    logger.info("Cleaning up serving_test table...")

    client = WTGatewayClient(config=TEST_SERVING_CONFIG)

    try:
        deleted = client.delete_serving(
            "job_id = 'serving-test-tags' AND dataset_type = 'TEST_TAGS'"
        )
        logger.info(f"Deleted {deleted} records from serving_test table")
    finally:
        client.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--cleanup":
        cleanup_serving_test_table()
    else:
        init_serving_test_table()
