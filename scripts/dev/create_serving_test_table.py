"""
Initialize serving_test table for testing Wind Tunnel data platform.
This creates a test table in s3://wind-tunnel-dldb for testing purposes.
"""
import dldb
from wt_sdk.config import default_config
from wt_sdk.core.schemas import (
    SERVING_PARTITIONS,
    SERVING_PARTITION_COLUMN,
    SERVING_PARTITION_TYPE,
    SERVING_SCHEMA,
    SERVING_SCALAR_INDEXES,
)


TABLE_NAME = "serving_test"
PARTITION_COLUMN = SERVING_PARTITION_COLUMN


def init_serving_test_table():
    """Initialize serving_test table with DLDB wrapper SDK."""
    print(f"Connecting to {default_config.tables.db_uri}...")

    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options()
    )

    # 1. Create table with partition support
    print(f"\nCreating table '{TABLE_NAME}' with partition_column='{PARTITION_COLUMN}'...")
    session.create_table(
        TABLE_NAME,
        SERVING_SCHEMA,  # Required: PyArrow schema
        partition_column=PARTITION_COLUMN,
        partition_type=SERVING_PARTITION_TYPE,
        partitions=SERVING_PARTITIONS,
    )
    print(f"  ✓ Table '{TABLE_NAME}' created with partition on '{PARTITION_COLUMN}'")

    print(f"\n✓ Serving test table initialization complete!")
    print(f"  Table: {TABLE_NAME}")
    print(f"  Partition key: {PARTITION_COLUMN} ({SERVING_PARTITION_TYPE})")
    print(f"  Hash buckets: {SERVING_PARTITIONS}")

    # Index information
    print(f"\nScalar Indexes ({len(SERVING_SCALAR_INDEXES)} configured):")
    for column, index_type in SERVING_SCALAR_INDEXES:
        print(f"  - {column} ({index_type})")

    print(f"\n  NOTE: For HASH partitioned tables, indexes are created per bucket.")
    print(f"  After ingesting data, run the following to create indexes:")
    print(f"    # Check index status")
    print(f"    python scripts/inspect/show_table_indexes.py {TABLE_NAME}")
    print(f"    # Add missing indexes and optimize existing buckets")
    print(f"    python scripts/ops/maintain_table_indexes.py --table {TABLE_NAME} --all-partitions")


if __name__ == "__main__":
    init_serving_test_table()
