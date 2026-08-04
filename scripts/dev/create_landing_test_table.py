"""
Initialize landing_test table for testing Wind Tunnel data platform.
This creates a test table in s3://wind-tunnel-dldb for testing purposes.
"""
import dldb
from wt_sdk.config import default_config
from wt_sdk.core.schemas import (
    LANDING_PARTITIONS,
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_SCALAR_INDEXES,
    LANDING_SCHEMA,
)


TABLE_NAME = "landing_test"
PARTITION_COLUMN = LANDING_PARTITION_COLUMN  # job_id


def init_landing_test_table():
    """Initialize landing_test table with DLDB wrapper SDK."""
    print(f"Connecting to {default_config.tables.db_uri}...")

    # Use DLDB wrapper SDK instead of LanceDB native
    session = dldb.connect(
        default_config.tables.db_uri,
        storage_options=default_config.s3.to_storage_options()
    )

    try:
        # 1. Create table with partition support
        print(
            f"\nCreating table '{TABLE_NAME}' with partition_column='{PARTITION_COLUMN}', "
            f"partition_type='{LANDING_PARTITION_TYPE}'..."
        )
        create_kwargs = {
            "partition_column": LANDING_PARTITION_COLUMN,
            "partition_type": LANDING_PARTITION_TYPE,
        }
        if LANDING_PARTITION_TYPE == "HASH":
            create_kwargs["partitions"] = LANDING_PARTITIONS

        session.create_table(
            TABLE_NAME,
            LANDING_SCHEMA,
            **create_kwargs,
        )

        record = session.schema_table.get(TABLE_NAME)
        actual_metadata = {
            "partition_column": record.partition_column,
            "partition_type": record.partition_type,
            "partitions": record.partitions,
        }
        expected_metadata = {
            "partition_column": LANDING_PARTITION_COLUMN,
            "partition_type": LANDING_PARTITION_TYPE,
            "partitions": LANDING_PARTITIONS,
        }
        if actual_metadata != expected_metadata:
            raise RuntimeError(
                f"Created table metadata mismatch: expected {expected_metadata}, "
                f"got {actual_metadata}"
            )
        if session.get_schema(TABLE_NAME) != LANDING_SCHEMA:
            raise RuntimeError("Created table schema does not match LANDING_SCHEMA")
    finally:
        session.shutdown()
    print(f"  ✓ Table '{TABLE_NAME}' created with {LANDING_PARTITION_TYPE} partition on '{PARTITION_COLUMN}'")

    print(f"\n✓ Landing test table initialization complete!")
    print(f"  Table: {TABLE_NAME}")
    print(f"  Partition key: {PARTITION_COLUMN}")
    if LANDING_PARTITION_TYPE == "HASH":
        print(f"  Hash buckets: {LANDING_PARTITIONS}")

    # Index information
    print(f"\nScalar Indexes ({len(LANDING_SCALAR_INDEXES)} configured):")
    for column, index_type in LANDING_SCALAR_INDEXES:
        print(f"  - {column} ({index_type})")

    print(f"\n  NOTE: For partitioned tables, indexes are created per physical partition.")
    print(f"  After ingesting data, run the following to create indexes:")
    print(f"    # Check index status")
    print(f"    python scripts/inspect/show_table_indexes.py {TABLE_NAME}")
    print(f"    # Add missing indexes and optimize existing buckets")
    print(f"    python scripts/ops/maintain_table_indexes.py --table {TABLE_NAME} --all-partitions")


if __name__ == "__main__":
    init_landing_test_table()
