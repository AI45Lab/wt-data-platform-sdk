"""
Initialize landing table for Wind Tunnel data with DLDB partition support.
"""
import argparse

import dldb
from wt_sdk.config import DEFAULT_LANDING_TABLE, db_uri_for_table, storage_options_for
from wt_sdk.core.schemas import (
    LANDING_PARTITIONS,
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_SCHEMA,
    LANDING_SCALAR_INDEXES,
)


TABLE_NAME = DEFAULT_LANDING_TABLE
PARTITION_COLUMN = LANDING_PARTITION_COLUMN


def init_landing_table(dry_run: bool = False):
    """Initialize landing table with DLDB wrapper SDK."""
    print(
        f"Creating table '{TABLE_NAME}' with partition_column='{PARTITION_COLUMN}', "
        f"partition_type='{LANDING_PARTITION_TYPE}'..."
    )
    create_kwargs = {
        "partition_column": LANDING_PARTITION_COLUMN,
        "partition_type": LANDING_PARTITION_TYPE,
    }
    if LANDING_PARTITION_TYPE == "HASH":
        create_kwargs["partitions"] = LANDING_PARTITIONS

    if dry_run:
        print("[DRY RUN] No table was created.")
        return

    db_uri = db_uri_for_table(TABLE_NAME)
    connect_kwargs = {}
    storage_options = storage_options_for(db_uri)
    if storage_options is not None:
        connect_kwargs["storage_options"] = storage_options
    print(f"Connecting to {db_uri}...")
    session = dldb.connect(db_uri, **connect_kwargs)
    try:
        session.create_table(
            TABLE_NAME,
            LANDING_SCHEMA,
            **create_kwargs,
        )
    finally:
        session.shutdown()

    print(
        f"  ✓ Table '{TABLE_NAME}' created with {LANDING_PARTITION_TYPE} "
        f"partition on '{PARTITION_COLUMN}'"
    )

    print(f"\n✓ Landing table initialization complete!")
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
    parser = argparse.ArgumentParser(
        description="Create an empty production landing table using HASH(job_id)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the intended table configuration without connecting or creating a table.",
    )
    args = parser.parse_args()
    init_landing_table(dry_run=args.dry_run)
