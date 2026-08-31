import pandas as pd
import pyarrow as pa

import dldb
from dldb.table import SimpleTable

from wt_sdk.etl import Checkpoint
from wt_sdk.etl.checkpoint import DldbCheckpointStore, TEST_CHECKPOINT_TABLE


def test_legacy_unpartitioned_metadata_reopens_as_simple_table(tmp_path):
    db_uri = str(tmp_path / "legacy-simple-table")
    session = dldb.connect(db_uri)
    try:
        session.create_table(
            "evaluation_env_config",
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("job_id", pa.string(), nullable=False),
                ]
            ),
        )
        session.add(
            "evaluation_env_config",
            pd.DataFrame([{"id": 1, "job_id": "prod-job"}]),
        )

        # Reproduce the legacy metadata shape: VALUE plus an empty partition
        # column. dldb-v1.1.0 must treat the empty column as authoritative and
        # reopen the physical table as an unpartitioned SimpleTable.
        session.schema_table.table.update(
            where="table_name = 'evaluation_env_config'",
            values={"partition_type": "VALUE", "partition_column": ""},
        )
        session.schema_table.reload()
        session.tables.pop("evaluation_env_config", None)

        frame = session.filter(
            "evaluation_env_config",
            query="job_id = 'prod-job'",
            checkout_latest=True,
        )

        assert frame.to_dict("records") == [{"id": 1, "job_id": "prod-job"}]
        assert isinstance(session.tables["evaluation_env_config"], SimpleTable)
    finally:
        session.shutdown()


def test_dldb_resolves_similarly_prefixed_hash_tables_exactly(tmp_path):
    db_uri = str(tmp_path / "exact-table-lookup")
    session = dldb.connect(db_uri)
    try:
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("value", pa.string(), nullable=False),
            ]
        )
        session.create_table(
            "landing_test",
            schema=schema,
            partition_column="value",
            partition_type="HASH",
            partitions=4,
        )
        session.create_table(
            "landing_test_backup",
            schema=schema,
            partition_column="value",
            partition_type="HASH",
            partitions=4,
        )
        session.add("landing_test", pd.DataFrame([{"id": 1, "value": "active"}]))
        session.add("landing_test_backup", pd.DataFrame([{"id": 2, "value": "backup"}]))
        session.tables.clear()

        active = session.filter("landing_test", query="id IS NOT NULL")
        backup = session.filter("landing_test_backup", query="id IS NOT NULL")

        assert active.to_dict("records") == [{"id": 1, "value": "active"}]
        assert backup.to_dict("records") == [{"id": 2, "value": "backup"}]
    finally:
        session.shutdown()


def test_dldb_checkpoint_store_round_trips_on_simple_table(tmp_path):
    store = DldbCheckpointStore(
        str(tmp_path / "checkpoint-state"),
        table_name=TEST_CHECKPOINT_TABLE,
    )
    checkpoint = Checkpoint(
        pipeline_name="landing_to_serving_pipeline",
        pipeline_version="1",
        source_table="landing_test",
        target_table="serving_test",
        bucket=7,
        committed_until_ms=123_000,
        last_run_id="run-1",
        status="IDLE",
        updated_at_ms=124_000,
    )
    try:
        assert store.initialize() is True
        store.save(checkpoint)

        assert store.load(
            pipeline_name=checkpoint.pipeline_name,
            pipeline_version=checkpoint.pipeline_version,
            source_table=checkpoint.source_table,
            target_table=checkpoint.target_table,
            bucket=checkpoint.bucket,
        ) == checkpoint
    finally:
        store.close()
