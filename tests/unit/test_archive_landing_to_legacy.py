from types import SimpleNamespace

import pandas as pd
import pyarrow as pa

from scripts.migrations.production_landing_2026_07 import archive_landing_to_legacy as archive


class _FakeTable:
    def __init__(self, rows_by_partition):
        self.rows_by_partition = rows_by_partition

    def list_partitions(self):
        return list(self.rows_by_partition)


class _FakeSession:
    def __init__(self, source_rows, schema):
        self.source_rows = source_rows
        self.archive_rows = {}
        self.schema = schema
        self.source_record = SimpleNamespace(
            partition_column="dt", partition_type="VALUE", partitions=-1
        )
        self.archive_record = None
        self.tables = {"wind_tunnel_landing": _FakeTable(self.source_rows)}
        self.schema_table = SimpleNamespace(get=self._get_record)
        self.shutdown_called = False

    def _get_record(self, table_name):
        if table_name == "wind_tunnel_landing":
            return self.source_record
        if table_name == "wind_tunnel_landing_legacy":
            return self.archive_record
        return None

    def table_exists(self, table_name):
        return self._get_record(table_name) is not None

    def get_schema(self, table_name):
        assert self.table_exists(table_name)
        return self.schema

    def create_table(self, table_name, schema, partition_column, partition_type):
        assert table_name == "wind_tunnel_landing_legacy"
        assert schema == self.schema
        self.archive_record = SimpleNamespace(
            partition_column=partition_column,
            partition_type=partition_type,
            partitions=-1,
        )
        self.tables[table_name] = _FakeTable(self.archive_rows)

    def count_rows(self, table_name, partition=None):
        rows = self.source_rows if table_name == "wind_tunnel_landing" else self.archive_rows
        if partition is not None:
            return len(rows.get(partition, []))
        return sum(len(partition_rows) for partition_rows in rows.values())

    def filter(self, table_name, query, limit, offset, partition_cond):
        assert table_name == "wind_tunnel_landing"
        assert query == ""
        partition = partition_cond.split(" = ", 1)[1].strip("'")
        frame = pd.DataFrame(self.source_rows[partition])
        return frame.iloc[offset : offset + limit].reset_index(drop=True)

    def add(self, table_name, frame, partition):
        assert table_name == "wind_tunnel_landing_legacy"
        self.archive_rows.setdefault(partition, []).extend(frame.to_dict("records"))

    def shutdown(self):
        self.shutdown_called = True


def test_archive_copies_each_partition_in_batches_and_verifies(monkeypatch):
    schema = pa.schema([
        pa.field("id", pa.string(), nullable=False),
        pa.field("dt", pa.string(), nullable=False),
        pa.field("job_id", pa.string(), nullable=True),
    ])
    source_rows = {
        "2025-01-06": [
            {"id": "a", "dt": "2025-01-06", "job_id": None},
            {"id": "b", "dt": "2025-01-06", "job_id": None},
            {"id": "c", "dt": "2025-01-06", "job_id": None},
        ],
        "2025-01-15": [
            {"id": "d", "dt": "2025-01-15", "job_id": None},
        ],
    }
    session = _FakeSession(source_rows, schema)

    monkeypatch.setattr(archive.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(archive, "_pin_exact_dldb_table", lambda *args: None)

    archive.archive_landing(
        source_table="wind_tunnel_landing",
        archive_table="wind_tunnel_landing_legacy",
        batch_size=2,
        dry_run=False,
        resume=False,
    )

    assert session.archive_rows == source_rows
    assert session.shutdown_called
