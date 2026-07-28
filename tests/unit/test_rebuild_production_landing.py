from types import SimpleNamespace

import pyarrow as pa

from scripts.migrations.production_landing_2026_07 import rebuild_wind_tunnel_landing as rebuild
from wt_sdk.core.schemas import (
    LANDING_PARTITIONS,
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_SCHEMA,
)


LEGACY_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("dt", pa.string(), nullable=False),
])


class _FakeSession:
    def __init__(self):
        self.records = {
            rebuild.SOURCE_TABLE: SimpleNamespace(
                partition_column="dt", partition_type="VALUE", partitions=-1
            ),
            rebuild.ARCHIVE_TABLE: SimpleNamespace(
                partition_column="dt", partition_type="VALUE", partitions=-1
            ),
        }
        self.schemas = {
            rebuild.SOURCE_TABLE: LEGACY_SCHEMA,
            rebuild.ARCHIVE_TABLE: LEGACY_SCHEMA,
        }
        self.counts = {
            rebuild.SOURCE_TABLE: 7,
            rebuild.ARCHIVE_TABLE: 7,
        }
        self.tables = {}
        self.schema_table = SimpleNamespace(get=self.records.get)
        self.dropped_tables = []
        self.shutdown_called = False

    def table_exists(self, table_name):
        return table_name in self.records

    def get_schema(self, table_name):
        return self.schemas[table_name]

    def count_rows(self, table_name):
        return self.counts[table_name]

    def drop_table(self, table_name):
        self.dropped_tables.append(table_name)
        self.records.pop(table_name)
        self.schemas.pop(table_name)
        self.counts.pop(table_name)

    def create_table(self, table_name, schema, partition_column, partition_type, partitions):
        self.records[table_name] = SimpleNamespace(
            partition_column=partition_column,
            partition_type=partition_type,
            partitions=partitions,
        )
        self.schemas[table_name] = schema
        self.counts[table_name] = 0

    def shutdown(self):
        self.shutdown_called = True


def test_rebuild_dry_run_keeps_production_table(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(rebuild.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(rebuild, "_pin_exact_dldb_table", lambda *args: None)

    rebuild.rebuild_wind_tunnel_landing(dry_run=True, confirm_rebuild=False)

    assert session.dropped_tables == []
    assert session.counts[rebuild.SOURCE_TABLE] == 7
    assert session.shutdown_called


def test_rebuild_replaces_only_landing_after_archive_check(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(rebuild.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(rebuild, "_pin_exact_dldb_table", lambda *args: None)

    rebuild.rebuild_wind_tunnel_landing(dry_run=False, confirm_rebuild=True)

    assert session.dropped_tables == [rebuild.SOURCE_TABLE]
    assert session.records[rebuild.SOURCE_TABLE].partition_column == LANDING_PARTITION_COLUMN
    assert session.records[rebuild.SOURCE_TABLE].partition_type == LANDING_PARTITION_TYPE
    assert session.records[rebuild.SOURCE_TABLE].partitions == LANDING_PARTITIONS
    assert session.schemas[rebuild.SOURCE_TABLE] == LANDING_SCHEMA
    assert session.counts[rebuild.SOURCE_TABLE] == 0
    assert session.counts[rebuild.ARCHIVE_TABLE] == 7
