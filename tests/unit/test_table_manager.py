from types import SimpleNamespace

from scripts.ops import table_manager


class _FakeSession:
    def __init__(self):
        self.record = SimpleNamespace(partition_type="HASH")
        self.schema_table = SimpleNamespace(get=lambda name: self.record if name == "landing_test" else None)
        self.db_conn = object()
        self.tables = {}
        self.dropped = []
        self.shutdown_called = False

    def drop_table(self, table_name, partition=None):
        self.dropped.append((table_name, partition))

    def shutdown(self):
        self.shutdown_called = True


def test_drop_table_requires_exact_noninteractive_confirmation(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(table_manager.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(table_manager, "_pin_exact_dldb_table", lambda *args: None)

    assert not table_manager.drop_table("landing_test", force=True)
    assert session.dropped == []

    assert table_manager.drop_table(
        "landing_test",
        force=True,
        confirm_table="landing_test",
    )
    assert session.dropped == [("landing_test", None)]


def test_drop_table_refuses_unknown_exact_metadata(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(table_manager.dldb, "connect", lambda *args, **kwargs: session)

    assert not table_manager.drop_table(
        "landing_test_backup",
        force=True,
        confirm_table="landing_test_backup",
    )
    assert session.dropped == []
    assert session.shutdown_called


def test_drop_table_interactive_requires_table_name_and_drop(monkeypatch):
    session = _FakeSession()
    monkeypatch.setattr(table_manager.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(table_manager, "_pin_exact_dldb_table", lambda *args: None)
    inputs = iter(["landing_test", "DROP"])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(inputs))

    assert table_manager.drop_table("landing_test")
    assert session.dropped == [("landing_test", None)]


def test_index_field_supports_dldb_index_objects_and_dicts():
    object_index = SimpleNamespace(name="dataset_type_idx", index_type="BTREE")
    dict_index = {"name": "is_terminal_idx", "type": "BITMAP"}

    assert table_manager._index_field(object_index, "name") == "dataset_type_idx"
    assert table_manager._index_field(object_index, "type") == "BTREE"
    assert table_manager._index_field(dict_index, "name") == "is_terminal_idx"
    assert table_manager._index_field(dict_index, "type") == "BITMAP"
