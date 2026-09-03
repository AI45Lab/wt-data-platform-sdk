import json

import pytest

from scripts.ops.update_table_rows import (
    build_config,
    ids_filter,
    parse_updates,
    profile_table_name,
    rows_requiring_update,
    update_and_verify,
)


def test_profile_table_name_resolves_only_active_tables():
    assert profile_table_name("test", "landing") == "v2_landing_test"
    assert profile_table_name("test", "serving") == "serving_test"
    assert profile_table_name("prod", "landing") == "wind_tunnel_landing"
    assert profile_table_name("production", "serving") == "wind_tunnel_serving"

    with pytest.raises(ValueError, match="table role"):
        profile_table_name("test", "legacy")
    with pytest.raises(ValueError, match="profile"):
        profile_table_name("staging", "landing")


def test_build_config_pins_profile_tables_despite_environment_overrides(monkeypatch):
    monkeypatch.setenv("WT_SDK_LANDING_TABLE", "unexpected_landing")
    monkeypatch.setenv("WT_SDK_SERVING_TABLE", "unexpected_serving")

    config = build_config("prod", "memory://test")

    assert config.tables.profile == "production"
    assert config.tables.db_uri == "memory://test"
    assert config.tables.landing_table == "wind_tunnel_landing"
    assert config.tables.serving_table == "wind_tunnel_serving"


def test_parse_updates_validates_columns_and_serializes_json_documents():
    updates = parse_updates(
        json.dumps(
            {
                "is_session_completed": True,
                "meta_json": {"provider": "anthropic"},
                "tags": ["dataset", "harness"],
            }
        )
    )

    assert updates == {
        "is_session_completed": True,
        "meta_json": '{"provider":"anthropic"}',
        "tags": ["dataset", "harness"],
    }

    with pytest.raises(ValueError, match="non-empty JSON object"):
        parse_updates("{}")
    with pytest.raises(ValueError, match="unknown update columns"):
        parse_updates('{"unknown": true}')
    with pytest.raises(ValueError, match="cannot be updated"):
        parse_updates('{"source_updated_at": 1}')
    with pytest.raises(ValueError, match="not nullable"):
        parse_updates('{"dataset_type": null}')


def test_rows_requiring_update_skips_existing_values_and_handles_nulls():
    rows = [
        {"id": "one", "is_session_completed": False, "tags": None},
        {"id": "two", "is_session_completed": True, "tags": None},
        {"id": "three", "is_session_completed": True, "tags": ["a"]},
    ]

    changed = rows_requiring_update(
        rows,
        {"is_session_completed": True, "tags": None},
    )

    assert [row["id"] for row in changed] == ["one", "three"]


def test_ids_filter_scopes_hash_collisions_and_escapes_values():
    predicate = ids_filter([{"id": "record'1"}], "job'1")
    assert predicate == "job_id = 'job''1' AND id IN ('record''1')"


class _FakeSession:
    def __init__(self):
        self.calls = []

    def update(self, table_name, predicate, updates, partition=None):
        self.calls.append((table_name, predicate, dict(updates), partition))


class _FakeClient:
    SERVING_PARTITION_KEY = "job_id"

    def __init__(self, verified_rows):
        self.verified_rows = verified_rows
        self.landing_calls = []
        self.session = _FakeSession()

    def update_landing(self, filter_query, updates, partition=None):
        self.landing_calls.append((filter_query, dict(updates), partition))

    def query_data(self, **kwargs):
        return list(self.verified_rows)

    def _resolve_explicit_partition_for_table(self, table_name, job_id, fallback_key):
        assert table_name == "serving_test"
        assert fallback_key == "job_id"
        return 17 if job_id == "job-1" else None


def test_update_and_verify_uses_landing_api_and_preserves_source_filter():
    client = _FakeClient([{"id": "record-1", "is_session_completed": True}])

    verified = update_and_verify(
        client,
        "landing",
        "v2_landing_test",
        [{"id": "record-1", "job_id": "job-1"}],
        {"is_session_completed": True},
        "is_session_completed = false",
    )

    assert verified == 1
    predicate, updates, partition = client.landing_calls[0]
    assert predicate == (
        "(is_session_completed = false) AND "
        "(job_id = 'job-1' AND id IN ('record-1'))"
    )
    assert updates == {"is_session_completed": True}
    assert partition == "job-1"


def test_update_and_verify_updates_serving_through_dldb_and_stamps_time(monkeypatch):
    client = _FakeClient([{"id": "record-1", "is_session_completed": True}])
    monkeypatch.setattr("scripts.ops.update_table_rows.sdk_time.now_ms", lambda: 123456)

    verified = update_and_verify(
        client,
        "serving",
        "serving_test",
        [{"id": "record-1", "job_id": "job-1"}],
        {"is_session_completed": True},
        "is_session_completed = false",
    )

    assert verified == 1
    table_name, predicate, updates, partition = client.session.calls[0]
    assert table_name == "serving_test"
    assert predicate == (
        "(is_session_completed = false) AND "
        "(job_id = 'job-1' AND id IN ('record-1'))"
    )
    assert updates == {
        "is_session_completed": True,
        "serving_updated_at": 123456,
    }
    assert partition == 17
