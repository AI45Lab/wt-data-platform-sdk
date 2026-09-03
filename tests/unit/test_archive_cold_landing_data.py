from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import scripts.ops.archive_cold_landing_data as archive


def _job_frame(**overrides):
    rows = [
        {
            "id": "row-1",
            "job_id": "dataset#harness#model#task#20260801",
            "session_id": "session-1",
            "source_updated_at": 100,
            "is_session_completed": False,
            "is_trainable": False,
        },
        {
            "id": "row-2",
            "job_id": "dataset#harness#model#task#20260801",
            "session_id": "session-1",
            "source_updated_at": 200,
            "is_session_completed": True,
            "is_trainable": True,
        },
    ]
    for row in rows:
        row.update(overrides)
    return pd.DataFrame(rows)


def test_parse_cutoff_date_is_inclusive_in_shanghai_timezone():
    cutoff_date, exclusive_ms = archive.parse_cutoff_date("2026-08-18")

    expected = datetime(
        2026,
        8,
        19,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    assert cutoff_date.isoformat() == "2026-08-18"
    assert exclusive_ms == int(expected.timestamp() * 1000)


def test_parse_cutoff_date_rejects_non_iso_day():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        archive.parse_cutoff_date("2026/08/18")


def test_default_archive_table_uses_shanghai_calendar_date():
    value = archive.default_archive_table(
        now=datetime(2026, 8, 24, 16, 30, tzinfo=ZoneInfo("UTC"))
    )
    assert value == "archived_20260825_wind_tunnel_landing"


def test_resolve_archive_target_is_test_safe_and_uses_test_archive_name():
    target = archive.resolve_archive_target(
        "test",
        now=datetime(2026, 8, 24, 16, 30, tzinfo=ZoneInfo("UTC")),
    )

    assert target.profile == "test"
    assert target.source_table == "v2_landing_test"
    assert target.serving_table == "serving_test"
    assert target.archive_table == "archived_20260825_v2_landing_test"


def test_evaluate_cold_job_requires_complete_enriched_whole_job():
    job_id = "dataset#harness#model#task#20260801"
    job, reason = archive.evaluate_cold_job(
        _job_frame(),
        job_id=job_id,
        bucket=7,
        cutoff_exclusive_ms=1_000,
    )

    assert reason is None
    assert job is not None
    assert job.job_id == job_id
    assert job.bucket == 7
    assert job.row_count == 2
    assert job.max_source_updated_at == 200
    assert job.trainable_ids == frozenset({"row-2"})


@pytest.mark.parametrize(
    ("frame", "reason_fragment"),
    [
        (_job_frame(source_updated_at=1_000), "newer than"),
        (_job_frame(is_session_completed=False), "incomplete sessions"),
        (_job_frame(is_trainable=None), "has not populated"),
    ],
)
def test_evaluate_cold_job_rejects_unsafe_jobs(frame, reason_fragment):
    job, reason = archive.evaluate_cold_job(
        frame,
        job_id="dataset#harness#model#task#20260801",
        bucket=7,
        cutoff_exclusive_ms=1_000,
    )

    assert job is None
    assert reason_fragment in reason


def test_verify_serving_publication_requires_matching_source_version(monkeypatch):
    job, _ = archive.evaluate_cold_job(
        _job_frame(),
        job_id="dataset#harness#model#task#20260801",
        bucket=7,
        cutoff_exclusive_ms=1_000,
    )
    assert job is not None

    monkeypatch.setattr(
        archive,
        "query_partition",
        lambda *args, **kwargs: pd.DataFrame(
            [{"id": "row-2", "source_updated_at": 200}]
        ),
    )
    valid, reason = archive.verify_serving_publication(
        object(),
        job=job,
        serving_table="serving_test",
        serving_partitions={7},
    )
    assert valid is True
    assert reason is None

    monkeypatch.setattr(
        archive,
        "query_partition",
        lambda *args, **kwargs: pd.DataFrame(
            [{"id": "row-2", "source_updated_at": 199}]
        ),
    )
    valid, reason = archive.verify_serving_publication(
        object(),
        job=job,
        serving_table="serving_test",
        serving_partitions={7},
    )
    assert valid is False
    assert "stale source_updated_at" in reason


def test_dry_run_discovers_without_creating_archive(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.shutdown_called = False

        def shutdown(self):
            self.shutdown_called = True

    session = FakeSession()
    job, _ = archive.evaluate_cold_job(
        _job_frame(),
        job_id="dataset#harness#model#task#20260801",
        bucket=7,
        cutoff_exclusive_ms=1_000,
    )
    assert job is not None

    monkeypatch.setattr(archive.dldb, "connect", lambda *args, **kwargs: session)
    monkeypatch.setattr(archive, "verify_landing_layout", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        archive,
        "discover_cold_jobs",
        lambda *args, **kwargs: ([job], {"not ready": 2}),
    )
    monkeypatch.setattr(
        archive,
        "ensure_archive_table",
        lambda *args, **kwargs: pytest.fail("dry run must not create an archive table"),
    )
    monkeypatch.setattr(
        archive,
        "resolve_archive_target",
        lambda profile: archive.ArchiveTarget(
            profile="test",
            source_table="v2_landing_test",
            serving_table="serving_test",
            archive_table="archived_20260825_v2_landing_test",
        ),
    )

    result = archive.run_archive(
        cutoff_date_value="2026-08-18",
        batch_size=500,
        execute=False,
        confirm_delete=False,
        max_jobs=None,
        db_uri="s3://test-db",
        profile="test",
    )

    assert result["archive_table"] == "archived_20260825_v2_landing_test"
    assert result["source_table"] == "v2_landing_test"
    assert result["eligible_jobs"] == 1
    assert result["eligible_rows"] == 2
    assert result["affected_buckets"] == [7]
    assert result["archived_jobs"] == []
    assert session.shutdown_called is True


def test_execute_requires_explicit_delete_confirmation():
    with pytest.raises(ValueError, match="--confirm-delete"):
        archive.run_archive(
            cutoff_date_value="2026-08-18",
            batch_size=500,
            execute=True,
            confirm_delete=False,
            max_jobs=None,
            db_uri="s3://test-db",
            profile="test",
        )


def test_rollback_removes_staged_archive_only_when_source_is_intact(monkeypatch):
    job, _ = archive.evaluate_cold_job(
        _job_frame(),
        job_id="dataset#harness#model#task#20260801",
        bucket=7,
        cutoff_exclusive_ms=1_000,
    )
    assert job is not None

    class FakeSession:
        def __init__(self):
            self.deleted = []

        def delete(self, table_name, query, partition=None):
            self.deleted.append((table_name, query, partition))

    session = FakeSession()
    responses = iter(
        [
            dict(job.source_updated_at_by_id),
            {},
        ]
    )
    monkeypatch.setattr(
        archive,
        "query_job_id_timestamps",
        lambda *args, **kwargs: next(responses),
    )
    monkeypatch.setattr(archive, "list_partitions", lambda *args, **kwargs: [7])

    assert archive.rollback_archive_job_if_source_intact(
        session,
        job=job,
        source_table="v2_landing_test",
        archive_table="archived_20260825_wind_tunnel_landing",
    ) is True
    assert session.deleted == [
        (
            "archived_20260825_wind_tunnel_landing",
            "job_id = 'dataset#harness#model#task#20260801'",
            7,
        )
    ]
