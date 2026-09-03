from types import SimpleNamespace

import pytest

from scripts.inspect.show_partition_status import (
    _resolve_partitions,
    build_partition_report,
    expected_indexes_for_table,
)


def _coverage(name, indexed, unindexed, fully_indexed):
    return SimpleNamespace(
        index_name=name,
        num_indexed_rows=indexed,
        num_unindexed_rows=unindexed,
        fully_indexed=fully_indexed,
    )


def _status(*, rows, fragments, small_fragments, coverage):
    return SimpleNamespace(
        materialized=True,
        version=17,
        stats=SimpleNamespace(
            num_rows=rows,
            total_bytes=4096,
            fragment_stats=SimpleNamespace(
                num_fragments=fragments,
                num_small_fragments=small_fragments,
                lengths=SimpleNamespace(min=1, p50=4, p99=20, max=24),
            ),
        ),
        coverage=coverage,
    )


def test_expected_indexes_use_role_specific_sdk_definitions():
    landing = expected_indexes_for_table("v2_landing_test")
    serving = expected_indexes_for_table("wind_tunnel_serving")

    assert landing["id_idx"] == "BTREE"
    assert landing["is_trainable_idx"] == "BITMAP"
    assert "tags_idx" not in landing
    assert serving["tags_idx"] == "LABEL_LIST"


def test_build_partition_report_combines_missing_tail_and_fragment_signals():
    status = _status(
        rows=100,
        fragments=8,
        small_fragments=5,
        coverage=[
            _coverage("id_idx", 100, 0, True),
            _coverage("job_id_idx", 80, 20, False),
            _coverage("custom_idx", None, None, False),
        ],
    )

    report = build_partition_report(
        19,
        status,
        {"id_idx", "job_id_idx", "session_id_idx"},
    )

    assert report.state == "missing_idx+index_tail+fragmented"
    assert report.rows == 100
    assert report.fragments == 8
    assert report.small_fragments == 5
    assert report.missing_indexes == ("session_id_idx",)
    assert report.unexpected_indexes == ("custom_idx",)
    assert [index.name for index in report.index_tails] == ["custom_idx", "job_id_idx"]


def test_empty_shell_is_not_classified_as_an_index_or_fragment_problem():
    report = build_partition_report(
        7,
        _status(rows=0, fragments=0, small_fragments=0, coverage=[]),
        {"id_idx"},
    )

    assert report.state == "empty_shell"
    assert report.missing_indexes == ("id_idx",)


def test_unmaterialized_partition_has_no_physical_index_expectation():
    report = build_partition_report(
        94,
        SimpleNamespace(materialized=False, stats=None, coverage=[]),
        {"id_idx"},
    )

    assert report.state == "unmaterialized"
    assert report.materialized is False
    assert report.missing_indexes == ()


def test_resolve_partitions_validates_and_deduplicates_buckets():
    assert _resolve_partitions([34, 7, 34], False, 128) == [7, 34]
    assert _resolve_partitions([], True, 3) == [0, 1, 2]

    with pytest.raises(ValueError, match="outside valid range"):
        _resolve_partitions([128], False, 128)


def test_expected_indexes_reject_unknown_table():
    with pytest.raises(ValueError, match="Unsupported table"):
        expected_indexes_for_table("archive")
