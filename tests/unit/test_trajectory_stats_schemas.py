import pyarrow as pa

from wt_sdk.config import (
    DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE,
    DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE,
)
from wt_sdk.trajectory_stats.schemas import (
    CATEGORY_TRAJECTORY_STATS_SCHEMA,
    CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS,
    METRIC_NAMES,
    SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN,
    SESSION_TRAJECTORY_METRICS_PARTITION_TYPE,
    SESSION_TRAJECTORY_METRICS_PARTITIONS,
    SESSION_TRAJECTORY_METRICS_SCALAR_INDEX_COLUMNS,
    SESSION_TRAJECTORY_METRICS_SCHEMA,
    SESSION_TRAJECTORY_METRICS_UPSERT_KEYS,
)


def test_session_metrics_schema_keys_partitioning_and_independent_metrics():
    assert DEFAULT_SESSION_TRAJECTORY_METRICS_TABLE == "wt_session_trajectory_metrics"
    assert SESSION_TRAJECTORY_METRICS_UPSERT_KEYS == ("job_id", "session_id")
    assert SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN == "job_id"
    assert SESSION_TRAJECTORY_METRICS_PARTITION_TYPE == "HASH"
    assert SESSION_TRAJECTORY_METRICS_PARTITIONS == 128
    assert SESSION_TRAJECTORY_METRICS_SCALAR_INDEX_COLUMNS == ("job_id", "session_id")

    assert not SESSION_TRAJECTORY_METRICS_SCHEMA.field("job_id").nullable
    assert not SESSION_TRAJECTORY_METRICS_SCHEMA.field("session_id").nullable
    assert "sub_job_id" not in SESSION_TRAJECTORY_METRICS_SCHEMA.names
    for metric in METRIC_NAMES:
        assert SESSION_TRAJECTORY_METRICS_SCHEMA.field(metric).nullable
        assert SESSION_TRAJECTORY_METRICS_SCHEMA.field(metric).type == pa.int64()
        assert not SESSION_TRAJECTORY_METRICS_SCHEMA.field(f"{metric}_status").nullable
        assert SESSION_TRAJECTORY_METRICS_SCHEMA.field(f"{metric}_reason").nullable


def test_category_stats_schema_has_all_aggregates_but_no_serving_row_metrics():
    assert DEFAULT_CATEGORY_TRAJECTORY_STATS_TABLE == "wt_category_trajectory_stats"
    assert CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS == (
        "dataset",
        "harness",
        "model",
        "task_type",
        "level",
    )
    for key in CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS:
        assert not CATEGORY_TRAJECTORY_STATS_SCHEMA.field(key).nullable

    for metric in METRIC_NAMES:
        for statistic in ("mean", "median"):
            field = CATEGORY_TRAJECTORY_STATS_SCHEMA.field(f"{metric}_{statistic}")
            assert field.type == pa.float64()
            assert field.nullable
        for statistic in ("max", "min"):
            field = CATEGORY_TRAJECTORY_STATS_SCHEMA.field(f"{metric}_{statistic}")
            assert field.type == pa.int64()
            assert field.nullable
        assert not CATEGORY_TRAJECTORY_STATS_SCHEMA.field(
            f"{metric}_valid_session_count"
        ).nullable
        assert not CATEGORY_TRAJECTORY_STATS_SCHEMA.field(
            f"{metric}_missing_session_count"
        ).nullable
        assert not CATEGORY_TRAJECTORY_STATS_SCHEMA.field(f"{metric}_coverage").nullable

    assert "delivered_session_count" in CATEGORY_TRAJECTORY_STATS_SCHEMA.names
    assert not {
        "success_count",
        "total_serving_count",
        "last_delivered_at",
    }.intersection(CATEGORY_TRAJECTORY_STATS_SCHEMA.names)
