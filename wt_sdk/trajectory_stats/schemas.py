"""Arrow schemas for production trajectory-statistics tables."""

import pyarrow as pa


METRIC_NAMES = ("total_tokens", "total_steps", "tool_calls")

CLASSIFICATION_STATUS_VALID = "VALID"
CLASSIFICATION_STATUS_INVALID_JOB_ID = "INVALID_JOB_ID"

METRIC_STATUS_VALID = "VALID"
METRIC_STATUS_MISSING = "MISSING"
METRIC_STATUS_INCOMPLETE = "INCOMPLETE"
METRIC_STATUSES = frozenset(
    {METRIC_STATUS_VALID, METRIC_STATUS_MISSING, METRIC_STATUS_INCOMPLETE}
)

SESSION_TRAJECTORY_METRICS_UPSERT_KEYS = ("job_id", "session_id")
SESSION_TRAJECTORY_METRICS_PARTITION_COLUMN = "job_id"
SESSION_TRAJECTORY_METRICS_PARTITION_TYPE = "HASH"
SESSION_TRAJECTORY_METRICS_PARTITIONS = 128
SESSION_TRAJECTORY_METRICS_SCALAR_INDEX_COLUMNS = ("job_id", "session_id")

CATEGORY_TRAJECTORY_STATS_UPSERT_KEYS = (
    "dataset",
    "harness",
    "model",
    "task_type",
    "level",
)


SESSION_TRAJECTORY_METRICS_SCHEMA = pa.schema(
    [
        pa.field("job_id", pa.string(), nullable=False),
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("classification_status", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=True),
        pa.field("harness", pa.string(), nullable=True),
        pa.field("model", pa.string(), nullable=True),
        pa.field("task_type", pa.string(), nullable=True),
        pa.field("level", pa.string(), nullable=False),
        *[
            field
            for metric in METRIC_NAMES
            for field in (
                pa.field(metric, pa.int64(), nullable=True),
                pa.field(f"{metric}_status", pa.string(), nullable=False),
                pa.field(f"{metric}_reason", pa.string(), nullable=True),
            )
        ],
        pa.field("updated_at_ms", pa.int64(), nullable=False),
    ]
)


CATEGORY_TRAJECTORY_STATS_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("harness", pa.string(), nullable=False),
        pa.field("model", pa.string(), nullable=False),
        pa.field("task_type", pa.string(), nullable=False),
        pa.field("level", pa.string(), nullable=False),
        pa.field("delivered_session_count", pa.int64(), nullable=False),
        *[
            field
            for metric in METRIC_NAMES
            for field in (
                pa.field(f"{metric}_mean", pa.float64(), nullable=True),
                pa.field(f"{metric}_median", pa.float64(), nullable=True),
                pa.field(f"{metric}_max", pa.int64(), nullable=True),
                pa.field(f"{metric}_min", pa.int64(), nullable=True),
                pa.field(f"{metric}_valid_session_count", pa.int64(), nullable=False),
                pa.field(f"{metric}_missing_session_count", pa.int64(), nullable=False),
                pa.field(f"{metric}_coverage", pa.float64(), nullable=False),
            )
        ],
        pa.field("updated_at_ms", pa.int64(), nullable=False),
    ]
)
