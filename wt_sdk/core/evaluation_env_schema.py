"""
Evaluation Environment Config Schema.

Defines the shared schema for the production ``evaluation_env_config`` and
test ``env_config_test`` tables stored in LanceDB.

Schema matches the original SQLite schema from evaluation platform:
- id: Auto-increment ID
- env_id: UUID for unique environment identification
- job_id: Job identifier for grouping related environments
- group_id: Group ID for GRPO aggregation (RL scenarios)
- finished: Whether environment is completed
- env_name: Environment name (must match registered env name)
- env_params: User-defined parameters (JSON)
- image: Docker image for the environment
- created_at: Creation timestamp
- sub_job_ids: Write shard ids for split jobs (JSON string array).
  Written once at task creation by the job creator, read-only afterwards.
  Empty/None for non-split jobs.
"""
import pyarrow as pa


# Evaluation Environment Config Schema
EVALUATION_ENV_SCHEMA = pa.schema([
    # Primary key - auto-increment (managed by application, not LanceDB)
    pa.field('id', pa.int64(), nullable=False),

    # Unique environment UUID
    pa.field('env_id', pa.string(), nullable=False),

    pa.field('job_id', pa.string(), nullable=False),

    # Group ID for GRPO aggregation (RL scenarios)
    pa.field('group_id', pa.string(), nullable=True),

    # Completion status
    pa.field('finished', pa.bool_(), nullable=False),

    # Environment name (must match registered env name)
    pa.field('env_name', pa.string(), nullable=False),

    # User-defined parameters (JSON string)
    pa.field('env_params', pa.string(), nullable=True),

    # Docker image for the environment
    pa.field('image', pa.string(), nullable=True),

    # Creation timestamp (Unix timestamp)
    pa.field('created_at', pa.int64(), nullable=False),

    # 2026.10 job splitting: list of write shard ids, stored as a JSON
    # string array (same storage pattern as env_params). Only rows of split
    # jobs carry a value; written once at task creation, never updated.
    pa.field('sub_job_ids', pa.string(), nullable=True),
])


# 2026.10 job splitting: partition the env table by job_id (HASH).
# Every env read path filters by job_id first, so this enables partition
# pruning for the SDK's job_id -> sub_job_ids resolution lookups.
EVALUATION_ENV_PARTITION_COLUMN = "job_id"
EVALUATION_ENV_PARTITION_TYPE = "HASH"
EVALUATION_ENV_PARTITIONS = 128


# Index configuration for the table
# Scalar indexes should be created on these columns for query performance
SCALAR_INDEX_COLUMNS = [
    "env_name",   # For filtering by environment name
    "env_id",     # For lookups by environment ID
    "job_id",     # For job-scoped config reads and cleanup
    "group_id",   # For grouping queries (GRPO)
    "finished",   # For filtering by completion status
]
