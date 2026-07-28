"""
Evaluation Environment Config Schema.

Defines the schema for evaluation_env_config table stored in LanceDB.
This table stores environment configurations for the evaluation platform.

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
])


# Index configuration for the table
# Scalar indexes should be created on these columns for query performance
SCALAR_INDEX_COLUMNS = [
    "env_name",   # For filtering by environment name
    "env_id",     # For lookups by environment ID
    "group_id",   # For grouping queries (GRPO)
    "finished",   # For filtering by completion status
]
