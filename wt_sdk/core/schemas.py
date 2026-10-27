import pyarrow as pa

# JSON payload columns are opaque UTF-8 JSON strings stored as large_string
# (64-bit offsets, no 2GB per-column offset limit). Their document shape is
# intentionally not validated by the SDK; the Python/Pandas boundary stays a
# plain str and callers serialize with json.dumps(). deserialize_json decodes
# them by column name (JSON_STRING_COLUMNS below), never by Arrow type.
JSON_STRING_TYPE = pa.large_string()

# ==============================================================================
# 2. 基础字段集
#    两张表共有的字段，避免重复定义
# ==============================================================================

BASE_FIELDS = [
    # --- 分区键 ---
    # 目前lanceDB似乎不支持partition，用scaler index代替
    pa.field('dataset_type', pa.string(), nullable=False),
    pa.field('dt', pa.string(), nullable=False),

    # --- 基础信息  ---
    pa.field('id', pa.string(), nullable=False),
    pa.field('session_id', pa.string(), nullable=True),  # Optional for dataset types like PreTrain
    pa.field('created_at', pa.int64(), nullable=False),
    pa.field('source_updated_at', pa.int64(), nullable=False),
    pa.field('serving_updated_at', pa.int64(), nullable=True),

    # --- RL 核心  ---
    pa.field('step_id', pa.int32()),
    pa.field('is_terminal', pa.bool_()),
    # 2026.3.3 added
    pa.field('env_id', pa.string(), nullable=True),
    pa.field('job_id', pa.string(), nullable=True),
    # 2026.10 job splitting: sub_job_id is the internal write shard id.
    # job_id remains the full, user-visible task id everywhere.
    # Non-split tasks: sub_job_id = job_id (self-reference convention).
    # Split tasks: sub_job_id = "{job_id}#{random suffix}", unique per shard.
    pa.field('sub_job_id', pa.string(), nullable=True),
    pa.field('is_truncated', pa.bool_(), nullable=True),
    
    # --- 评分  ---
    pa.field('step_reward', pa.float32()),
    pa.field('reward', pa.float32()),

    # --- Payload  ---
    pa.field('messages', JSON_STRING_TYPE),
    pa.field('response', JSON_STRING_TYPE),
    pa.field('chosen_trace', JSON_STRING_TYPE),
    pa.field('rejected_trace', JSON_STRING_TYPE),

    # --- 答案与文本  ---
    pa.field('ground_truth_answer', pa.string()),
    pa.field('reference_answer', pa.string()),
    pa.field('search_text', pa.string()),       # ETL 聚合的全文检索字段

    # --- Meta 信息  ---
    pa.field('agent_model', pa.string()),
    pa.field('env_name', pa.string()),
    pa.field('is_session_completed', pa.bool_()),
    pa.field('is_trainable', pa.bool_()),
    pa.field('meta_json', JSON_STRING_TYPE), # 兜底，Python API 仍使用 JSON 字符串
    pa.field('tags', pa.list_(pa.string())),

    # --- 资产清单  ---
    # Ingest 阶段生成，Landing表也需要它来支持训练平台的预加载
    pa.field('blob_manifest', pa.list_(pa.string())),

    # --- 2026.10 schema evolution v2 新增字段（全部 nullable）---
    # 网关侧指标（请求耗时、token 用量等）
    pa.field('gateway_metrics', JSON_STRING_TYPE),
    # 展示名：由写入方填写，一般等于 job_id。job_id 是分区键不可变，
    # dashboard 改名走 job_name。
    pa.field('job_name', pa.string(), nullable=True),
    # 评测信息
    pa.field('eval', JSON_STRING_TYPE),
    # 数据集信息
    pa.field('dataset', JSON_STRING_TYPE),
    # 角色（如 agent / env / user 等）
    pa.field('role', pa.string(), nullable=True),
    # 执行框架标识
    pa.field('harness', pa.string(), nullable=True),
    # 训练效果（仅 serving 表写入）
    pa.field('training_results', JSON_STRING_TYPE),
    # 轨迹分析结果（仅 serving 表写入）
    pa.field('analysis', JSON_STRING_TYPE),
    # 权重版本
    pa.field('weight_version', pa.int64(), nullable=True),
    # 分组 id（一次 rollout 的组，如 group-RL / best-of-n）
    pa.field('group_id', pa.string(), nullable=True),
    # token 统计大 JSON
    pa.field('tokens_info', JSON_STRING_TYPE),
]

# ==============================================================================
# 3. 最终 Schema 定义
# ==============================================================================

# --- Landing Table  ---
# 仅包含基础字段
LANDING_SCHEMA = pa.schema(BASE_FIELDS)

# --- Serving Table  ---
# 与 Landing Table 使用完全相同的字段定义
SERVING_SCHEMA = LANDING_SCHEMA

# 所有大 JSON payload 列（存储为 large_string 的不透明 JSON 字符串）。
# deserialize 按列名匹配（不依赖 Arrow 类型），converters / 运维脚本统一引用此处。
JSON_STRING_COLUMNS = (
    'messages',
    'response',
    'chosen_trace',
    'rejected_trace',
    'meta_json',
    'gateway_metrics',
    'eval',
    'dataset',
    'training_results',
    'analysis',
    'tokens_info',
)

# ==============================================================================
# 4. Partition Definitions
# ==============================================================================

# Landing table: partition by sub_job_id (HASH partition).
# 2026.10 job splitting: workers of a split job write disjoint sub_job_ids,
# so concurrent writers land in different physical buckets and no longer
# contend on the same commit. job_id stays queryable via scalar index.
LANDING_PARTITION_COLUMN = "sub_job_id"
LANDING_PARTITION_TYPE = "HASH"
LANDING_PARTITIONS = 128

# Serving table: partition by job_id (HASH partition)
SERVING_PARTITION_COLUMN = "job_id"
SERVING_PARTITION_TYPE = "HASH"
SERVING_PARTITIONS = 128

# ==============================================================================
# 5. Scalar Index Definitions
#    Define which columns should have scalar indexes for query performance
#    Note: job_id仍需要索引，用于HASH bucket内处理碰撞后的精确过滤。
# ==============================================================================

# Scalar indexes for landing table
LANDING_SCALAR_INDEXES = [
    ("id", "BTREE"),
    ("job_id", "BTREE"),
    ("sub_job_id", "BTREE"),
    ("session_id", "BTREE"),
    ("created_at", "BTREE"),
    ("source_updated_at", "BTREE"),
    ("is_terminal", "BITMAP"),
    ("is_trainable", "BITMAP"),
    ("is_session_completed", "BITMAP"),
    ("group_id", "BTREE"),
]

# Scalar indexes for serving table
SERVING_SCALAR_INDEXES = [
    ("id", "BTREE"),
    ("job_id", "BTREE"),
    ("session_id", "BTREE"),
    ("created_at", "BTREE"),
    ("source_updated_at", "BTREE"),
    ("serving_updated_at", "BTREE"),
    ("dataset_type", "BITMAP"),
    ("is_terminal", "BITMAP"),
    ("is_trainable", "BITMAP"),
    ("is_session_completed", "BITMAP"),
    ("step_reward", "BTREE"),
    ("reward", "BTREE"),
    ("agent_model", "BTREE"),
    ("env_name", "BTREE"),
    ("tags", "LABEL_LIST"),
    ("group_id", "BTREE"),
]

# search_text 使用包含匹配（LIKE '%keyword%'），普通 BTREE 无法有效加速。
# 等 dldb 暴露 LanceDB FTS 后，再为该字段增加全文索引。
