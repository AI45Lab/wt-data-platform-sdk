import pyarrow as pa

# JSON extension columns use UTF-8 strings at the Python/Pandas boundary.
# Their document shape is intentionally not validated by the SDK.
JSON_TYPE = pa.json_(pa.string())

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

    # --- RL 核心  ---
    pa.field('step_id', pa.int32()),
    pa.field('is_terminal', pa.bool_()),
    # 2026.3.3 added
    pa.field('env_id', pa.string(), nullable=True),
    pa.field('job_id', pa.string(), nullable=True),
    pa.field('is_truncated', pa.bool_(), nullable=True),
    
    # --- 评分  ---
    pa.field('step_reward', pa.float32()),
    pa.field('reward', pa.float32()),

    # --- Payload  ---
    pa.field('messages', JSON_TYPE),
    pa.field('response', JSON_TYPE),
    pa.field('chosen_trace', JSON_TYPE),
    pa.field('rejected_trace', JSON_TYPE),
    
    # --- 答案与文本  ---
    pa.field('ground_truth_answer', pa.string()),
    pa.field('reference_answer', pa.string()),
    pa.field('search_text', pa.string()),       # ETL 聚合的全文检索字段

    # --- Meta 信息  ---
    pa.field('agent_model', pa.string()),
    pa.field('env_name', pa.string()),
    pa.field('is_session_completed', pa.bool_()),
    pa.field('is_trainable', pa.bool_()),
    pa.field('meta_json', JSON_TYPE), # 兜底，Python API 仍使用 JSON 字符串
    pa.field('tags', pa.list_(pa.string())),

    # --- 资产清单  ---
    # Ingest 阶段生成，Landing表也需要它来支持训练平台的预加载
    pa.field('blob_manifest', pa.list_(pa.string()))
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

# ==============================================================================
# 4. Partition Definitions
# ==============================================================================

# Landing table: partition by job_id (HASH partition)
LANDING_PARTITION_COLUMN = "job_id"
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
    ("session_id", "BTREE"),
    ("created_at", "BTREE"),
    ("is_terminal", "BITMAP"),
    ("is_trainable", "BITMAP"),
]

# Scalar indexes for serving table
SERVING_SCALAR_INDEXES = [
    ("id", "BTREE"),
    ("job_id", "BTREE"),
    ("session_id", "BTREE"),
    ("created_at", "BTREE"),
    ("dataset_type", "BITMAP"),
    ("is_terminal", "BITMAP"),
    ("is_trainable", "BITMAP"),
    ("step_reward", "BTREE"),
    ("reward", "BTREE"),
    ("agent_model", "BTREE"),
    ("env_name", "BTREE"),
    ("tags", "LABEL_LIST"),
]

# search_text 使用包含匹配（LIKE '%keyword%'），普通 BTREE 无法有效加速。
# 等 dldb 暴露 LanceDB FTS 后，再为该字段增加全文索引。
