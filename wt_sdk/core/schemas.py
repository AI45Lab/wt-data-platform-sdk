import pyarrow as pa

# ==============================================================================
# 1. 公共子结构定义
#    保证 Landing 和 Serving 对复杂对象的定义一致
# ==============================================================================

# --- 1.1 多模态内容单元 ---
# 包含文本、S3引用(图片/音频)以及直接存储的小二进制数据
content_item_type = pa.struct([
    pa.field('type', pa.string()),          # 枚举: text, image_url, input_audio
    pa.field('text', pa.string()),          # 文本内容
    
    # 图片引用 (OpenAI Compatible)
    pa.field('image_url', pa.struct([
        pa.field('url', pa.string()),       # s3://...
        pa.field('detail', pa.string())     # auto, low, high
    ])),
    
    # 音频引用 (OpenAI Compatible)
    pa.field('input_audio', pa.struct([
        pa.field('url', pa.string()),       # s3://...
        pa.field('format', pa.string())     # wav, mp3
    ])),
    
    # 直接存储的小文件 (保留字段)
    pa.field('media_type', pa.string()),    # e.g., image/png
    pa.field('image_bytes', pa.binary())    # 存 icon 或 embedding bytes
])

# --- 1.2 工具调用 ---
# OpenAI Function Calling 标准结构
tool_call_type = pa.struct([
    pa.field('id', pa.string()),
    pa.field('type', pa.string()),          # usually 'function'
    pa.field('function', pa.struct([        # 嵌套 struct
        pa.field('name', pa.string()),
        pa.field('arguments', pa.string())  # JSON string
    ]))
])

# --- 1.3 消息体 ---
# 聊天记录的核心单元
message_type = pa.struct([
    pa.field('role', pa.string()),
    pa.field('content', pa.list_(content_item_type)), # 核心内容列表
    pa.field('name', pa.string()),
    pa.field('refusal', pa.string()),                 # 拒答内容
    pa.field('tool_calls', pa.list_(tool_call_type)), # 工具调用列表
    pa.field('tool_call_id', pa.string())             # Tool 回传 ID
])

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
    pa.field('messages', pa.list_(message_type)),
    pa.field('response', message_type),
    pa.field('chosen_response', message_type),
    pa.field('rejected_response', message_type),
    
    # --- 答案与文本  ---
    pa.field('ground_truth_answer', pa.string()),
    pa.field('reference_answer', pa.string()),

    # --- Meta 信息  ---
    pa.field('agent_model', pa.string()),
    pa.field('env_name', pa.string()),
    pa.field('is_session_completed', pa.bool_()),
    pa.field('is_trainable', pa.bool_()),
    pa.field('meta_json', pa.string()), # 兜底

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
# 包含基础字段 + 增值字段
SERVING_SCHEMA = pa.schema(BASE_FIELDS + [
    # 搜索增强
    pa.field('search_text', pa.string()),       # 全文检索聚合字段
    pa.field('tags', pa.list_(pa.string())),    # 业务标签

    # 向量能力
    # 1536维 float32 向量 (适配 OpenAI embedding)
    pa.field('instruction_vector', pa.list_(pa.float32(), 1536)),

    # 超大向量/矩阵存储路径 (ETL 产物)
    pa.field('vector_file_path', pa.string())
])

# ==============================================================================
# 4. Partition Definitions
# ==============================================================================

# Landing table: partition by job_id (HASH partition)
LANDING_PARTITION_COLUMN = "job_id"
LANDING_PARTITION_TYPE = "HASH"
LANDING_PARTITIONS = 128

# Serving table: partition by dataset_type (VALUE partition)
SERVING_PARTITION_COLUMN = "dataset_type"
SERVING_PARTITION_TYPE = "VALUE"

# ==============================================================================
# 5. Scalar Index Definitions
#    Define which columns should have scalar indexes for query performance
#    Note: partition key (job_id) does not need a separate index.
# ==============================================================================

# Scalar indexes for landing table
LANDING_SCALAR_INDEXES = [
    ("dataset_type", "BTREE"),
    ("is_terminal", "BTREE"),
    ("is_trainable", "BTREE"),
]

# Scalar indexes for serving table
SERVING_SCALAR_INDEXES = [
    ("id", "BTREE"),
    ("session_id", "BTREE"),
    ("created_at", "BTREE"),
    ("step_reward", "BTREE"),
    ("reward", "BTREE"),
    ("agent_model", "BTREE"),
    ("env_name", "BTREE"),
]

# TODO: Add FTS (Full-Text Search) index for 'search_text' field
#       when dldb/LanceDB supports it. This will enable efficient
#       full-text search across message content.
#
# TODO: Add vector index for 'instruction_vector' field
#       when dldb/LanceDB supports it. This will enable efficient
#       vector similarity search for semantic retrieval.
