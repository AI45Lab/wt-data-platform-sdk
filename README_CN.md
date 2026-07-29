# WT Data Platform SDK

<p align="center">
    中文 &nbsp; | &nbsp; <a href="README.md">English</a>
</p>

用于在 WT 数据平台上写入、查询和管理智能体轨迹数据的 Python SDK。所有数据库操作均通过 dldb 完成，由 dldb 在 LanceDB 之上管理逻辑分区。

## 安装

dldb 当前通过 `pyproject.toml` 中声明的代码仓库安装。

```bash
python -m pip install -e .
python -m pip install -e ".[dev]"  # 开发和测试依赖
```

## 集成前配置

SDK 会在创建客户端时读取进程环境变量。已有应用代码可以继续直接使用 `WTGatewayClient()`。

### 本地使用 .env

将 [.env.example](.env.example) 复制到集成 SDK 的服务中并命名为 `.env`，替换其中的占位值，同时确保真实配置文件不会进入版本控制。

```bash
# .env
WT_SDK_DB_URI=s3://your-dldb-bucket
WT_SDK_ENV_CONFIG_DB_URI=s3://your-env-config-bucket
WT_SDK_S3_ENDPOINT=http://your-s3-endpoint:8060
WT_SDK_S3_ALLOW_HTTP=true
AWS_ACCESS_KEY_ID=replace-with-your-access-key
AWS_SECRET_ACCESS_KEY=replace-with-your-secret-key
AWS_EC2_METADATA_DISABLED=true
WT_SDK_PROFILE=production
```

启动应用前加载配置：

```bash
set -a && source .env && set +a
python your_service.py
```

### 直接导出环境变量

在 CI、systemd、Kubernetes 或其他部署系统中，可以直接注入相同的环境变量：

```bash
export WT_SDK_DB_URI=s3://your-dldb-bucket
export WT_SDK_ENV_CONFIG_DB_URI=s3://your-env-config-bucket
export WT_SDK_S3_ENDPOINT=http://your-s3-endpoint:8060
export WT_SDK_S3_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=replace-with-your-access-key
export AWS_SECRET_ACCESS_KEY=replace-with-your-secret-key
export AWS_EC2_METADATA_DISABLED=true
export WT_SDK_PROFILE=production
python your_service.py
```

Docker Compose 可以复用同一个 `.env`：

```yaml
services:
  app:
    env_file:
      - .env
```

### 表环境

`WT_SDK_PROFILE` 用于选择默认逻辑表。显式传入的 `GatewayConfig` 配置以及 `search(table="...")` 等方法参数具有更高优先级。

| Profile | Landing 表 | Serving 表 |
| --- | --- | --- |
| `production` 或未配置 | `wind_tunnel_landing` | `wind_tunnel_serving` |
| `test` | `landing_test` | `serving_test` |

设置 `WT_SDK_PROFILE=test` 即可在不修改应用代码的情况下使用测试表。

`EnvConfigManager` 使用独立的 `WT_SDK_ENV_CONFIG_DB_URI` 数据库访问
`evaluation_env_config`，不受 `WT_SDK_PROFILE` 影响。上面的 endpoint 和 AWS
凭证由两个数据库共用。显式传给 `EnvConfigManager` 的 `db_uri=` 具有更高优先级。

## 快速开始

Landing 表使用 128 个 bucket 的 `HASH(job_id)` 分区。读取和更新 landing 数据时应包含 `job_id`，使 SDK 能够剪枝到单个物理 bucket。

```python
import time

from wt_sdk import ChatMessage, ContentItem, LandingRecord, WTGatewayClient

record = LandingRecord(
    id="trajectory-step-001",
    dataset_type="RL",
    job_id="job-001",
    session_id="session-001",
    step_id=1,
    created_at=int(time.time()),
    is_terminal=False,
    messages=[ChatMessage(role="user", content=[ContentItem(type="text", text="Solve the task")])],
    response=ChatMessage(role="assistant", content=[ContentItem(type="text", text="Working on it")]),
)

with WTGatewayClient() as client:
    client.ingest_landing(record)
    steps = client.query_landing(
        "job_id = 'job-001' AND session_id = 'session-001'",
        order_by="step_id",
    )
```

`id` 和 `created_at` 由调用方提供。`dt` 和 `blob_manifest` 在记录转换过程中生成。同一个 session 内的 `step_id` 应保持唯一且单调递增。

## 客户端接口

### Landing 数据

| 方法 | 用途 |
| --- | --- |
| `ingest_landing(record)` | 写入一条 `LandingRecord`。 |
| `ingest_landing_batch(records)` | 写入记录列表或 `LandingRecordBatch`。 |
| `query_landing(filter_query, ...)` | 查询 landing 记录；设置 `as_dataframe=True` 时返回 DataFrame。 |
| `update_landing(filter_query, updates, ...)` | 更新匹配记录。`id`、`created_at` 和 `job_id` 为受保护字段。 |
| `count_landing(partition=None)` | 统计行数，可选指定原始 `job_id` 或 hash bucket。 |
| `delete_landing(filter_query)` | 删除匹配的 landing 记录。 |

为兼容已有调用方式，`query_landing()` 和 `update_landing()` 接受原始的 `partition="job-id"`。在 HASH 表上，SDK 会将其转换为 bucket 并补充 `job_id` 条件。仍建议在 `filter_query` 中显式包含 `job_id`。

执行带排序或 limit 的 landing 查询时，应在 `filter_query` 中包含 `job_id`。在当前 dldb 分区模型中，跨 bucket 的 `order_by + limit` 不是全局归并排序。

```python
with WTGatewayClient() as client:
    latest = client.query_landing(
        "job_id = 'job-001' AND session_id = 'session-001'",
        order_by="step_id",
        ascending=False,
        limit=1,
    )
    result = client.update_landing(
        "job_id = 'job-001' AND session_id = 'session-001' AND step_id = 1",
        {"is_terminal": True, "is_trainable": True},
    )
```

`update_landing()` 返回执行确认。dldb 目前不会返回跨逻辑分区精确汇总的匹配行数或更新行数。

### Serving、搜索和分页

| 方法 | 用途 |
| --- | --- |
| `ingest_serving(record)` / `ingest_serving_batch(records)` | 写入处理后的 serving 记录。 |
| `count_serving(partition=None)` / `delete_serving(filter_query)` | 对 serving 数据执行统计或删除。 |
| `search(query, ...)` | 类 SQL 关键词搜索，默认查询 serving 表。 |
| `get_tags_distribution()` | 返回 serving 标签频次。 |
| `get_by_id(record_id)` | 先在 serving 表、再在 landing 表中查找 ID。 |
| `pull_data(...)` / `fetch_data(...)` | 使用游标分页或分批读取 landing 数据。 |
| `get_max_created_at(where_sql)` / `extract_cursor(df)` | 构建基于游标的读取流程。 |

dldb 当前尚未开放向量搜索。`search()` 接受关键词查询；设置 `stream=True` 时，会返回包含当前结果 DataFrame 的迭代器。

### 索引维护

Landing 标量索引配置在 `dataset_type`、`is_terminal` 和 `is_trainable` 字段上。索引维护不应放入同步写入链路：

```python
with WTGatewayClient() as client:
    client.maintain_landing_indexes(all_partitions=True)
```

`maintain_landing_indexes()` 会为各 bucket 创建缺失索引，并可选执行 dldb optimize。

## 时延与指标

```bash
WT_SDK_DLDB_MODEL=metrics
WT_SDK_LOG_DLDB_TIMING=1
WT_SDK_DLDB_METRICS_LOG=./wt_metrics_log.jsonl
```

在 metrics 模式下，`client.close()` 会返回 dldb session 汇总，并将单次调用事件和汇总事件追加到配置的 JSONL 文件中。

## 运行测试

### 单元测试

单元测试使用 fake，不会连接 S3 或 dldb：

```bash
pytest -q
```

时延测试会将便于阅读的 JSONL 示例写入被忽略的
`tests/artifacts/metrics_log.txt`。

### 真实 DLDB/S3 集成测试

集成测试会向现有 `landing_test` 表写入少量数据，并在结束后清理。测试始终使用 `landing_test`，不受 `WT_SDK_PROFILE` 影响；数据库由 `WT_SDK_DB_URI` 选择。该表必须使用当前的 `HASH(job_id)` schema。

```bash
set -a && source .env && set +a
WT_SDK_RUN_INTEGRATION=1 python -m pytest -q tests/integration
```

`WT_SDK_RUN_INTEGRATION=1` 是 pytest 的安全开关，不是 SDK 运行时配置。

## 运维脚本

执行脚本前加载同一个 `.env`：

```bash
set -a && source .env && set +a
```

### 表管理

```bash
# 列出逻辑表
python scripts/ops/table_manager.py list

# 列出另一个 dldb 数据库中的逻辑表
python scripts/ops/table_manager.py list --db-uri s3://my-dldb-bucket

# 查看 schema、分区元数据和标量索引
python scripts/ops/table_manager.py show-schema wind_tunnel_landing

# 列出一个逻辑表背后的 dldb/Lance 物理表
python scripts/ops/table_manager.py show-physical landing_test

# 查看独立的环境配置表
python scripts/ops/table_manager.py show-schema evaluation_env_config \
  --db-uri "$WT_SDK_ENV_CONFIG_DB_URI"

# 环境配置表初始化具有破坏性；请先预览或显式确认
python scripts/ops/init_evaluation_env_table.py --dry-run
python scripts/ops/init_evaluation_env_table.py --confirm-recreate

# 交互式删除：输入完整表名，然后输入 DROP
python scripts/ops/table_manager.py drop landing_test

# 非交互式删除：再次传入完全相同的目标表名
python scripts/ops/table_manager.py drop landing_test \
  --force --confirm-table landing_test

# 删除一个 VALUE 分区（使用相同的两次确认机制）
python scripts/ops/table_manager.py drop serving_test --partition SFT
```

### 查询与数据检查

```bash
# 统计行数
python scripts/inspect/query_data.py --table wind_tunnel_landing --count

# 查询指定列
python scripts/inspect/query_data.py --table landing_test \
  --query "job_id = 'job-001'" --columns "id,session_id,step_id,is_terminal"

# 查看嵌套字段并关闭显示截断
python scripts/inspect/query_data.py --table landing_test --limit 1 \
  --show-nested --no-truncate

# 按分区对比预期标量索引和现有标量索引
python scripts/inspect/show_table_indexes.py landing_test

# 扫描逻辑表中的重复 ID
python scripts/inspect/scan_duplicate_id.py --table landing_test --max-output 100

# 定位包含无法读取嵌套字段的 HASH bucket 和候选记录
python scripts/inspect/scan_landing_nested_decode.py --table landing_test

# 查看 serving 标签或 serving 文本搜索行为
python scripts/inspect/get_unique_tags.py --table wind_tunnel_serving
python scripts/inspect/check_search_text.py --table wind_tunnel_serving
```

### 数据清理与索引

```bash
# 删除前预览匹配的 landing 数据
python scripts/ops/cleanup_data.py --table landing_test \
  --query "job_id = 'job-001'" --dry-run

# 删除匹配的测试数据
python scripts/ops/cleanup_data.py --table landing_test \
  --query "job_id = 'job-001'"

# 创建缺失索引并 optimize 所有已有 landing bucket
python scripts/ops/maintain_landing_indexes.py \
  --table wind_tunnel_landing --all-partitions

# 为逻辑表执行通用的缺失索引维护
python scripts/ops/add_missing_indexes.py wind_tunnel_serving
```

`scripts/dev` 包含可随时重建的测试表配置脚本。`scripts/migrations` 包含已经执行完成的历史迁移，不属于日常初始化流程。完整目录说明见 [scripts/README.md](scripts/README.md)。

## 仓库结构

```text
wt_sdk/                 对外 SDK 包
scripts/ops/            运维命令
scripts/inspect/        只读诊断工具
scripts/dev/            可随时重建的测试表配置
scripts/migrations/     历史一次性迁移
tests/unit/             默认的隔离单元测试
tests/integration/      显式运行的真实 DLDB/S3 测试
```

## 许可证

MIT
