# WT Data Platform SDK

<p align="center">
    中文 &nbsp; | &nbsp; <a href="README.md">English</a>
</p>

用于在 WT 数据平台上写入、查询和管理智能体轨迹数据的 Python SDK。所有数据库操作均通过 dldb 完成，由 dldb 在 LanceDB 之上管理逻辑分区。

## 安装

dldb 当前通过 `pyproject.toml` 中声明的公开仓库
[DeepLink-org/Persisting](https://github.com/DeepLink-org/Persisting) 安装。
当前支持 Python 3.10 至 3.12。

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
WT_SDK_PROFILE=test
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
export WT_SDK_PROFILE=test
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

| Profile | Landing 表 | Serving 表 | ETL checkpoint 表（仅运行 ETL 时使用） |
| --- | --- | --- | --- |
| `test` 或未配置 | `landing_test` | `serving_test` | `etl_checkpoints_test` |
| `production` 或 `prod` | `wind_tunnel_landing` | `wind_tunnel_serving` | `wind_tunnel_etl_checkpoints` |

未配置 `WT_SDK_PROFILE` 时会安全地默认使用 `test`；访问生产表必须显式配置
`production` 或 `prod`。

checkpoint 表位于独立的 ETL database 中。只有初始化 checkpoint 表和运行默认增量
ETL 时，才必须配置 `WT_SDK_ETL_STATE_DB_URI=s3://wind-tunnel-etl`，或向 ETL 命令传入
`--state-db-uri`。普通 SDK client、上游写入服务以及按 job/session/时间/filter 定向执行的
手动 ETL 都不要求、也不会读取该配置；缺少它不会影响 `WT_SDK_PROFILE` 的解析。只有实际
使用 ETL 状态库时，同一个 profile 才会选择上表中的 checkpoint 表。

`EnvConfigManager` 使用独立的 `WT_SDK_ENV_CONFIG_DB_URI` 数据库访问
`evaluation_env_config`，不受 `WT_SDK_PROFILE` 影响。上面的 endpoint 和 AWS
凭证由两个数据库共用。显式传给 `EnvConfigManager` 的 `db_uri=` 具有更高优先级。

## 端到端最佳实践

下面用一次 agent evaluation job 串起原始轨迹写入、训练消费和 serving
检索。数据库凭证和表环境通过环境变量加载；每个进程内部应复用同一个
client。

### 1. 写入并完成一条轨迹

必须立即落库的事件使用 `ingest_landing()`，已经在内存中缓冲的事件使用
`ingest_landing_batch()`。`id` 和 `created_at` 由上游生成，同一 session
中的 `step_id` 应唯一且单调递增。

```python
import json
import time
import uuid

from wt_sdk import LandingRecord, WTGatewayClient


def message(role: str, text: str) -> dict:
    return {"role": role, "content": text}


job_id = "evaluation-run-001"       # 一次评测运行，也是 HASH 分区键
session_id = str(uuid.uuid4())      # 一条 agent 轨迹
created_at = int(time.time())


def make_record(step_id: int, terminal: bool = False) -> LandingRecord:
    return LandingRecord(
        id=f"{session_id}:{step_id}",  # 由调用方生成且全局唯一
        dataset_type="RL",
        job_id=job_id,
        session_id=session_id,
        step_id=step_id,
        created_at=created_at + step_id,
        is_terminal=terminal,
        is_session_completed=terminal,
        # 轨迹 payload 列在 SDK 边界使用不透明 JSON 字符串。
        messages=json.dumps([message("user", f"task input for step {step_id}")]),
        response=json.dumps(message("assistant", f"result for step {step_id}")),
        meta_json=json.dumps(
            {"task_id": "benchmark-task-42", "group_id": "group-a"}
        ),
    )


first = make_record(1)
buffered = [make_record(2), make_record(3, terminal=True)]
scope = f"job_id = '{job_id}' AND session_id = '{session_id}'"

with WTGatewayClient() as client:
    client.ingest_landing(first)             # 低时延单条写入
    client.ingest_landing_batch(buffered)    # 更高吞吐的批量写入

    # reward 可能在 terminal 事件落库后才返回。
    client.update_landing(
        f"{scope} AND step_id = 3",
        {"reward": 1.0, "step_reward": 1.0, "is_trainable": True},
    )  # SDK 自动刷新 source_updated_at。

    trajectory = client.query_data(
        scope,
        order_by="step_id",
        checkout_latest=True,
        exclude_none=True,  # 默认省略每条结果中值为 null 的表列。
        deserialize_json=True,  # 将 JSON 列返回为 dict/list。
    )
    first_step_id = trajectory[0]["step_id"]  # query_data() 返回 List[dict]。
    job_row_count = client.count_landing(partition=job_id)

    # 同时知道 job_id 和 id 时，优先使用这种可剪枝查询。
    exact_event = client.query_data(
        f"job_id = '{job_id}' AND id = '{buffered[-1].id}'",
        limit=1,
    )
```

`query_data()` 返回普通字典，并默认省略值为 null 的表列；需要完整
schema 形状时传入 `exclude_none=False`。JSON 列默认保持为字符串，
`exclude_none` 不会修改其内容；传入 `deserialize_json=True` 可返回 Python
`dict/list`，且不会删除 JSON 内部的 null。

```python
deserialize_json=False  # LanceDB 原生返回：JSON 字符串（默认）。
deserialize_json=True   # SDK 调用 json.loads()：返回 Python dict/list。
```

landing 和 serving 的 `messages`、`response`、`chosen_trace`、
`rejected_trace` 和 `meta_json` 都使用 Arrow `json<string>`。调用方写入前需用
`json.dumps()` 序列化完整 JSON 文档。trace 是编码在一个字符串中的 JSON
数组，不再是 Arrow `list<struct>`。SDK 暂不校验 OpenAI 或其他 provider 的
内部结构；landing 可将 provider 原始数据保留在 `meta_json`，ETL 再向 serving
写入归一化的 OpenAI 风格 payload JSON。

### SDK 管理的时间字段

| 字段 | 单位 | 含义 |
| --- | --- | --- |
| `created_at` | 保持调用方现有单位 | 原始轨迹创建时间，写入后不可修改。 |
| `source_updated_at` | Unix epoch 毫秒 | 可能影响 ETL/serving 结果的 source 最后实质变化时间；model 自动初始化，landing update 默认刷新。 |
| `serving_updated_at` | Unix epoch 毫秒 | serving 最后一次成功 ingest/upsert 的发布时间；landing 中始终为 null。 |

回放或迁移时调用方可以显式提供 `source_updated_at`，否则
`LandingRecord`/`ServingRecord` 会自动初始化。serving 写入保留该 source 时间，
并在内部副本上生成新的 `serving_updated_at`，不会修改调用方传入的 model。

### 2. 消费已完成事件

持续运行的训练消费者每次使用 `pull_data()` 拉取一页，并且只在该页处理成功后
持久化新游标。`pull_data()` 会自动添加 `dataset_type` 条件；`where_sql` 中
仍应包含 `job_id`，以便进行 HASH 剪枝。下面的 `load_checkpoint()`、
`process_page()` 和 `save_checkpoint()` 代表消费方应用自己的逻辑；持久化
checkpoint 时应按消费者和查询/表范围进行隔离。

```python
job_filter = "job_id = 'evaluation-run-001' AND is_terminal = True"
page_size = 1000
stored_cursor = load_checkpoint()  # 没有 checkpoint 时返回 None。

with WTGatewayClient() as client:
    while True:
        page = client.pull_data(
            dataset_type="RL",
            where_sql=job_filter,
            cursor=stored_cursor,
            limit=page_size,
            checkout_latest=True,
            deserialize_json=True,
        )
        if page.empty:
            break

        process_page(page)
        next_cursor = client.extract_cursor(page)
        save_checkpoint(next_cursor)  # 仅在本页处理成功后持久化。
        stored_cursor = next_cursor

        if len(page) < page_size:
            break

    # 可选：检查当前拉取范围的消费水位。
    latest_record = client.get_max_created_at(
        where_sql=f"dataset_type = 'RL' AND {job_filter}",
    )
```

`get_max_created_at()` 是配合 `pull_data()` 使用的游标/水位辅助方法，并不是一种
独立的数据查询方式。它适合初始化、监控或故障恢复检查；正常推进分页时，仍应在
成功处理当前页后，通过 `extract_cursor(page)` 提取并持久化游标。

一次性扫描或回填时，可以使用 `iter_data_batches()` 自动维护 `created_at` 游标，并
惰性地逐批返回 DataFrame：

```python
with WTGatewayClient() as client:
    for batch in client.iter_data_batches(
        dataset_type="RL",
        where_sql="job_id = 'evaluation-run-001'",
        chunk_size=1000,
        deserialize_json=True,
    ):
        print(f"received {len(batch)} rows")
```

`pull_data()` 和 `iter_data_batches()` 默认查询 landing。外部消费方可传入
`table=client.config.tables.serving_table` 查询 serving。需要 Python payload 对象时
还应传入 `deserialize_json=True`，并按普通 `dict/list` 处理，不再依赖旧的
Arrow 嵌套容器。这两个接口都使用 `created_at` 游标；当多条记录可能共享
同一时间戳时，不应将它们作为正式离线导出的接口。

### 3. 发布增值数据

ETL 或训练筛选完成后，可以把增强后的记录发布到 serving。Landing 与 serving
使用相同 schema；尚未经过增值处理的字段在 landing 中保持 null。dldb 当前尚未
开放向量搜索。

```python
from wt_sdk import ServingRecord

serving_data = buffered[-1].model_dump()
serving_data.update(
    reward=1.0,
    step_reward=1.0,
    is_trainable=True,
    search_text="benchmark task final successful response",
    tags=["trainable", "successful"],
)
serving_record = ServingRecord(**serving_data)

with WTGatewayClient() as client:
    # ETL 发布应支持安全重试，避免 query-then-write 竞态。
    client.upsert_serving(serving_record)
    published = client.get_by_id(
        serving_record.id,
        exclude_none=True,
        deserialize_json=True,
    )
    assert published and published["id"] == serving_record.id

    matches = client.search(
        "successful",
        dataset_type="RL",
        tags=["trainable"],
        limit=20,
        deserialize_json=True,
    )
```

`get_by_id()` 默认查询 serving，也可以精确指定 `table`；它不会跨越内部/外部表边界
自动兜底。单独一个 ID 无法定位 HASH bucket，因此 landing 高频查询应优先使用同时
包含 `job_id` 和 `id` 的 `query_data()`。和 `query_data()` 一样，它返回普通字典，
默认省略 null 表列，但不会修改 JSON 字符串内容。

ETL 应通过 `upsert_serving()` 或 `upsert_serving_batch()` 发布完整记录。
它们直接调用 dldb 的 `columns=["id"]` upsert，不应自行实现“先查询、再决定
ingest/update”。重复 upsert 的业务内容最终一致，但 `serving_updated_at` 表示
最后一次成功发布时间，因此重试时允许变化。每条 upsert 记录都必须带非空且不可变
的 `job_id`。dldb HASH 表不提供跨 bucket 唯一约束，因此调用方必须保证 `id`
全局唯一，且已有 ID 不能迁移到另一个 `job_id`。保留 append/add 语义的
`ingest_serving(_batch)` 仍可使用，也会刷新 `serving_updated_at`。

仓库现已包含 ETL v1 引擎、按 HASH bucket 持久化的 checkpoint、手动 backfill 模式，
以及内置的 chosen-trace/tags stage。贡献者必须遵守
[`wt_sdk/etl/README.md`](wt_sdk/etl/README.md) 中的 stage contract 与接入规范；运维
入口为 [`scripts/etl/run.py`](scripts/etl/run.py)。

无需连接数据库即可列出当前已注册的 pipeline：

```bash
python scripts/etl/run.py --list-pipelines
```

serving 数据发布完成后，正式离线导出请使用 `export_data_batches()`。它默认查询
serving，在返回第一批数据之前先生成完整的唯一 ID 清单，随后按精确 ID 取数并逐批
校验。清单生成之后新增的记录不会混入本次导出；如果发现重复 ID，或者源记录被
删除/修改后不再满足条件，接口会直接失败，不会静默漏数：

`export_data_batches()` 要求 Python 运行时带有标准库 `sqlite3`
支持；无需安装额外的 SQLite 服务或 pip 依赖。

```python
with WTGatewayClient() as client:
    for batch in client.export_data_batches(
        filter_query="dataset_type = 'RL' AND is_trainable = True",
        batch_size=5000,
        columns=["id", "job_id", "chosen_trace", "tags", "meta_json"],
    ):
        write_to_temporary_export(batch)

    publish_completed_export()
```

导出迭代结束前，应将选中的 serving 记录视为不可变数据；只有迭代完整成功后，才
发布最终导出文件。dldb 目前不提供跨所有 HASH 物理 bucket 的单一原子快照，因此
该清单与校验协议可以固定行集合，但如果选中记录在导出过程中被原地更新，无法保留
更新前的字段值。

### 4. 维护增量索引

不要在同步写入链路中构建索引。一个 job 完成后，或由独立后台运维任务，仅刷新
该 job 触达的 bucket：

```python
with WTGatewayClient() as client:
    summary = client.maintain_table_indexes(
        "wind_tunnel_landing",
        partitions=["evaluation-run-001"],
    )
```

该方法会创建缺失的预设索引并执行 dldb optimize，使新增数据进入已有索引。
`all_partitions=True` 只用于定期全表维护，不应在每次写入后调用。context
manager 会负责关闭 dldb session，并在启用 metrics 时输出最终汇总。

### 数据读取接口如何选择

| 接口 | 接收参数 | 返回方式 | 分页控制 | 默认表 | 适用场景 |
| --- | --- | --- | --- | --- | --- |
| `query_data()` | `filter_query`、`limit`、`columns`、`partition`、`order_by`、`ascending`、`checkout_latest`、`table`、`exclude_none`、`deserialize_json` | 一次返回 `List[dict]` | 不维护游标；执行一次查询，可使用 `limit` | Landing | 交互式条件查询、轨迹/详情查询、Dashboard 列表、小规模有界结果集 |
| `get_by_id()` | `record_id`、`table`、`exclude_none`、`deserialize_json` | 返回一个 `dict` 或 `None` | 不分页；仅凭 ID 无法定位 HASH bucket，因此扫描所选表 | Serving | 只知道全局唯一 ID 时的低频单条查询 |
| `pull_data()` | `dataset_type`、`where_sql`、`start_time`、`end_time`、`cursor`、`order_by`、`ascending`、`limit`、`checkout_latest`、`table`、`deserialize_json` | 一次返回一页 DataFrame | 调用方传入、提取并持久化 `created_at` 游标 | Landing | 增量消费、轮询、失败重试、需要可靠 checkpoint 的处理流程 |
| `iter_data_batches()` | `dataset_type`、`where_sql`、`start_time`、`end_time`、`chunk_size`、`order_by`、`ascending`、`table`、`deserialize_json` | 返回迭代器，每次 yield 一个 DataFrame batch | SDK 内部推进 `created_at` 游标，直到数据读完 | Landing | 允许时间戳并列的一次性扫描、回填和离线处理 |
| `export_data_batches()` | `filter_query`、`batch_size`、`columns`、`table`、`deserialize_json` | 返回迭代器，每次 yield 一个经过校验的 DataFrame manifest batch | SDK 先生成完整唯一 ID 清单，再按精确 ID 取数并校验 | Serving | 要求固定行集合、检测重复 ID、且不能因时间戳游标漏数的正式离线导出 |
| `search()` | `query`、`limit`、`tags`、`where_sql`、`dataset_type`、`stream`、`table`、`search_fields`、`deserialize_json` | 返回一个 DataFrame；`stream=True` 时返回单个 DataFrame 的迭代器 | 一次有界搜索 | Serving | Dashboard 对 `search_text`、tags 和标量条件的关键词搜索 |

`query_data()`、`pull_data()` 和 `iter_data_batches()` 默认查询 landing，也可传入
`table=client.config.tables.serving_table`。`get_by_id()`、`search()` 和
`export_data_batches()` 默认查询 serving。查询条件中应尽量包含 `job_id`，以便
执行 HASH bucket 剪枝。

`query_data()` 和 `get_by_id()` 默认省略 null 表列；传入 `exclude_none=False`
可以保留。所有返回完整记录的读取接口默认保留 JSON 字符串；
`deserialize_json=True` 返回 Python 值且保留 JSON 内部 null。无法解析的 JSON
会保持为字符串，不会因展示选项让整行读取失败。

## 客户端接口

### Landing 数据

| 方法 | 用途 |
| --- | --- |
| `ingest_landing(record)` | 写入一条 `LandingRecord`。 |
| `ingest_landing_batch(records)` | 写入记录列表或 `LandingRecordBatch`。 |
| `query_data(filter_query, ..., table=None, exclude_none=True, deserialize_json=False)` | 默认查询 landing，也可查询指定表；始终返回 `List[dict]`。 |
| `update_landing(filter_query, updates, ..., touch_source_updated_at=True)` | patch 匹配记录并默认刷新 `source_updated_at`；SDK 时间字段以及 `id`、`created_at`、`job_id` 均受保护。 |
| `count_landing(partition=None)` | 统计行数，可选指定原始 `job_id` 或 hash bucket。 |
| `delete_landing(filter_query)` | 删除匹配的 landing 记录。 |

为兼容已有调用方式，`query_data()` 和 `update_landing()` 接受原始的 `partition="job-id"`。在 HASH 表上，SDK 会将其转换为 bucket 并补充 `job_id` 条件。仍建议在 `filter_query` 中显式包含 `job_id`。

执行带排序或 limit 的 landing 查询时，应在 `filter_query` 中包含 `job_id`。在当前 dldb 分区模型中，跨 bucket 的 `order_by + limit` 不是全局归并排序。

```python
with WTGatewayClient() as client:
    latest = client.query_data(
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

现有 `update_landing()` 调用不需要修改：新增的 keyword-only 参数
`touch_source_updated_at` 默认为 `True`。更新 `is_trainable`、payload、reward、
`meta_json`、`agent_model` 或任何 serving 会消费的 enrichment 字段时都必须保持
默认值，包括 landing 原地 ETL。只有确认不会影响任何下游结果的纯运维/诊断更新
才允许设置 `touch_source_updated_at=False`。ETL engine 应在字段值实际没有变化时
避免调用 update。返回值中的 `updated_fields` 仍表示调用方字段，
`effective_updated_fields` 表示实际提交字段；dldb 目前不会返回跨逻辑分区精确汇总
的匹配行数或更新行数。

### Serving、搜索和分页

| 方法 | 用途 |
| --- | --- |
| `ingest_serving(record)` / `ingest_serving_batch(records)` | append 写入 serving，并刷新 `serving_updated_at`。 |
| `upsert_serving(record)` / `upsert_serving_batch(records)` | ETL 按全局唯一 `id` 发布；保留 `source_updated_at` 并刷新 `serving_updated_at`。 |
| `query_data(filter_query, ..., table=serving_table, exclude_none=True, deserialize_json=False)` | 使用相同的过滤和 HASH 剪枝行为查询 serving；始终返回 `List[dict]`。 |
| `count_serving(partition=None)` / `delete_serving(filter_query)` | 对 serving 数据执行统计或删除。 |
| `search(query, ..., deserialize_json=False)` | 检索 serving 的 `search_text`、tags/SQL 或显式指定的标量字符串字段。 |
| `get_tags_distribution()` | 返回 serving 标签频次。 |
| `get_by_id(record_id, table=None, exclude_none=True, deserialize_json=False)` | 默认从 serving 返回一个精简字典，或精确查询一个指定表。 |
| `pull_data(..., table=None, deserialize_json=False)` / `iter_data_batches(..., table=None, deserialize_json=False)` | 默认读取 landing，或按指定表进行手动单页/自动分批读取。 |
| `export_data_batches(filter_query="", ..., table=None, deserialize_json=False)` | 默认从 serving 可靠导出固定 ID 清单，并校验每个精确 ID batch。 |

dldb 当前尚未开放向量搜索。关键词检索默认查询 `search_text`；如需检索其他
字符串列，可显式传入标量 `search_fields`。不透明 JSON trace 应通过 ETL 生成的
`search_text`、普通 SQL 条件或 tags 查询。设置 `stream=True` 时，会返回包含
当前结果 DataFrame 的迭代器。

### 索引维护

`maintain_table_indexes()` 只接受两张生产表或两张测试表的精确表名，
表名直接决定使用 landing 还是 serving 索引定义。调用方必须显式传入
原始 `job_id`/HASH bucket，或使用 `all_partitions=True`；方法会创建缺失索引，
并默认执行 dldb optimize。

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

集成测试会向现有 `landing_test` 和 `serving_test` 写入少量使用唯一范围的数据，
并在 `finally` 中清理和验证。测试显式使用这两张测试表，不受
`WT_SDK_PROFILE` 影响；数据库由 `WT_SDK_DB_URI` 选择。两张表都必须使用当前的
`HASH(job_id)` schema。

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

# 删除可丢弃测试表中的一个 HASH bucket
python scripts/ops/table_manager.py drop serving_test --partition 42
```

### 查询与数据检查

```bash
# 统计行数
python scripts/inspect/query_data.py --table wind_tunnel_landing --count

# 查询指定列
python scripts/inspect/query_data.py --table landing_test \
  --query "job_id = 'job-001'" --columns "id,session_id,step_id,is_terminal"

# 解码并查看 JSON payload 列，同时关闭显示截断
python scripts/inspect/query_data.py --table landing_test --limit 1 \
  --show-nested --no-truncate

# 将结果写为 pretty JSON，并展开 JSON payload 列
python scripts/inspect/query_data.py --table landing_test --limit 1 \
  --output ./artifacts/landing_sample.json

# 按分区对比预期标量索引和现有标量索引
python scripts/inspect/show_table_indexes.py landing_test

# 扫描逻辑表中的重复 ID
python scripts/inspect/scan_duplicate_id.py --table landing_test --max-output 100

# 旧 schema 数据的 payload 解码失败诊断工具
python scripts/inspect/scan_landing_nested_decode.py --table landing_test

# 查看 serving 标签和 ETL 生成的搜索文本
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
python scripts/ops/maintain_table_indexes.py \
  --table wind_tunnel_landing --all-partitions

# 创建缺失索引并 optimize 所有已有 serving bucket
python scripts/ops/maintain_table_indexes.py \
  --table wind_tunnel_serving --all-partitions
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
