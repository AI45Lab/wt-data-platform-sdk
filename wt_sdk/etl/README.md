# WT ETL v1 开发与接入规范

本目录提供 Wind Tunnel 轨迹数据的 ETL v1 引擎。它负责从 landing
发现发生过变化的数据，以 `(job_id, session_id)` 为完整轨迹范围加载，依次执行
stage，并由引擎统一持久化到 landing 或 serving。

本文既是框架说明，也是所有 ETL stage 贡献者必须遵守的 coding contract。

代码放置约定：引擎、checkpoint、pipeline 编排留在 `wt_sdk/etl/`；可复用业务规则各自
放在 `wt_sdk/etl/stages/<stage_name>.py`，并从 `stages/__init__.py` 导出；手动/定时
触发入口只放在 `scripts/etl/`。不要把新框架或业务 stage 放回历史目录
`scripts/existing_data_etl/`，也不要在 stage 文件 import 时自动连接数据库或执行注册。

## 核心对象与 factory 语义

以下对象不要混为一谈：

- **Stage**：一个纯业务转换规则，例如生成 `chosen_trace`。
- **`PipelineDefinition`**：某条逻辑 pipeline 的静态定义，包含名称、版本、模式、入口
  selector、stage 集合和 DAG；创建时会立即做静态校验，但不会读写数据。
- **Pipeline factory**：无参数函数，每次调用返回一个 `PipelineDefinition`。它不是正在
  运行的 ETL 实例，也不持有 client、checkpoint 或运行状态。
- **`ETLEngine`**：真正执行 pipeline 的运行时对象，负责扫描、加载 session、调用 stage、
  写入和 checkpoint。
- **一次 ETL run**：一个 `scripts/etl/run.py` 进程。它可以通过多个
  `--pipeline-factory` 加载多条 pipeline，并交给同一个 engine 串行执行。

因此，一个 factory 通常对应“一条可复用的 pipeline 配置”，而不是“一次 ETL 任务
实例”。同一个 factory 可以被多次手动运行或定时调用，每次都会产生新的 definition，
但增量状态由持久化 checkpoint 标识，而不保存在 factory 中。

### Stage 集合如何配置

Stage 集合由 factory 构造 `PipelineDefinition` 时显式确定。新增 stage 后，应把它加入相应
factory/builder 的 `stages=(...)`，而不是依赖目录自动发现。

v1 暂不提供运行时 `--skip-stage`/`--only-stage`。如果确实需要一条不包含某些 stage 的
pipeline，应创建一个名称和版本清晰的独立 factory，并保证剩余 stage 的依赖闭包完整。
尤其是增量执行不能临时删减 stage 后继续推进原 pipeline 的 checkpoint，否则会把没有
执行完整业务规则的数据标记为已处理。仅用于一次性实验的变体应优先使用 job/session 或
时间范围手动模式，并使用独立 pipeline identity。

## v1 的边界

v1 只有一个执行引擎，但支持两类 pipeline：

1. `PipelineMode.LANDING`：在 landing 内做 enrichment。引擎只提交实际发生变化的
   patch，并通过 `update_landing()` 自动刷新 `source_updated_at`。
2. `PipelineMode.SERVING`：从 landing 读取完整记录，处理后通过
   `upsert_serving_batch()` 按全局唯一 `id` 幂等发布到 serving。

两类 pipeline 在一个进程内按 **landing 在前、serving 在后** 串行执行。landing
发生实际变化的 session 会被立即交给后续 serving pipeline，因而不需要等待默认的
稳定期。若两类 pipeline 分开运行，serving 会在后续增量扫描中凭
`source_updated_at` 最终发现变更。

v1 不做以下事情：

- 不允许 stage 自己读写数据库、S3 或 checkpoint。
- 不并行执行 landing 与 serving pipeline。
- 不自动删除已经发布、后来又变成不可训练的 serving 行。该 retraction 语义需要单独
  确认后再实现。
- 不接受缺少 `job_id`、`session_id` 或 `step_id` 的轨迹；不猜测这类数据的归属。
- 不提供分布式 lease/并发调度。同一组
  `(pipeline, version, source table, target table)` 同时只能有一个增量 runner；调度器必须
  保证不重入。不同 pipeline 也先按 v1 规定串行运行。

## 数据范围与执行语义

- 一条完整轨迹的逻辑主键是 `(job_id, session_id)`。`session_id` 不要求全局唯一。
- 一个 session 内 `id` 必须唯一，`step_id` 必须非空且唯一，记录按 `step_id` 排序。
- 一个 session 最多对应一个非空 `env_id`。
- discovery 只读取 `id/job_id/session_id/source_updated_at`，发现任意一行变化后再完整
  加载整个 session。这保证 session 级 stage 看见完整轨迹。
- `source_updated_at` 表示 landing 来源数据最后一次业务变化；landing enrichment 也是
  会影响 serving 的业务变化，因此实际 patch 成功后必须刷新它。
- landing pipeline 下次可能再次扫描到自己更新的行。这是预期行为。引擎先做字段 diff，
  幂等 stage 在第二次执行时不会产生 patch，也不会再次刷新时间戳，因而不会死循环。
- serving 保留 landing 的 `source_updated_at`；SDK 在每次 serving upsert 时写入新的
  `serving_updated_at`。

## Stage coding contract

每个 stage 继承 `ETLStage`，并声明稳定的元数据：

```python
from wt_sdk.etl import ETLStage, StageContext


class ExampleStage(ETLStage):
    name = "example_enrichment"
    version = "1"
    required_fields = ("meta_json",)
    output_fields = ("search_text",)
    dependencies = ()

    def applies(self, record, context: StageContext) -> bool:
        return record.get("meta_json") is not None

    def transform(self, record, context: StageContext):
        return {"search_text": "..."}
```

贡献的 stage 必须满足以下约束：

1. **纯函数与确定性**：相同的 `record + context` 必须产生相同 patch。不得使用当前时间、
   随机数或未固定版本的外部状态决定结果。
2. **禁止 I/O**：不得创建 SDK client，不得读写 landing、serving、checkpoint、网络或
   本地文件。引擎统一处理 I/O、重试边界和提交顺序。
3. **不得原地修改输入**：`transform()` 返回新 `dict`，只包含该 stage 声明拥有的
   `output_fields`。返回完整 record、未声明字段或非 dict 都会失败。
4. **字段单一所有者**：同一 pipeline 内，一个输出字段只能由一个 stage 拥有。若后续
   stage 需要前序结果，用 `dependencies` 声明 stage 名称。required/output 字段都必须
   存在于当前统一 schema，且 `applies()` 必须返回真正的 bool。某 stage 适用时，它
   声明的全部 dependency 也必须已经对当前记录实际执行；否则该记录会失败。
5. **不得改系统字段**：stage 不能输出 `id`、`job_id`、`session_id`、`created_at`、
   `source_updated_at` 或 `serving_updated_at`。
6. **幂等**：stage 必须允许同一行、同一 session 被重复执行。checkpoint 失败恢复、手动
   backfill、landing 时间戳回扫都会导致合理的重复执行。
7. **条件分层**：pipeline 级 selector 决定记录是否进入整条 pipeline；`applies()` 只决定
   当前 stage 是否执行。不要在多个 stage 中复制互相矛盾的总入口条件。
8. **错误策略明确**：数据损坏或无法安全生成结果时抛出异常，使当前 checkpoint 窗口失败
   并可恢复；只有业务明确要求 best effort 的字段才允许返回 `None`。不要静默吞掉未知
   异常。
9. **JSON 边界**：`messages`、`response`、`chosen_trace`、`rejected_trace` 和
   `meta_json` 在 SDK 边界都是 JSON 字符串。stage 可在内存解析，但 patch 必须重新输出
   合法 JSON 字符串，不能输出 Python dict/list。
10. **版本可追踪**：改变 stage 结果语义时更新 stage `version`；改变 pipeline 的 stage
    集合、选择条件或结果语义时必须更新 pipeline `version`。pipeline version 属于
    checkpoint identity，新版本需要显式选择重新扫描起点。

`StageContext.session` 是按 `step_id` 排序的原始 session 快照，可用于 session 级判断。
它不会随着当前记录的前序 patch 改变；当前记录在同一 pipeline 内的 stage patch 会依次
合并，后序依赖 stage 可以从自己的 `record` 参数读取前序输出。

## 当前 pipeline 与 stage 清单

框架本身没有预置 landing enrichment 业务 pipeline；landing pipeline 的 stage 由具体
业务 factory 提供。当前提供的是标准跨表 `serving_publish` builder：

| 执行顺序 | Stage | 依赖 | 输入/输出 | 核心逻辑与状态 |
| --- | --- | --- | --- | --- |
| 1 | `normalize_claude_messages` | 无 | `agent_model/meta_json` → `messages` | Claude 轨迹判定和消息标准化扩展点，由对应贡献者实现；框架要求名称固定且声明输出 `messages`。 |
| 2 | `build_chosen_trace` | `normalize_claude_messages` | `is_trainable/messages/response` → `chosen_trace` | 解析标准化 `messages` 和 `response`，按顺序拼成 JSON trace；畸形 JSON 直接失败。已实现。 |
| 3 | `derive_job_tags` | `build_chosen_trace` | `is_trainable/job_id` → `tags` | 按 `#` 尽最大努力提取 job ID 前四段；不符合规则时写 `None`，不阻断运行。已实现。 |

### v1 serving pipeline 的固定业务规则

标准 serving pipeline 用 `build_serving_publish_pipeline(claude_stage)` 构造，入口条件是：

```text
is_trainable is True AND claude_stage.applies(record, context)
```

它的顺序固定为：

1. `normalize_claude_messages`：由贡献者实现，从 Claude 原始 `meta_json` 生成标准化
   `messages`。
2. `build_chosen_trace`：将标准化后的 `messages` 与 `response` 拼接为
   `chosen_trace`。
3. `derive_job_tags`：从 `job_id` 尽最大努力提取前四段
   `[数据集名字, harness名字, 模型名字, 任务类型]`。

Claude stage 必须命名为 `normalize_claude_messages`，并把 `messages` 声明为输出字段。
Claude 判定逻辑只写在它的 `applies()` 中。`job_id` 不满足约定、分段不足或前四段存在
空值时，`tags` 为 `None`，不能因此阻断整条轨迹。

示例 factory：

```python
from wt_sdk.etl import build_serving_publish_pipeline


def build_serving_pipeline():
    return build_serving_publish_pipeline(
        NormalizeClaudeMessagesStage(),
        version="1",
    )
```

factory 必须是无参数 callable，并返回一个 `PipelineDefinition`。运行入口通过
`module.path:callable_name` 显式加载，不做隐式目录扫描或 import-time 注册。factory 本身
也必须只做对象组装，不得连接数据库、访问网络或产生其他 import/run-time side effect。

## 新 Stage 的完整接入步骤

1. 在 `wt_sdk/etl/stages/<stage_name>.py` 新建一个 `ETLStage` 子类。
2. 声明唯一稳定的 `name`、`version`、`required_fields`、`output_fields` 和
   `dependencies`；实现 `applies()` 与纯函数 `transform()`。
3. 从 `wt_sdk/etl/stages/__init__.py` 导出该 class；若它属于公共 SDK API，再从
   `wt_sdk/etl/__init__.py` 导出。
4. 将 stage 显式加入目标 pipeline factory/builder。不要只把文件放进 `stages/` 目录，
   引擎不会自动扫描。
5. 若 stage 集合、依赖、selector 或结果语义变化，更新 pipeline version；不要只更新
   stage version 后继续使用旧 checkpoint identity。
6. 添加 stage 单测、依赖串联测试和非法 DAG 测试。
7. 先运行静态 `validate_dag()`/`--validate-only`，再用 `--list-stages` 核对最终执行顺序。
8. 最后在 test profile 使用真实 landing 数据执行 `--dry-run`，确认 runtime predicate、
   JSON 解析、session 校验和 transform 结果。

## DAG 校验与运行前检查

### Python 静态 API

开发者可以只构造 stage，不创建完整 pipeline，也不连接数据库：

```python
from wt_sdk.etl import PipelineDefinition


ordered_stages = PipelineDefinition.validate_dag(
    (
        NormalizeClaudeMessagesStage(),
        BuildChosenTraceStage(),
        DeriveJobTagsStage(),
    )
)
print([stage.name for stage in ordered_stages])
```

`validate_dag()` 成功时返回确定的拓扑执行顺序；失败时抛出
`PipelineConfigurationError`。它会检查：

- stage 类型、名称、版本以及字段/依赖声明；
- 缺失依赖、重复依赖和 DAG 环；
- 重复 stage 名；
- 多个 stage 争用同一个输出字段；
- schema 中不存在的 required/output 字段；
- 对 identity/timestamp 等不可修改字段的声明。

创建 `PipelineDefinition` 时会自动调用同一个方法，因此静态 API 与真实 pipeline 构造使用
完全一致的规则。`pipeline.describe_dag()` 可获得 JSON-serializable 的 stage 清单、执行
顺序和 dependency edges。

### CLI 静态检查

只校验一个或多个 factory，不创建 SDK client、不访问 dldb/S3，也不要求 `--profile`：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline-factory your_package.etl:build_serving_pipeline \
  --validate-only
```

列出 factory 最终生成的 stage、版本、输入输出、执行顺序和依赖边：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline-factory your_package.etl:build_serving_pipeline \
  --list-stages
```

传入多个 factory 时，这两个命令还会检查 v1 的跨 pipeline 顺序：landing pipeline 必须在
serving pipeline 之前，同一次 run 不能包含重复 pipeline identity。

### `--validate-only` 与 `--dry-run` 的区别

| 模式 | 连接/读取数据库 | 执行 `applies/transform` | 写业务表/checkpoint | 主要用途 |
| --- | --- | --- | --- | --- |
| `--validate-only` | 否 | 否 | 否 | 快速检查 stage 元数据、字段所有权、依赖 DAG 和 pipeline 顺序。 |
| `--list-stages` | 否 | 否 | 否 | 在静态校验通过后展示实际 stage 清单和 DAG。 |
| `--dry-run` | 是，会扫描真实 source | 是 | 否 | 用真实数据检查 selector、stage runtime、JSON、session 和输出 model。 |

所以 `--dry-run` 的确能覆盖静态 DAG 校验，并额外覆盖数据相关逻辑，但成本更高且依赖真实
环境；贡献 stage 时应先执行 `--validate-only`，然后运行单元测试，最后再做 test profile
的 `--dry-run`。

## 测试与代码评审要求

每个新 stage 至少提交以下 hermetic unit tests：

- `applies()` 的正例和反例；
- 正常输入对应的精确 patch；
- 缺失字段、null、畸形 JSON 和错误数据类型；
- 重复执行产生相同结果；
- 与依赖 stage 串联后顺序和结果正确；
- best-effort 字段不会使 row/session/batch 失败。

测试不得访问真实 S3。真实 integration test 只能显式使用 `landing_test` 和
`serving_test`，且必须遵守仓库根目录 `AGENTS.md` 的安全开关与清理约束。

提交 review 时需说明：stage 的输入字段、输出字段、触发条件、错误策略、幂等依据、依赖
关系，以及是否改变 pipeline version/backfill 范围。

## Checkpoint 与扫描模式

增量模式使用独立的、非分区 dldb 控制表 `wt_etl_checkpoints`。checkpoint identity 是：

```text
(pipeline_name, pipeline_version, source_table, target_table, HASH bucket)
```

checkpoint 保存已提交 watermark、当前固定窗口和页内 `last_processed_id`。只有一页内所有
session 的 landing update 或 serving batch upsert 成功后，页游标才推进；整个固定窗口
成功后才推进 watermark。进程崩溃后会从持久化的活动窗口恢复，而不是依赖内存状态。
恢复窗口完成后，同一次正式增量运行会继续追赶到本次启动时计算出的 cutoff。

默认稳定期是 2 小时，单次运行的截止时间在启动时固定为：

```text
cutoff = run_started_at - settle_delay
```

这里的 watermark 是“这个 pipeline、这个 bucket 已成功处理到哪个
`source_updated_at`”，不是当前系统时间。首次运行以及以后首次出现的新 HASH bucket 都要
提供同一个稳定的 `--start-from` bootstrap 起点。

以下手动模式不读写全局 checkpoint，并天然支持立即执行：

- `--job-id ... --session-id ...`
- `--job-id ...`
- `--start-time ... [--end-time ...]`
- `--force-unsettled`：把当前运行的稳定期设为 0；这是显式接受仍在变化数据的操作。

所有模式都要求 stage 和 serving upsert 幂等。手动模式适合补历史遗漏、刚完成数据的即时
验证，以及不知道具体 job_id 时从某一时间开始 backfill。

## 初始化与运行

checkpoint 可以与业务表使用同一个 dldb database URI，也可以使用单独的控制库；它必须
通过 `WT_SDK_ETL_STATE_DB_URI` 或命令行显式指定。初始化只创建缺失表，不会删除或重建：

```bash
set -a && source .env && set +a
.venv-dldb-v1/bin/python scripts/ops/init_etl_checkpoint_table.py \
  --db-uri s3://wind-tunnel-dldb \
  --confirm-create
```

先在 test profile 做 dry run：

```bash
WT_SDK_ETL_STATE_DB_URI=s3://wind-tunnel-dldb \
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --profile test \
  --pipeline-factory your_package.etl:build_serving_pipeline \
  --start-from 2026-08-01T00:00:00Z \
  --dry-run
```

正式增量运行去掉 `--dry-run`。首次 dry run 不写 checkpoint，因此随后正式运行仍需保留
`--start-from`。实际 ETL 执行强制显式选择 `--profile test|production`；静态
`--validate-only`/`--list-stages` 不需要 profile。任何非 dry-run 的 production 执行还
必须传入 `--confirm-production`。

同一次运行串联 landing 与 serving：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --profile test \
  --pipeline-factory your_package.etl:build_landing_pipeline \
  --pipeline-factory your_package.etl:build_serving_pipeline \
  --start-from 2026-08-01T00:00:00Z
```

即时处理一个 session：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --profile test \
  --pipeline-factory your_package.etl:build_serving_pipeline \
  --job-id 'dataset#harness#model#task#date#owner#extra' \
  --session-id 'session-id'
```

生产运行前必须先在 test tables 完成验证，并确认 profile、表名、pipeline version、起始
watermark 和 state database。`--start-from` 对首次 bootstrap 是包含边界；正常增量窗口是
`(上次 watermark, 本次 cutoff]`。不要把 `--force-unsettled` 当成定时任务默认参数。
