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
- **一次 ETL run**：一个 `scripts/etl/run.py` 进程。它可以通过 `--pipeline` 后的名称列表
  加载多条 pipeline，并交给同一个 engine 串行执行。

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

每条 pipeline 是 `wt_sdk/etl/pipelines/` 下一个独立 Python 文件。文件名（不含 `.py`）必须
和 `PipelineDefinition.name` 完全相同，文件只暴露无参数 `build_pipeline()`。CLI 根据短名称
导入对应文件，不需要用户写 Python package 路径。`--list-pipelines` 可查看所有可用名称，
`--list-stages` 可查看所选 pipeline 的 stage/DAG。

当前两条 pipeline：

- `landing_enrichment_pipeline.py`：landing 原地 enrichment pipeline；目前唯一业务 stage
  `UpdateIsTrainableStage` 已留好 TODO，
  贡献者实现前不能真实执行。
- `landing_to_serving_pipeline.py`：当前 OpenCode 可直接运行，包含 chosen trace 和 tags。

### Stage 顺序由谁负责

引擎会根据每个 stage 声明的 `dependencies` 做拓扑排序，因此真实的前置依赖不要求贡献者
手工把 stage 放在正确位置；即使 factory 声明顺序相反，依赖边仍会保证前置 stage 先执行。
但贡献者必须准确声明真实依赖，pipeline owner 必须决定哪些 stage 被接入。对于彼此完全
独立、DAG 中没有路径关系的 stage，引擎使用 pipeline 文件中的声明顺序作为稳定 tie-break。
不能为了展示顺序虚构 dependency；若一个 stage 必须读取另一个 stage 的 patch，就必须声明
dependency 并添加串联测试。

唯一需要特别区分的是“可选前处理”：例如 Claude normalizer 只对 Claude 行执行，而 chosen
trace 在 normalizer 不存在或不适用时也必须运行。这不是 hard dependency；pipeline owner
把可选 normalizer 声明在 consumer 前，并分别测试“前处理执行/跳过”两条路径。

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
- `--page-size` 只限制每页 discovery 轻量行数，不限制完整 session 大小。同一 session 的
  discovery 行可以落在不同 page；引擎在当前 bucket/run 内按 `(job_id, session_id)` 去重，
  第一次发现时就重新加载整个 session，因此不会只处理半条轨迹，也不会因跨页重复处理。
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

### `landing_enrichment_pipeline`（同表原地更新）

文件：`wt_sdk/etl/pipelines/landing_enrichment_pipeline.py`；CLI 名称：
`landing_enrichment_pipeline`。该名称不绑定某个字段；未来其他 landing 原地 enrichment
stage 也接入这条 pipeline，并由 DAG 编排。

| 执行顺序 | Stage | 依赖 | 输入/输出 | 核心逻辑与状态 |
| --- | --- | --- | --- | --- |
| 1 | `update_is_trainable` | 无 | 贡献者补充 → `is_trainable` | pipeline、diff patch、landing sink 和 `source_updated_at` 刷新已接好；`applies()`、`transform()`、required fields 和单测由贡献者在 `wt_sdk/etl/stages/trainability.py` 完成。当前 TODO 会显式报错，防止误运行。 |

贡献者只修改该 stage 及其测试；不在 stage 中调用 SDK/dldb，也不自行更新时间戳。
`transform()` 只返回 `{"is_trainable": bool}`。引擎会去掉与原值相同的 patch；只有实际
变化才调用 `update_landing()`，由该接口默认刷新 `source_updated_at`。

### `landing_to_serving_pipeline`（landing → serving）

文件：`wt_sdk/etl/pipelines/landing_to_serving_pipeline.py`；CLI 名称：
`landing_to_serving_pipeline`。

| 执行顺序 | Stage | 依赖 | 输入/输出 | 核心逻辑与状态 |
| --- | --- | --- | --- | --- |
| 1 | `build_chosen_trace` | 无 | `is_trainable/messages/response` → `chosen_trace` | 解析现有 `messages` 和 `response`，按顺序拼成 JSON trace；畸形 JSON 直接失败。已实现。 |
| 2 | `derive_job_tags` | 无 | `is_trainable/job_id` → `tags` | 按 `#` 尽最大努力提取 job ID 前四段；不符合规则时写 `None`，不阻断运行。已实现。 |

### v1 serving pipeline 的固定业务规则

标准 serving pipeline 的入口条件是：

```text
is_trainable is True
```

OpenCode 数据已有可直接使用的 `messages`，所以当前无需 provider normalization。
`build_chosen_trace` 和 `derive_job_tags` 彼此独立，不声明 dependency，只按 factory 声明
顺序执行。

未来 Claude stage 完成后，通过
`build_serving_publish_pipeline(NormalizeClaudeMessagesStage())` 加在同一 pipeline 的最前面：

1. `normalize_claude_messages`：由贡献者实现，从 Claude 原始 `meta_json` 生成标准化
   `messages`；其 `applies()` 只对 Claude 数据返回 true。
2. `build_chosen_trace`：对所有 trainable 数据使用“当前 record 的 messages”与 response
   生成 `chosen_trace`。Claude 行会看到前序 normalization patch，OpenCode 行继续使用原始
   messages。
3. `derive_job_tags`：从 `job_id` 尽最大努力提取前四段
   `[数据集名字, harness名字, 模型名字, 任务类型]`。

Claude stage 必须命名为 `normalize_claude_messages`，并把 `messages` 声明为输出字段。
Claude 判定逻辑只写在它的 `applies()` 中。它和 chosen trace 不声明硬 dependency，因为
normalizer 对 OpenCode 不适用时 chosen trace 仍必须执行；正确先后由 factory 声明顺序保证。
`job_id` 不满足约定、分段不足或前四段存在空值时，`tags` 为 `None`，不能因此阻断整条轨迹。
把 Claude stage 加入已经运行过的 serving pipeline 时必须提升 pipeline version，并为新版本
指定 backfill 起点，不能继续沿用旧版本 checkpoint。

示例 factory：

```python
from wt_sdk.etl import build_serving_publish_pipeline


def build_pipeline():
    return build_serving_publish_pipeline(
        NormalizeClaudeMessagesStage(),
        name="landing_to_serving_pipeline",
        version="2",
    )
```

每个 pipeline 文件的 `build_pipeline()` 必须是无参数 callable，并返回一个
`PipelineDefinition`。文件名、definition name 和 CLI short name 必须一致。factory 本身
只做对象组装，不得连接数据库、访问网络或产生其他 import/run-time side effect。

## 新 Stage 的完整接入步骤

1. 在 `wt_sdk/etl/stages/<stage_name>.py` 新建一个 `ETLStage` 子类。
2. 声明唯一稳定的 `name`、`version`、`required_fields`、`output_fields` 和
   `dependencies`；实现 `applies()` 与纯函数 `transform()`。
3. 从 `wt_sdk/etl/stages/__init__.py` 导出该 class；若它属于公共 SDK API，再从
   `wt_sdk/etl/__init__.py` 导出。
4. 将 stage 显式加入 `wt_sdk/etl/pipelines/<pipeline_name>.py` 的 `build_pipeline()`。不要只把文件放进 `stages/` 目录，
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

列出当前可用 pipeline 文件，不创建 SDK client、不访问 dldb/S3：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py --list-pipelines
```

只校验一个或多个 pipeline，不创建 SDK client、不访问 dldb/S3，也不要求 profile：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_to_serving_pipeline \
  --validate-only
```

列出 factory 最终生成的 stage、版本、输入输出、执行顺序和依赖边：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_to_serving_pipeline \
  --list-stages
```

传入多个 pipeline 名称时，这两个命令还会检查 v1 的跨 pipeline 顺序：landing pipeline 必须在
serving pipeline 之前，同一次 run 不能包含重复 pipeline identity。

对没有依赖关系的 stage，DAG 会保持它们在 factory 中的声明顺序；`dependencies` 只表达
真实的数据/执行前置条件，不能为了显示顺序而虚构 dependency。

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

增量模式使用按 profile 隔离的非分区 dldb checkpoint 控制表。checkpoint identity 是：

```text
(pipeline_name, pipeline_version, source_table, target_table, HASH bucket)
```

checkpoint 保存已提交 watermark、当前固定窗口、页内 `last_processed_id` 和最近一次写入
该 checkpoint 的 `last_run_id`。只有一页内所有
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

`--start-from` 和 `--start-time` 的区别：

- `--start-from` 只用于默认增量模式，是某个新 checkpoint/new bucket 第一次从哪里开始的
  bootstrap watermark。运行成功会持续推进 checkpoint，后续通常不用再传。
- `--start-time` 开启一次性手动时间范围 backfill；可以搭配 `--end-time`，但完全不读取或
  推进全局 checkpoint，重复执行同一命令就会重复处理同一范围。

| 场景 | 参数示例 | 结果 |
| --- | --- | --- |
| 第一次启动持续增量 ETL | `--start-from 2026-08-01T00:00:00Z` | 从该时间 bootstrap；成功后保存/推进每个 bucket checkpoint。 |
| 后续持续增量 ETL | 不传三种手动 selector，也通常不再传 `--start-from` | 从已有 watermark 追到本次固定 cutoff。 |
| 一次性补某段历史 | `--start-time 2026-08-01T00:00:00Z --end-time 2026-08-02T00:00:00Z` | 处理包含边界的时间范围，不读写全局 checkpoint。 |
| 从某时刻手动补到稳定 cutoff | `--start-time 2026-08-01T00:00:00Z` | 结束时间取本次 `now - settle delay`，不读写 checkpoint。 |

以下手动模式不读写全局 checkpoint，并天然支持立即执行：

- `--job-id job-a job-b`：一个参数后直接给 job list，处理各 job 的全部合法 session；
- `--job-id job-a --session-id s1 s2`：一个 job 下给 session list；
- `--session job-a s1 --session job-b s2`：跨多个 job 精确指定任意 job/session pair；
- `--start-time ... [--end-time ...]`
- `--source-filter "..."`：高级 dldb WHERE predicate，对每个 landing HASH bucket 做
  discovery，再按发现的 `(job_id, session_id)` 加载完整 session；
- `--force-unsettled`：把当前运行的稳定期设为 0；这是显式接受仍在变化数据的操作。

`--job-id`/`--session-id` 使用空格分隔的 list，也允许重复传参；多个 job 各自只处理部分
session 时使用可重复的 `--session JOB_ID SESSION_ID`，避免依赖不全局唯一的 session ID 猜测
归属。结构化的 job/session/time 参数仍是默认推荐：它们容易校验、含义清晰，并能在 job 模式下
直接 HASH pruning。`--source-filter` 不替代这些参数，只用于它们不能自然表达的临时筛选；
它接收 WHERE 条件表达式而不是完整 `SELECT`，会扫描所有现有 landing HASH buckets，且不写
checkpoint。条件命中的行只用于发现 session；一旦某行命中，引擎仍加载完整 session，并让
pipeline selector 决定其中每一行是否处理。三类入口 `--job-id`、`--start-time`、
`--source-filter` 互斥。

所有模式都要求 stage 和 serving upsert 幂等。手动模式适合补历史遗漏、刚完成数据的即时
验证，以及不知道具体 job_id 时从某一时间开始 backfill。

## 失败处理与 Audit Report

v1 不新增持久化 failure 表，但每条 pipeline 的每次实际执行都会生成一个独立 JSON report，
默认写入当前工作目录下的 `etl_reports/`，同时也在 terminal 输出。文件名格式为：

```text
<pipeline_name>__v<pipeline_version>__<UTC开始时间>__<随机后缀>.json
```

该完整文件名前缀也是 `pipeline_run_id`。可通过 `--report-dir` 改目录。report 使用临时文件
加原子 rename 落盘，避免把半个 JSON 当成完整审计结果。可归因到记录的 selector、stage、
输出 model、landing sink 和 serving sink 错误会收集为：

```json
{
  "record_id": "row-id",
  "job_id": "job-id",
  "session_id": "session-id",
  "stage_name": "build_chosen_trace",
  "error_type": "StageTransformError",
  "message": "response contains malformed JSON"
}
```

每条 pipeline 完成后都会输出以下 audit 计数：

- `discovery_rows_read`：增量/时间范围 discovery 读取的轻量行数；定向 session 模式可能为 0。
- `source_rows_read`：加载完整 session 后实际送入 pipeline 的 source 行数。
- `rows_selected`：通过 pipeline selector、进入 stage 流程的行数。
- `rows_succeeded`：通过 selector，且 stage、输出校验和实际 sink 均成功的行数；dry-run 时表示
  stage/output 成功，不包含真实 sink 写入。
- `rows_failed`：失败记录数。
- `landing_rows_updated` / `serving_rows_upserted`：成功产生的实际写入数；dry-run 时表示计划
  写入数。

Report 还包含 `pipeline_run_id`、`started_at`、`ended_at`、毫秒时间、`duration_ms`、`status`、
`sessions_processed`、`sessions_failed`、`failed_row_ids`、完整 `failures` 和实际
`report_path`。失败记录同时保留 job/session scope，因此后续可以按一次 report 批量构造
session 重试。存在行级失败时命令仍会先写 report、打印汇总，然后以 exit code `1` 结束。

增量执行不会越过失败位置提交 page cursor/window watermark；checkpoint 标为 `FAILED`，下次
运行会安全重放。失败前已经成功的 landing patch/serving upsert 也会重放，因此 stage 和 sink
必须幂等。不同 HASH bucket 独立提交：一个 bucket 失败不阻止其他 bucket 的安全 checkpoint。

report 目录应由调度器作为运行 artifact 保存或上传到约定的 ETL audit 路径。若后续需要跨
run 结构化查询、告警、重试次数和保留周期，再增加单独的 `wt_etl_failures` dldb 表；在这些
需求确认前不把失败明细混入 checkpoint 表。

## 初始化与运行

### `run.py` 完整参数表

| 参数 | 是否必需/默认值 | 作用与约束 | Sample |
| --- | --- | --- | --- |
| `-h`, `--help` | 可选 | 显示完整 CLI 帮助并退出。 | `--help` |
| `--pipeline` | ETL/检查必需，可接 list | `pipelines/` 下的短名称；多个名称按给定顺序串行执行。 | `--pipeline landing_enrichment_pipeline landing_to_serving_pipeline` |
| `--profile` | 可选，覆盖环境变量 | 同时选择业务表和 checkpoint 表；优先级为命令行、`WT_SDK_PROFILE`、SDK 默认 `test`。静态检查不需要。 | `--profile test` |
| `--list-pipelines` | 可选 | 列出 `wt_sdk/etl/pipelines/` 下可用 pipeline 名称；不需要 `--pipeline`/profile。 | `--list-pipelines` |
| `--list-stages` | 可选，默认关闭 | 加载并校验所选 pipeline，输出 stage、字段、顺序和依赖边；不创建 SDK client。 | `--pipeline landing_to_serving_pipeline --list-stages` |
| `--validate-only` | 可选，默认关闭 | 只做 pipeline/stage DAG 静态校验；不访问数据库、不执行 transform。 | `--validate-only` |
| `--landing-table` | 可选，按 profile | 覆盖 source landing 逻辑表名。 | `--landing-table landing_test` |
| `--serving-table` | 可选，按 profile | 覆盖 serving 目标逻辑表名。 | `--serving-table serving_test` |
| `--page-size` | 可选，默认 `1000` | 每页轻量 discovery 行数；不是完整 session 截断大小，跨 page 的同一 session 会去重并整组加载。 | `--page-size 500` |
| `--settle-delay-seconds` | 可选，默认 `7200` | 增量 cutoff 的稳定延迟；无显式 `--end-time` 的时间范围也使用它。 | `--settle-delay-seconds 3600` |
| `--start-from` | 首次增量/新 bucket 必需 | 首个 checkpoint 的包含式 bootstrap 时间；支持 ISO 8601、epoch 秒或 epoch 毫秒。不能与 job/time-range 模式组合。 | `--start-from 2026-08-01T00:00:00Z` |
| `--start-time` | 手动时间范围必需 | 按 `source_updated_at` 做包含式 backfill；不推进全局 checkpoint。 | `--start-time 2026-08-04T00:00:00Z` |
| `--end-time` | 可选 | 手动范围包含式结束时间；必须和 `--start-time` 一起使用。省略时取当前 cutoff。 | `--end-time 2026-08-05T00:00:00Z` |
| `--job-id` | 可选，接 list | 立即处理一个或多个 job 的全部合法 session；也允许重复参数。 | `--job-id job-a job-b` |
| `--session-id` | 可选，接 list | 一个 job 下处理多个 session；要求恰好一个 `--job-id`。 | `--job-id job-a --session-id s1 s2` |
| `--session JOB_ID SESSION_ID` | 可选，可重复 | 精确指定来自任意多个 job 的 job/session pair。不能与 `--job-id/--session-id` 混用。 | `--session job-a s1 --session job-b s2` |
| `--source-filter` | 可选 | 高级手动 dldb WHERE 表达式；扫描所有 landing HASH buckets，发现后加载完整 session，不使用 checkpoint。与 job/time 模式互斥。 | `--source-filter "is_trainable = true AND agent_model = 'opencode'"` |
| `--force-unsettled` | 可选，默认关闭 | 将本次隐式 cutoff 的 settle delay 设为 0；显式接受仍可能变化的数据。定向 job/session 本来就立即执行。 | `--force-unsettled` |
| `--dry-run` | 可选，默认关闭 | 读取真实 source 并执行 selector/stage/output 校验，但不写 landing、serving 或 checkpoint。 | `--dry-run` |
| `--confirm-production` | production 写入必需 | 非 dry-run production 执行的二次安全确认。 | `--confirm-production` |
| `--state-db-uri` | 默认增量模式必需，可用环境变量 | ETL 控制表所在独立 dldb database；也可设置 `WT_SDK_ETL_STATE_DB_URI`。普通 SDK 和手动定向 ETL 不需要。 | `--state-db-uri s3://wind-tunnel-etl` |
| `--checkpoint-table` | 可选，按 profile | 覆盖 checkpoint 表名；默认 test=`etl_checkpoints_test`，production=`wind_tunnel_etl_checkpoints`。 | `--checkpoint-table custom_checkpoints` |
| `--report-dir` | 可选，默认 `etl_reports` | 每条 pipeline 每次运行的 JSON audit report 目录；静态检查不生成 report。 | `--report-dir /var/log/wt-etl/reports` |

job/session、`--start-time`、`--source-filter` 三种手动入口互斥；`--start-from` 仅用于默认
增量模式。命令接受多个 pipeline 名称，但 v1 要求所有 landing pipeline 位于 serving
pipeline 之前。

### 辅助表与建表

当前 ETL v1 只定义一种 checkpoint schema，但在独立 database 中创建 test/prod 两张逻辑表：

| `WT_SDK_PROFILE` | 业务表 | Checkpoint 表 |
| --- | --- | --- |
| `test` | `landing_test` / `serving_test` | `etl_checkpoints_test` |
| `production` | `wind_tunnel_landing` / `wind_tunnel_serving` | `wind_tunnel_etl_checkpoints` |

它们不是直接通过原生 LanceDB API 定义或访问的；SDK 使用 PyArrow schema 描述字段，并统一
通过 `dldb` 创建、查询和 upsert，底层物理存储仍由 dldb/LanceDB 管理。

ETL 控制数据不放进只承载轨迹数据的 `s3://wind-tunnel-dldb`。约定使用新的独立 database
URI `s3://wind-tunnel-etl`，未来的 failure/control 表也放在这里。2026-08-05 的只读 catalog
检查确认旧轨迹库里没有 checkpoint 表；本次代码变更不会自动创建新的 S3 database/table。

Schema 定义位于 `wt_sdk/etl/checkpoint.py` 的 `ETL_CHECKPOINT_SCHEMA`：

| 字段 | 类型/可空 | 含义 |
| --- | --- | --- |
| `id` | string, non-null | checkpoint identity 的物化字符串，不是轨迹 row ID。 |
| `pipeline_name` / `pipeline_version` | string, non-null | 隔离不同 pipeline 及版本。 |
| `source_table` / `target_table` | string, non-null | 隔离 test/prod 及 landing/serving sink。 |
| `bucket` | int32, non-null | landing 的 dldb HASH bucket。 |
| `committed_until_ms` | int64, non-null | 已完整成功处理到的 `source_updated_at` watermark。 |
| `last_run_id` | string, nullable | 最近一次更新该 bucket checkpoint 的 `pipeline_run_id`，用于关联 JSON report。 |
| `active_window_start_ms` / `active_window_end_ms` | int64, nullable | 正在执行或失败待恢复的固定时间窗口。 |
| `last_processed_id` | string, nullable | 活动窗口内最后安全提交的分页 ID cursor。 |
| `status` | string, non-null | `IDLE`、`RUNNING` 或 `FAILED`。 |
| `updated_at_ms` | int64, non-null | checkpoint 状态最后更新时间。 |

checkpoint **不是每次运行追加一组历史行**。每个
`(pipeline_name, pipeline_version, source_table, target_table, bucket)` 只有一行，并通过
`id` upsert 为当前恢复状态。一次 pipeline 执行由 JSON report 的 `pipeline_run_id` 标识；
checkpoint 的 `last_run_id` 只关联最近一次触碰该 bucket 的执行。完整运行历史属于 report，
不是 checkpoint 表。

两张表都是非分区控制表，需要在第一次默认增量运行前显式创建；runner 不会自动建表或覆盖
错误 schema。job/session 和时间范围手动模式不使用全局 checkpoint；但默认增量 dry-run
仍会读取并校验 checkpoint 表。

默认增量 ETL 和 checkpoint 初始化必须通过 `WT_SDK_ETL_STATE_DB_URI` 或命令行
`--state-db-uri` 显式指定 checkpoint database。普通 `WTGatewayClient`、上游 landing writer
和按 job/session/时间/`--source-filter` 运行的手动定向 ETL 不读取、也不要求这个变量；未配置
它不会影响 `WT_SDK_PROFILE`。项目根目录的本地 `.env` 已为 ETL 开发配置
`s3://wind-tunnel-etl`，提交的 `.env.example` 则将它保留为注释形式的 ETL 可选项。
初始化脚本默认检查并创建 test/prod 两张表，只创建缺失表，不会删除或重建已有表：

```bash
set -a && source .env && set +a
.venv-dldb-v1/bin/python scripts/ops/init_etl_checkpoint_table.py \
  --db-uri s3://wind-tunnel-etl \
  --confirm-create
```

source 根目录 `.env` 后，`WT_SDK_PROFILE=test` 会同时选择 test landing、serving 和
checkpoint 表，因此无需再写 `--profile test`。命令行 `--profile` 仍可临时覆盖环境变量；
如果命令行和环境变量都没有指定 profile，SDK 会安全地默认使用 `test`。
先做 test dry run：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_to_serving_pipeline \
  --start-from 2026-08-01T00:00:00Z \
  --dry-run
```

正式增量运行去掉 `--dry-run`。首次 dry run 不写 checkpoint，因此随后正式运行仍需保留
`--start-from`。静态检查不需要 profile。只有显式通过命令行或环境变量选择 production
才会访问生产业务表及生产 checkpoint 表；任何非 dry-run 的 production 执行还必须传入
`--confirm-production`。

同一次运行串联 landing 与 serving：

> `UpdateIsTrainableStage` 的 TODO 实现及单测合入前，只能对 landing pipeline 使用
> `--list-stages`/`--validate-only`，不要执行下面的真实数据命令。

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_enrichment_pipeline landing_to_serving_pipeline \
  --start-from 2026-08-01T00:00:00Z
```

即时处理一个 session：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_to_serving_pipeline \
  --job-id 'dataset#harness#model#task#date#owner#extra' \
  --session-id 'session-id'
```

一个 job 下立即处理多个 session：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_to_serving_pipeline \
  --job-id 'job-id' \
  --session-id 'session-1' 'session-2'
```

多个 job 各自只处理指定 session：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_to_serving_pipeline \
  --session 'job-a' 'session-1' \
  --session 'job-b' 'session-9'
```

高级条件 backfill：

```bash
.venv-dldb-v1/bin/python scripts/etl/run.py \
  --pipeline landing_to_serving_pipeline \
  --source-filter "is_trainable = true AND agent_model LIKE 'opencode%'" \
  --dry-run
```

生产运行前必须先在 test tables 完成验证，并确认 profile、表名、pipeline version、起始
watermark 和 state database。`--start-from` 对首次 bootstrap 是包含边界；正常增量窗口是
`(上次 watermark, 本次 cutoff]`。不要把 `--force-unsettled` 当成定时任务默认参数。
