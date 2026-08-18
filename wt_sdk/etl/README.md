# (WIP)WT ETL v1 使用与运维说明

本目录提供 Wind Tunnel 轨迹数据的 ETL v1 引擎。它负责从 landing
发现发生过变化的数据，以 `(job_id, session_id)` 为完整轨迹范围加载，依次执行
stage，并由引擎统一持久化到 landing 或 serving。

本文面向 ETL pipeline 的使用、测试与运维。如果要开发或接入新 stage，请直接阅读
[`README_STAGE_DEVELOPMENT.md`](README_STAGE_DEVELOPMENT.md)，其中包含 stage contract、
依赖声明、接入步骤、单元测试和 `landing_test` 集成测试规范；本文不再重复这些内容。

ETL 的 runtime、CLI、运维/检查工具、文档和测试全部收敛在 `wt_sdk/etl/`：业务规则放在
`stages/`，pipeline 放在 `pipelines/`，入口放在 `cli/`，fixture/只读检查工具放在 `tools/`，
测试放在 `tests/{unit,integration}`。ETL tests 被 setuptools 排除在发布包之外。任何 stage
文件在 import 时都不得连接数据库或执行注册。

普通 SDK 使用者无需 import ETL。ETL 专属第三方依赖必须声明在 `pyproject.toml` 的
`[project.optional-dependencies].etl`，使用者按需执行 `pip install ".[etl]"`；不得为了某个
stage 把依赖加入核心 SDK dependencies。ETL tests 由 setuptools 明确排除，不会进入安装包。

## 核心对象与 factory 语义

以下对象不要混为一谈：

- **Stage**：一个纯 session 转换规则。它读取前序 stage 完整处理后的只读 session，自行决定
  处理哪些行，并返回按 record ID 组织的字段 patches。
- **`PipelineDefinition`**：某条逻辑 pipeline 的静态定义，包含名称、版本、模式、stage
  集合和 DAG；创建时会立即做静态校验，但不会读写数据。
- **Pipeline factory**：无参数函数，每次调用返回一个 `PipelineDefinition`。它不是正在
  运行的 ETL 实例，也不持有 client、checkpoint 或运行状态。
- **`ETLEngine`**：真正执行 pipeline 的运行时对象，负责扫描、加载 session、调用 stage、
  写入和 checkpoint。
- **一次 ETL run**：一个 `python -m wt_sdk.etl.cli.run` 进程。它可以通过 `--pipeline` 后的名称列表
  加载多条 pipeline，并交给同一个 engine 串行执行。

因此，一个 factory 通常对应“一条可复用的 pipeline 配置”，而不是“一次 ETL 任务
实例”。同一个 factory 可以被多次手动运行或定时调用，每次都会产生新的 definition，
但增量状态由持久化 checkpoint 标识，而不保存在 factory 中。

### Pipeline 运行边界

CLI 使用 `wt_sdk/etl/pipelines/` 下的短名称加载 pipeline。`--list-pipelines` 可查看当前
可用名称，`--list-stages` 可查看所选 pipeline 的完整 stage 清单和 DAG。

v1 不提供运行时 `--skip-stage`/`--only-stage`。一次运行必须执行所选 pipeline 的完整定义，
否则继续推进同一 pipeline/version 的 checkpoint 会错误地把未执行完整规则的数据标记为
已处理。一次性补数或验证应通过 job/session、时间范围或 `--source-filter` 缩小数据范围，
而不是临时删减 stage。

## v1 的边界

v1 只有一个执行引擎，但支持两类 pipeline：

1. `PipelineMode.LANDING`：在 landing 内做 enrichment。引擎只提交实际发生变化的
   patch，并通过 `update_landing()` 自动刷新 `source_updated_at`。
2. `PipelineMode.SERVING`：从 landing 读取完整记录，处理后通过
   `upsert_serving_batch()` 按全局唯一 `id` 幂等发布到 serving。

两类 pipeline 在一个进程内按 **landing 在前、serving 在后** 串行执行。landing
发生实际变化的 session 会被立即交给后续 serving pipeline；即使运维显式配置了稳定延迟，
同一次命令中的两阶段也会立即衔接。若两类 pipeline 分开运行，serving 会在后续增量扫描中凭
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

### 强制运行前置条件

ETL 的正确运行要求以下约束始终成立：

- `is_session_completed=true` 只能在该 session 的全部数据写入完成后设置；完成后的 session
  不得继续追加、删除或修改轨迹数据。
- `source_updated_at` 必须使用单调前进的 Unix epoch 毫秒时间；任何影响下游结果的 landing
  变更都必须刷新该字段，禁止回填或倒退时间戳。
- 每次运行开始后新产生、因固定 snapshot 未进入本轮的数据，必须由后续 ETL 重跑覆盖。手动
  模式必须再次运行相应 job/range，增量模式必须继续从 checkpoint 追赶，不能把单次执行视为
  持续数据流的最终处理结果。

- 一条完整轨迹的逻辑主键是 `(job_id, session_id)`。`session_id` 不要求全局唯一。
- 一个 session 内 `id` 必须唯一，`step_id` 必须非空且唯一，记录按 `step_id` 排序。
- 一个 session 最多对应一个非空 `env_id`。
- discovery 只读取 `id/job_id/session_id/source_updated_at`，发现任意一行变化后再完整
  加载整个 session。这保证 session 级 stage 看见完整轨迹。
- `--job-id` 模式允许 stage 声明保守的 `job_discovery_filter`：当前 enrichment 用
  `is_session_completed=true` 发现完整 session，serving 用 `is_trainable=true`。只有 pipeline
  内每个 stage 都声明安全提示时才收窄，否则退回 job 全量 discovery；提示不是业务 selector。
- 同一 job 的 session 以有界批次合并读取（默认 25），随后按 `(job_id, session_id)` 拆回完整
  session。Stage 仍然一次只接收一个 session。
- 同一读取批次完成全部 session transform 后，内容完全相同的 landing patches 会合并成有界的
  `job_id + id IN (...)` update；多个 session 的 serving records 也会合并 upsert。默认每个
  sink 请求最多 100 行。单个 sink 批次失败时仍逐 record 记录 failure，已成功批次可安全重放。
- Pipeline 按 DAG 逐个 stage 执行。每个 stage 都读取当前完整 working session；它对所有行
  返回的 patches 通过校验并统一合并后，下一个 stage 才开始，因此后序 stage 能看到前序
  stage 对整个 session 的完整结果。
- Stage 输入递归只读，业务代码只能返回 `{record_id: {field: desired_value}}`。Stage 自己控制
  处理零行、一行或多行；引擎负责校验、合并以及最终持久化。
- `--page-size` 只限制每页 discovery 轻量行数，不限制完整 session 大小。同一 session 的
  discovery 行可以落在不同 page；引擎在当前 bucket/run 内按 `(job_id, session_id)` 去重，
  第一次发现时就重新加载整个 session，因此不会只处理半条轨迹，也不会因跨页重复处理。
- `source_updated_at` 表示 landing 来源数据最后一次业务变化；landing enrichment 也是
  会影响 serving 的业务变化，因此实际 patch 成功后必须刷新它。
- landing pipeline 下次可能再次扫描到自己更新的行。这是预期行为。引擎先做字段 diff，
  幂等 stage 在第二次执行时不会产生 patch，也不会再次刷新时间戳，因而不会死循环。
- serving 保留 landing 的 `source_updated_at`；SDK 在每次 serving upsert 时写入新的
  `serving_updated_at`。

## 哪些运行会改变 Landing 时间戳

先看 **pipeline 类型**，再看是否 `--dry-run`；`--job-id`、`--session`、时间范围和
`--source-filter` 只决定扫描范围，不改变写入语义。

| Pipeline 与运行方式 | Landing 数据 | Landing `source_updated_at` | Serving 数据 | ETL checkpoint |
| --- | --- | --- | --- | --- |
| `landing_to_serving_pipeline` + 任意手动模式 + `--dry-run` | 只读 | **不变** | 不写；report 中是计划 upsert 数 | 不写 |
| `landing_to_serving_pipeline` + 任意手动模式正式运行 | 只读 | **不变** | 按 `id` upsert，刷新 `serving_updated_at` | 不写 |
| `landing_to_serving_pipeline` + incremental + `--dry-run` | 只读 | **不变** | 不写 | 只读并校验，不推进 |
| `landing_to_serving_pipeline` + incremental 正式运行 | 只读 | **不变** | 按 `id` upsert，刷新 `serving_updated_at` | 成功后推进 |
| `landing_enrichment_pipeline` + 任意模式 + `--dry-run` | 只读 | **不变** | 不写 | 不写或只读，不推进 |
| `landing_enrichment_pipeline` + 手动模式正式运行 | 只更新产生非空 diff 的行 | **仅实际更新成功的行改变** | 不写 | 不写 |
| `landing_enrichment_pipeline` + incremental 正式运行 | 只更新产生非空 diff 的行 | **仅实际更新成功的行改变** | 不写 | 成功后推进 |
| 同一次命令先 enrichment、再 serving | enrichment 有实际 patch 时更新 | **有实际 landing patch 的行改变** | 后续 serving 立即发布变化后的完整 session | incremental 才推进各自 checkpoint |

“stage 被执行”不等于时间戳一定变化。Landing engine 会先比较 patch 与当前值；patch 为空或
所有值都相同时，不调用 `update_landing()`，因此不会刷新 `source_updated_at`。反过来，只要
landing enrichment 对下游有意义的字段发生实际变化，就必须使用默认
`touch_source_updated_at=True`，让后续 `landing_to_serving_pipeline` 能通过增量扫描发现它。

`landing_to_serving_pipeline` 永远不会修改 landing。它把 landing 的 `source_updated_at`
原样保留到 serving，并单独刷新 serving 的 `serving_updated_at`。可选的
`--settle-delay-seconds` 只影响本次扫描 cutoff，不会改变上述写入规则。

手动模式包括：`--job-id`/`--session-id`、`--session JOB SESSION`、
`--start-time [--end-time]` 和 `--source-filter`。这些模式都不读写全局 checkpoint；只有默认
incremental 模式会使用 checkpoint。

## 当前 pipeline 与 stage 清单

| Pipeline | 模式 | 当前 stage | 当前状态 |
| --- | --- | --- | --- |
| `landing_enrichment_pipeline` | landing 原地更新 | `update_is_trainable` | 业务逻辑仍为 TODO；实现合入前只能做静态检查，不能真实执行。未来 Claude normalization stage 也接入这里，并在 trainability 前完成。 |
| `landing_to_serving_pipeline` | landing → serving | `build_chosen_trace`、`derive_job_tags`、`build_search_text` | v3；可用于现有 OpenCode 轨迹，仅处理 `is_trainable is True` 的行。 |

`build_chosen_trace` 将 `messages + response` 写入 `chosen_trace`；`derive_job_tags` 从
`job_id` 前四段尽最大努力生成
`[数据集, harness, 模型, 任务类型]`，无法解析时写 `None`。
`build_search_text` 依赖前述两个 stage，将 `chosen_trace`、`rejected_trace`、`agent_model`、
`meta_json`、`dataset_type` 和 `tags` 中非空的文本按固定顺序用换行符拼接到 `search_text`；
`tags` 按元素逐项拼接。因为 `chosen_trace` 已完整包含当前行的 `messages + response`，搜索文本
不再重复拼接这两个原始字段。
Claude messages normalization 不属于 serving pipeline。它应先在 landing enrichment 中完成；
trainability 若依赖 normalized messages，必须声明对应 dependency。之后 serving pipeline 读取
已经 enrichment 的 landing session，继续生成 `chosen_trace`、`search_text` 和 `tags`。

如需开发、接入或调整 stage，请参见
[`README_STAGE_DEVELOPMENT.md`](README_STAGE_DEVELOPMENT.md)。

## Pipeline 静态检查

列出当前可用 pipeline 文件，不创建 SDK client、不访问 dldb/S3：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run --list-pipelines
```

只校验一个或多个 pipeline，不创建 SDK client、不访问 dldb/S3，也不要求 profile：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --validate-only
```

列出 factory 最终生成的 stage、版本、输入输出、执行顺序和依赖边：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --list-stages
```

传入多个 pipeline 名称时，这两个命令还会检查 v1 的跨 pipeline 顺序：landing pipeline 必须在
serving pipeline 之前，同一次 run 不能包含重复 pipeline identity。

### `--validate-only` 与 `--dry-run` 的区别

| 模式 | 连接/读取数据库 | 执行 `transform_session` | 写业务表/checkpoint | 主要用途 |
| --- | --- | --- | --- | --- |
| `--validate-only` | 否 | 否 | 否 | 快速检查 stage 元数据、字段所有权、依赖 DAG 和 pipeline 顺序。 |
| `--list-stages` | 否 | 否 | 否 | 在静态校验通过后展示实际 stage 清单和 DAG。 |
| `--dry-run` | 是，会扫描真实 source | 是 | 否 | 用真实数据检查 stage 的 session transform、JSON 和输出 model。 |

`--validate-only` 适合快速确认 pipeline 定义可以加载；`--dry-run` 会进一步使用真实 source
检查运行期行为，但成本更高且依赖 test 环境。stage 开发所需的测试和评审要求统一见
[`README_STAGE_DEVELOPMENT.md`](README_STAGE_DEVELOPMENT.md)。

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

每次命令启动时都会冻结一个固定扫描时间。默认稳定延迟是 `0`，因此默认扫描截止时间就是
本次命令的启动时间：

```text
cutoff = run_started_at - settle_delay
default: settle_delay = 0, cutoff = run_started_at
```

固定 cutoff 不会随着扫描过程继续向后移动；命令启动后新产生的数据由下一次运行处理。
只有显式传入例如 `--settle-delay-seconds 7200` 时，12:00 启动的命令才会只扫描到 10:00。
稳定延迟是可选的上游稳定性保护，不是默认行为，也不是只回看最近一段时间。

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
| 后续持续增量 ETL | 不传三种手动入口，也通常不再传 `--start-from` | 从已有 watermark 追到本次固定 cutoff。 |
| 一次性补某段历史 | `--start-time 2026-08-01T00:00:00Z --end-time 2026-08-02T00:00:00Z` | 处理包含边界的时间范围，不读写全局 checkpoint。 |
| 从某时刻手动补到命令启动时间 | `--start-time 2026-08-01T00:00:00Z` | 未传 `--end-time` 时默认结束于本次固定启动时间，不读写 checkpoint。 |
| 显式留出两小时稳定期 | `--settle-delay-seconds 7200` | cutoff 为本次固定启动时间减两小时；从 watermark/start 处理到该 cutoff。 |

以下手动模式不读写全局 checkpoint，并天然支持立即执行：

- `--job-id job-a job-b`：一个参数后直接给 job list，处理各 job 的全部合法 session；
- `--job-id job-a --session-id s1 s2`：一个 job 下给 session list；
- `--session job-a s1 --session job-b s2`：跨多个 job 精确指定任意 job/session pair；
- `--start-time ... [--end-time ...]`
- `--source-filter "..."`：高级 dldb WHERE predicate，对每个 landing HASH bucket 做
  discovery，再按发现的 `(job_id, session_id)` 加载完整 session；

`--job-id`/`--session-id` 使用空格分隔的 list，也允许重复传参；多个 job 各自只处理部分
session 时使用可重复的 `--session JOB_ID SESSION_ID`，避免依赖不全局唯一的 session ID 猜测
归属。结构化的 job/session/time 参数仍是默认推荐：它们容易校验、含义清晰，并能在 job 模式下
直接 HASH pruning。`--source-filter` 不替代这些参数，只用于它们不能自然表达的临时筛选；
它接收 WHERE 条件表达式而不是完整 `SELECT`，会扫描所有现有 landing HASH buckets，且不写
checkpoint。条件命中的行只用于发现 session；一旦某行命中，引擎仍加载完整 session。每个
stage 收到完整快照，并自行决定为哪些 record ID 返回 patch；pipeline 不设置共享业务过滤器。
至少被一个 stage 返回 patch 的 record 才进入输出流程。三类入口 `--job-id`、`--start-time`、
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
加原子 rename 落盘，避免把半个 JSON 当成完整审计结果。Stage session transform、patch 校验、
输出 model、landing sink 和 serving sink 错误会收集为：

```json
{
  "job_id": "job-id",
  "session_id": "session-id",
  "stage_name": "build_chosen_trace",
  "error_type": "StageTransformError",
  "message": "response contains malformed JSON"
}
```

Stage 还可以通过 `context.warn()` 上报不阻断执行的数据质量 warning。它们与 failure 分开存储：

```json
{
  "record_id": "row-id",
  "job_id": "job-id",
  "session_id": "session-id",
  "stage_name": "normalize_messages",
  "warning_type": "FallbackNormalization",
  "message": "messages used fallback normalization"
}
```

Warning 始终归属于当前 session，不终止当前 stage、后续 stage、sink 或其他 session，也不计入
`failed_rows`/`sessions_failed`。只有 warning 的执行仍为
`SUCCEEDED`、命令 exit code 为 `0`，增量 checkpoint 按正常成功规则推进。若同一 stage 后续
又发生真正 error，先前 warning 与 failure 会同时保留，但该 session 仍按 error 规则丢弃内存
业务输出。

每条 pipeline 完成后都会输出以下 audit 计数：

- `discovery_rows_read`：增量/时间范围 discovery 读取的轻量行数；定向 session 模式可能为 0。
- `source_rows_read`：加载完整 session 后实际送入 pipeline 的 source 行数。
- `rows_selected`：至少被一个 stage 返回非空 patch 的 record ID 数量。
- `rows_succeeded`：被 stage 选择，且 session transform、输出校验和实际 sink 均成功的行数；
  dry-run 时表示 stage/output 成功，不包含真实 sink 写入。
- `rows_failed`：失败事件数。Session-level stage 无法归因到单行时，failure 的 `record_id` 可以
  为 null，但始终保留 job/session scope。
- `warnings_emitted`：stage 发出的 warning 事件数；同一 session 可以产生多条。它与 report
  顶层的 `warning_count` 以及 `warnings` 数组长度相同。
- `sessions_warned`：至少发出一条 warning 的 session 执行次数。
- `landing_rows_updated` / `serving_rows_upserted`：成功产生的实际写入数；dry-run 时表示计划
  写入数。

Report 还包含 `pipeline_run_id`、`started_at`、`ended_at`、毫秒时间、`duration_ms`、`status`、
`sessions_processed`、`sessions_failed`、`sessions_warned`、`warning_count`、`failed_row_ids`、
完整 `failures`、完整 `warnings` 和实际
`report_path`。失败记录同时保留 job/session scope，因此后续可以按一次 report 批量构造
session 重试。存在 stage/session/record 失败时命令仍会先写 report、打印汇总，然后以
exit code `1` 结束。一个 stage 失败后，当前 session 不执行下游 stage，也不提交已计算的
业务 patches。

源表读取遇到明确的临时 S3/HTTP 错误时最多尝试三次并做指数退避。重试耗尽后，failure 归因到
`__discovery__` 或 `__session_load__`，report 保留失败前已经完成的 session 和行计数，不会再
退化成全零的 pipeline failure。

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
| `--session-batch-size` | 可选，默认 `25` | 每次源表查询合并加载的完整 session 数；stage 仍逐个完整 session 执行。 | `--session-batch-size 25` |
| `--sink-batch-size` | 可选，默认 `100` | 单次 landing update 或 serving upsert 最多包含的记录数；失败仍逐记录进入 report。 | `--sink-batch-size 100` |
| `--settle-delay-seconds` | 可选，默认 `0` | 从本次固定启动时间减去的可选稳定延迟；增量模式及未显式传 `--end-time` 的时间范围使用它。 | `--settle-delay-seconds 7200` |
| `--start-from` | 首次增量/新 bucket 必需 | 首个 checkpoint 的包含式 bootstrap 时间；支持 ISO 8601、epoch 秒或 epoch 毫秒。不能与 job/time-range 模式组合。 | `--start-from 2026-08-01T00:00:00Z` |
| `--start-time` | 手动时间范围必需 | 按 `source_updated_at` 做包含式 backfill；不推进全局 checkpoint。 | `--start-time 2026-08-04T00:00:00Z` |
| `--end-time` | 可选 | 手动范围包含式结束时间；必须和 `--start-time` 一起使用。省略时取当前 cutoff。 | `--end-time 2026-08-05T00:00:00Z` |
| `--job-id` | 可选，接 list | 立即处理一个或多个 job 的全部合法 session；也允许重复参数。 | `--job-id job-a job-b` |
| `--session-id` | 可选，接 list | 一个 job 下处理多个 session；要求恰好一个 `--job-id`。 | `--job-id job-a --session-id s1 s2` |
| `--session JOB_ID SESSION_ID` | 可选，可重复 | 精确指定来自任意多个 job 的 job/session pair。不能与 `--job-id/--session-id` 混用。 | `--session job-a s1 --session job-b s2` |
| `--source-filter` | 可选 | 高级手动 dldb WHERE 表达式；扫描所有 landing HASH buckets，发现后加载完整 session，不使用 checkpoint。与 job/time 模式互斥。 | `--source-filter "is_trainable = true AND agent_model = 'opencode'"` |
| `--dry-run` | 可选，默认关闭 | 读取真实 source 并执行 stage `transform_session` 与 output 校验，但不写 landing、serving 或 checkpoint。 | `--dry-run` |
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
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.init_checkpoint_tables \
  --db-uri s3://wind-tunnel-etl \
  --confirm-create
```

source 根目录 `.env` 后，`WT_SDK_PROFILE=test` 会同时选择 test landing、serving 和
checkpoint 表，因此无需再写 `--profile test`。命令行 `--profile` 仍可临时覆盖环境变量；
如果命令行和环境变量都没有指定 profile，SDK 会安全地默认使用 `test`。
先做 test dry run：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --start-from 2026-08-01T00:00:00Z \
  --dry-run
```

正式增量运行去掉 `--dry-run`。首次 dry run 不写 checkpoint，因此随后正式运行仍需保留
`--start-from`。静态检查不需要 profile。只有显式通过命令行或环境变量选择 production
才会访问生产业务表及生产 checkpoint 表；任何非 dry-run 的 production 执行还必须传入
`--confirm-production`。

同一次运行串联 landing 与 serving：

> `UpdateIsTrainableStage.transform_session()` 的 TODO 实现及单测合入前，只能对 landing pipeline 使用
> `--list-stages`/`--validate-only`，不要执行下面的真实数据命令。

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_enrichment_pipeline landing_to_serving_pipeline \
  --start-from 2026-08-01T00:00:00Z
```

即时处理一个 session：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --job-id 'dataset#harness#model#task#date#owner#extra' \
  --session-id 'session-id'
```

一个 job 下立即处理多个 session：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --job-id 'job-id' \
  --session-id 'session-1' 'session-2'
```

多个 job 各自只处理指定 session：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --session 'job-a' 'session-1' \
  --session 'job-b' 'session-9'
```

高级条件 backfill：

```bash
.venv-dldb-v1/bin/python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --source-filter "is_trainable = true AND agent_model LIKE 'opencode%'" \
  --dry-run
```

生产运行前必须先在 test tables 完成验证，并确认 profile、表名、pipeline version、起始
watermark 和 state database。`--start-from` 对首次 bootstrap 是包含边界；正常增量窗口是
`(上次 watermark, 本次 cutoff]`。默认 cutoff 等于命令启动时间；只有确认上游需要稳定窗口时
才显式传入非零 `--settle-delay-seconds`。
