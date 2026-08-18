# ETL Stage 贡献指南

这份文档面向贡献 ETL stage 的开发者。完整运行参数、checkpoint 和运维说明见
[`README.md`](README.md)。

## 先选择 pipeline

系统目前有两条 pipeline：

- `landing_enrichment_pipeline`：读取完整 landing session，在 landing 内原地补充或修正字段。
  引擎只提交最终实际发生变化的字段，并由 SDK 刷新 `source_updated_at`。
- `landing_to_serving_pipeline`：读取 enrichment 后的完整 landing session，生成面向外部用户的
  serving 记录，最终按全局唯一 `id` upsert；它不修改 landing。

一个 stage 应加入输出生命周期匹配的现有 pipeline。只有数据范围、执行模式或 checkpoint
语义确实独立时，才讨论新增 pipeline。Pipeline 文件必须在 `stages=(...)` 中显式列出成员，
禁止隐藏注册或自动发现。

## 唯一 Stage contract

每个 stage 继承 `ETLStage`，声明元数据，并只实现一个业务接口：

```python
from wt_sdk.etl import ETLStage, Session, SessionPatch, StageContext


class MockedNormalizeMessagesStage(ETLStage):
    """仅用于说明 contract 的 mocked 示例，不是实际业务 stage。"""

    name = "mocked_normalize_messages"
    version = "1"
    required_fields = ("id", "messages")
    output_fields = ("messages",)
    dependencies = ()
    job_discovery_filter = None

    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        patches = {}
        for record in session:
            desired_messages, used_fallback = mocked_normalize(record["messages"])
            if used_fallback:
                context.warn(
                    "messages used fallback normalization",
                    warning_type="FallbackNormalization",
                )
            if desired_messages != record["messages"]:
                patches[record["id"]] = {"messages": desired_messages}
        return patches
```

`session` 是按 `step_id` 排序、递归只读的完整 `(job_id, session_id)` 快照。返回值必须是：

```python
{
    "existing-record-id": {
        "declared_output_field": desired_value,
    },
}
```

Stage 自己决定处理零行、一行或多行。返回 `{}` 表示本 stage 不选择任何行；每个 record patch
必须非空。不得返回 session 之外的 ID，也不得返回 `output_fields` 之外的字段。

引擎按 DAG 逐个 stage 执行。一个 stage 完整返回并通过校验后，它对所有行的 patches 才会
统一合并进内存 working session；下一个 stage 收到的是前序 stage 全部处理完成后的新快照。
同一个 stage 内不会边遍历边改变输入，因此结果不依赖行遍历顺序。

### 非阻断 warning

当数据存在需要交给上游写入方分析的异常，但 stage 仍能确定并继续执行正确的业务逻辑时，使用
`context.warn()` 上报结构化 warning：

```python
context.warn(
    "response missing optional provider metadata; used fallback",
    warning_type="ProviderMetadataFallback",  # 可选，默认 StageWarning
)
```

`context.warn()` 返回 `None`，不会中断当前函数。Stage 在调用后继续执行剩余逻辑并正常返回
patch；后续 stage、当前 session 的 sink 和其他 session 都照常执行。Warning 不等同于 patch，
不会单独选择 record，也不会改变业务输出。

Warning 会按发出顺序写入最终 JSON report 的 `warnings`，包含 `job_id`、`session_id`、
`stage_name`、`warning_type` 和 `message`。Report 同时给出 `warning_count` 和
`sessions_warned`。仅有 warning 的 pipeline 状态仍是
`SUCCEEDED`，命令 exit code 仍为 `0`，checkpoint 按正常成功规则推进。

如果 stage 先发出 warning，随后遇到真正无法继续的错误，已经发出的 warning 与 failure 都会
保留在 report；该 session 仍遵循 error 语义，不产生业务输出。Warning message 和
`warning_type` 必须是非空字符串。

引擎本身也可能产生 session 级 warning。当前 session validation 遇到重复 `step_id` 时会发出
一条 `DuplicateStepId` warning，随后沿用原有的 `step_id` 排序并继续执行 stages；stage 无需
重复上报该 warning。其他 session 结构校验错误仍会中断该 session。

不要通过 `raise`、Python `warnings.warn()` 或日志代替这个接口：`raise` 会中断 stage，普通
Python warning/日志也不会进入结构化 ETL audit report。无法确定正确业务结果、patch 无法通过
校验或继续执行可能产生错误输出时，仍必须抛出明确异常。

### 可选的 `job_discovery_filter`

`job_discovery_filter` 只是显式 `--job-id` 模式的保守读取优化，不是共享业务 selector。它必须
是一个 dldb WHERE 行谓词，并满足：只要该 stage 可能处理某个 session，session 中至少有一行
会匹配这个谓词。命中任意行后，引擎仍会加载并校验完整 session，再由 `transform_session()`
作最终决定。

只有 pipeline 内所有 stage 都声明安全提示时，引擎才把这些提示用 OR 合并；任意 stage 保留
`None` 就自动退回 job 全量 discovery。无法用行级证据安全表达的跨行条件必须保留 `None`，
不能为了性能填写可能漏 session 的过滤条件。

### 不同 pipeline mode 应如何返回

`transform_session()` 返回哪些 record ID，既表达 stage 的业务范围，也决定这些记录是否被该
pipeline 选中。Landing 和 Serving stage 应遵循不同的返回原则。

**Landing enrichment stage** 应优先表达业务范围内每条记录的最终期望状态。例如
trainability 可以分析完整 session 后，为业务范围内所有行返回最终布尔值：

```python
def transform_session(self, session, context):
    del context
    trainable_ids = mocked_detect_trainable_ids(session)
    return {
        record["id"]: {
            "is_trainable": record["id"] in trainable_ids,
        }
        for record in session
    }
```

即使部分返回值与 Landing 当前值相同也没有正确性问题。引擎会在所有 stage 完成后统一比较
最终 working session 与原始 Landing，只对真实变化调用 `update_landing()`；未变化的行不会
刷新 `source_updated_at`。Stage 可以提前跳过相同值以减少 `selected_rows` 和内存 patch，
但这只是可选优化，不能因此漏掉需要从 `True` 改回 `False` 等反向修正。Audit 中
`selected_rows` 表示 stage 返回过 patch 的记录数，`landing_rows_updated` 才表示实际写入数。

**Landing-to-serving stage** 必须为所有满足该 stage 发布条件的记录返回 patch，即使对应 ID
可能已经存在于 Serving，或者生成值与 Landing 当前字段相同。Stage 不得查询 Serving、不得
自行做“是否已经发布”的判断，也不能因为可能重复就跳过；Serving upsert 和 checkpoint 负责
幂等及崩溃重放。一条记录只要被该 pipeline 的任意 stage 返回 patch，就会使用所有 stage
合并后的完整 working record 进行 Serving upsert。

## 必须满足的约束

- Stage 必须确定、幂等：相同 session 和 stage version 必须产生相同业务结果。
- 禁止直接修改传入的 session。只能返回 patches。
- 禁止网络、文件、SDK client、dldb/S3、数据库写入、当前时间、随机数和其他不可重放副作用。
- 不得修改 `id`、`job_id`、`session_id`、`created_at`、`source_updated_at`、
  `serving_updated_at`。不要手动调用 update/upsert 或更新时间戳。
- `required_fields` 声明 stage 会读取的 schema 字段；`output_fields` 声明唯一允许返回的字段。
- JSON schema 字段在 ETL 边界是 JSON 字符串；解析后必须重新序列化，不能返回 Python
  `dict/list`。
- 同一 pipeline 内一个 output field 只能由一个 stage 拥有。
- 无法继续计算出正确结果的坏数据应抛出明确异常；可安全降级但需要上游分析的数据使用
  `context.warn()`；只有业务明确为 best effort 的输出才返回 `None`。
- Stage 计算或校验失败时，当前 session 不产生任何业务输出，依赖它的 stage 不会继续执行。
  已完成的内存 patch 也不会落库。

Landing pipeline 最终会把所有 stage 的 working session 与最初 landing session 做 diff；只有
真实变化才调用 `update_landing()`。Serving pipeline 会发布至少被一个 stage 返回 patch 的
record ID，并使用所有前序 stage 合并后的完整记录。

## `dependencies` 的含义

只有当前 stage 的结果依赖前序 stage 的完整输出时才声明依赖：

```python
class MockedAnalyzeNormalizedSessionStage(ETLStage):
    name = "mocked_analyze_normalized_session"
    version = "1"
    required_fields = ("id", "messages", "is_trainable")
    output_fields = ("is_trainable",)
    dependencies = ("mocked_normalize_messages",)

    def transform_session(self, session, context):
        del context
        selected_ids = mocked_cross_row_analysis(session)
        return {
            record["id"]: {"is_trainable": record["id"] in selected_ids}
            for record in session
            if record["is_trainable"] is not (record["id"] in selected_ids)
        }
```

这里第二个 stage 看到的 `messages` 已经包含第一个 stage 对整个 session 的全部修改。两个
stage 条件相似、声明顺序相邻或修改不同字段，不构成依赖。无依赖 stage 使用 pipeline 中的
声明顺序作为稳定顺序。

## 从开发到接入

1. 在 `wt_sdk/etl/stages/<stage_name>.py` 实现 stage，并从 `stages/__init__.py` 导出。
2. 在 `wt_sdk/etl/tests/unit/test_<stage_name>.py` 添加单元测试，至少覆盖：
   - 空 patch、单行 patch、多行 patch；
   - 跨行分析；
   - 坏数据；
   - 重复执行幂等；
   - 输入 session 不被修改；
   - 如有 dependency，验证能看到前序 stage 更新后的完整 session。
3. 在目标 `pipelines/<pipeline_name>.py` 的 `stages=(...)` 中显式加入 stage。Stage 集合或
   结果语义变化时升级 pipeline version，使新 checkpoint/backfill 范围可明确管理。
4. 不连接数据库检查完整 DAG：

```bash
python -m wt_sdk.etl.cli.run \
  --pipeline landing_enrichment_pipeline \
  --validate-only

python -m wt_sdk.etl.cli.run \
  --pipeline landing_enrichment_pipeline \
  --list-stages
```

5. 运行完整 hermetic tests：

```bash
python -m pytest -q
```

## 贡献者的真实 pipeline 测试

每个 stage PR 都应在 `landing_test` 测试“自己的 stage + 该 pipeline 原有 stages”。优先使用
`run_sessions()`：它读取真实 landing、执行 canonical pipeline 并写目标表，但不创建或推进
checkpoint。

测试必须使用 UUID 生成独立的 `job_id/session_id/id`，只操作 `landing_test/serving_test`，
并在 `finally` 中删除并验证测试数据。可复用
`wt_sdk.etl.tests.integration.helpers.cleanup_test_trajectory`。

```python
pipeline = load_pipeline("landing_enrichment_pipeline")
summary = ETLEngine(client).run_sessions(
    pipeline,
    [SessionKey(job_id, session_id)],
)
assert summary.failed_rows == 0
```

真实测试命令：

```bash
set -a && source .env && set +a
WT_SDK_RUN_INTEGRATION=1 python -m pytest -q \
  wt_sdk/etl/tests/integration/test_<stage_name>.py
```

不要复制或修改表里的真实行，不要清空整张 test 表。若必须测试 incremental，还要使用唯一
pipeline name/version，并在 `finally` 删除并验证对应 checkpoint；普通 stage 接入优先使用
targeted session 模式。
