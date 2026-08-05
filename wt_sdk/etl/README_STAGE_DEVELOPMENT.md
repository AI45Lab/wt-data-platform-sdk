# ETL Stage 贡献指南

这份文档面向只需要贡献一个 stage 的开发者。完整引擎语义和所有运行参数见
[`README.md`](README.md)。

## Stage 必须遵守的 contract

每个 stage 必须继承 `ETLStage`，声明以下 class attributes，并实现两个方法：

```python
from wt_sdk.etl import ETLStage, StageContext


class NormalizeEnvNameStage(ETLStage):
    name = "normalize_env_name"              # pipeline 内稳定且唯一
    version = "1"                            # 结果语义变化时升级
    required_fields = ("env_name",)          # 读取的 schema 字段
    output_fields = ("env_name",)            # transform 可能返回的字段
    dependencies = ()                        # 真正的前序 stage 名称

    def applies(self, record, context: StageContext) -> bool:
        value = record.get("env_name")
        return isinstance(value, str) and value != value.strip()

    def transform(self, record, context: StageContext) -> dict:
        return {"env_name": record["env_name"].strip()}
```

必须满足：

- `applies()` 只能返回 `bool`；`transform()` 只能返回 `dict patch`，且不能包含
  `output_fields` 之外的字段。
- 相同 `record + context` 必须得到相同结果。禁止随机数、当前时间、网络、文件、SDK client、
  dldb/S3 读写；stage 只负责内存转换。
- 必须幂等。重复执行、checkpoint 恢复和手动 backfill 都不能持续产生新结果。
- 不得输出 `id`、`job_id`、`session_id`、`created_at`、`source_updated_at`、
  `serving_updated_at`。不要手动调用 update/upsert，也不要手动更新时间戳。
  Landing 实际字段发生变化时，引擎会通过 SDK 自动刷新 `source_updated_at`；serving 写入由
  SDK 自动刷新 `serving_updated_at`。绕过它会破坏增量 checkpoint 的有效性。
- JSON schema 字段在 SDK 边界是 JSON 字符串；解析后必须重新序列化，不能返回 Python
  `dict/list`。
- 同一 pipeline 内一个 output field 只能由一个 stage 拥有。坏数据应明确报错；只有业务
  约定为 best effort 的字段才返回 `None`。

### `dependencies` 怎么写

只有当前 stage **必须读取前序 stage 生成的 patch**，或者没有前序结果就不能正确执行时，
才声明 `dependencies=("previous_stage_name",)`。两个 stage 触发条件相同、执行顺序看起来相邻，
或者更新不同字段，都不构成依赖；这种情况保持 `dependencies=()`。

依赖 stage 对某条记录跳过、但当前 stage 又适用时，该记录会失败。因此条件 stage 之间声明
依赖时，必须保证 downstream 适用的每条记录上 upstream 也会执行。引擎会根据依赖做拓扑
排序；无依赖 stage 使用 pipeline 中的声明顺序作为稳定顺序。

## 从开发到接入

1. 在 `wt_sdk/etl/stages/<stage_name>.py` 实现 stage，并从 `stages/__init__.py` 导出。
2. 添加 `wt_sdk/etl/tests/unit/test_<stage_name>.py`，至少测试：适用/不适用、正常 patch、
   坏数据、重复执行幂等。若有 dependency，再测试前序 patch 确实被读取。
3. 把 stage 显式加入目标 `pipelines/<pipeline_name>.py` 使用的 factory/builder。不要依赖目录
   自动发现；stage 集合或结果语义变化时升级 pipeline version。
4. 在不连接数据库的情况下检查整条 DAG：

```bash
python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --validate-only

python -m wt_sdk.etl.cli.run \
  --pipeline landing_to_serving_pipeline \
  --list-stages
```

5. 运行默认单元测试：

```bash
python -m pytest -q
```

## 贡献者自己测试完整 pipeline

可以，而且每个 stage PR 都应在 `landing_test` 自测“自己的 stage + 该 pipeline 原有
stages”。推荐使用 `run_sessions()`：它会真实读取 landing、执行 canonical pipeline 并写入
目标表，但不创建或推进 checkpoint，清理最简单。

测试必须使用 UUID 生成独立的 `job_id/session_id/id`，只操作 `landing_test/serving_test`，
并在 `finally` 中同时删除、验证两张表。可直接复用
`wt_sdk.etl.tests.integration.helpers`：

```python
import uuid

from wt_sdk import WTGatewayClient
from wt_sdk.etl import ETLEngine, SessionKey, load_pipeline
from wt_sdk.etl.tests.integration.helpers import (
    TEST_TABLE_CONFIG,
    cleanup_test_trajectory,
)


def test_my_stage_inside_complete_pipeline():
    suffix = uuid.uuid4().hex
    job_id = f"my-dataset#my-harness#my-model#stage-test#20260805#dev#{suffix}"
    session_id = f"session-{suffix}"

    with WTGatewayClient(config=TEST_TABLE_CONFIG) as client:
        try:
            client.ingest_landing_batch(make_test_records(job_id, session_id, suffix))
            pipeline = load_pipeline("landing_to_serving_pipeline")
            summary = ETLEngine(client).run_sessions(
                pipeline,
                [SessionKey(job_id, session_id)],
            )
            assert summary.failed_rows == 0

            rows = client.query_data(
                filter_query=f"job_id = '{job_id}'",
                partition=job_id,
                table="serving_test",
                checkout_latest=True,
            )
            assert_my_stage_output(rows)
            assert_existing_stage_outputs(rows)
        finally:
            cleanup_test_trajectory(client, job_id)
```

真实测试命令：

```bash
set -a && source .env && set +a
WT_SDK_RUN_INTEGRATION=1 python -m pytest -q \
  wt_sdk/etl/tests/integration/test_<stage_name>.py
```

不要复制或修改表里的真实行，不要清空整张 test 表。若必须测试 incremental 模式，还要为
测试 pipeline 使用唯一 name/version，并在 `finally` 删除且验证对应 checkpoint；普通 stage
接入验收优先使用上面的 targeted session 模式。
