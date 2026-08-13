# Serving 数据交付

[English](README.md)

`export_serving_data.py` 是面向外部用户的只读数据导出命令，用于从 serving
表导出数据。默认读取生产表 `wind_tunnel_serving`。

该命令在不同运行之间不保存状态。每次运行会按照调用方提供的过滤条件固定一份
ID 清单，并完整导出该清单对应的数据。调用方负责构造后续的增量过滤条件、保存
游标，以及对不同批次之间可能重复的数据进行去重。

## 运行环境

请在独立的 Python 3.10～3.12 环境中运行。该环境需要安装 WT Data Platform SDK
及其兼容的 dldb/Lance 依赖。请激活平台管理员提供的环境。例如，平台提供的 Conda
环境名为 `wt-dldb-v1` 时：

```bash
conda activate wt-dldb-v1
```

环境名称、环境管理工具和安装路径都不是脚本契约的一部分。如果用户使用其他兼容
环境，应先激活对应环境，再使用该环境中的 `python` 运行脚本。如果环境或 `conda`
命令不可用，请先向平台管理员获取环境安装或激活方式。

运行前加载 SDK 所需的 S3 环境变量：

```bash
set -a && source .env && set +a
```

脚本默认固定读取 `wind_tunnel_serving`，不会因为 `.env` 中的表名覆盖项而被静默
切换到其他表。

## 使用方法

```bash
python scripts/delivery/export_serving_data.py \
  --filter "dataset_type = 'RL' AND serving_updated_at > 1786377600000" \
  --columns "id,job_id,serving_updated_at,chosen_trace,meta_json,tags" \
  --output-dir ./exports \
  --rows-per-file 1000
```

如需使用真实测试表进行集成验证，必须显式指定 `serving_test`：

```bash
python scripts/delivery/export_serving_data.py \
  --table serving_test \
  --filter "job_id = 'integration-job-id'" \
  --output-dir ./test-exports
```

脚本只允许读取 `wind_tunnel_serving` 和 `serving_test`，不接受任意表名或 landing
表名。

`--columns` 使用英文逗号分隔字段名。省略该参数时，脚本使用经过评审的默认对外
交付字段集合。`search_text` 是前端内部搜索字段，因此默认不导出；调用方仍可通过
`--columns` 显式选择它。

固定输出格式为 UTF-8 JSONL，每一行是一条独立的 JSON 记录。例如：

```json
{"id":"record-1","job_id":"job-1"}
{"id":"record-2","job_id":"job-1"}
```

`messages`、`chosen_trace` 和 `meta_json` 等 JSON 文档字段会转换为普通的嵌套 JSON
值。文本中裸露的 Unicode 行/段落分隔字符（`U+2028` 和 `U+2029`）会使用标准
JSON 转义形式写入，避免编辑器将其误认为 JSONL 记录边界。标准 JSON 解析器会
恢复原始字符值，不会改变交付数据的语义。

`--rows-per-file` 表示每个分片文件最多包含多少条记录，默认值为 `1000`。例如导出
2,450 条记录时，会生成：

```text
part-00000.jsonl  # 1,000 条
part-00001.jsonl  # 1,000 条
part-00002.jsonl  #   450 条
```

该参数限制的是记录数，不是文件字节大小；不同轨迹的内容长度不同，因此各分片的
实际文件大小可能不同。

## 推荐的增量拉取方式

脚本自身不保存 checkpoint。为了在下一次只拉取新发布到 serving 的数据，调用方
应在 `--columns` 中包含 `id` 和 `serving_updated_at`，并保存本次成功导出目录下
所有分片中的最大 `serving_updated_at`。

例如，获取某一次成功导出的最大时间戳：

```bash
jq -s 'map(.serving_updated_at) | max' \
  ./exports/export-20260811T103015000000Z-a3f92c01/part-*.jsonl
```

如果结果为 `1786377600000`，下一次将它作为包含边界的过滤条件：

```bash
python scripts/delivery/export_serving_data.py \
  --filter "job_id = 'job-001' AND serving_updated_at >= 1786377600000" \
  --columns "id,job_id,serving_updated_at,chosen_trace,meta_json,tags" \
  --output-dir ./exports
```

推荐使用 `>=` 而不是 `>`，因为多条记录可能具有相同的毫秒时间戳。这样会有意
重复导出边界时间戳上的记录，但不会因为时间戳相同而漏数；调用方应使用 `id`
去重。最大值必须从同一个成功导出目录下的所有分片共同计算，不要混合不同导出
目录的文件。

## 输出目录与失败处理

一次成功运行会发布一个新的独立目录：

```text
exports/
  export-20260811T103015000000Z-a3f92c01/
    part-00000.jsonl
    part-00001.jsonl
    manifest.json
    _SUCCESS
```

- `part-NNNNN.jsonl`：实际数据分片。
- `manifest.json`：记录表名、过滤条件、字段、总行数和各分片信息。
- `_SUCCESS`：表示本次导出已经完整成功。

调用方只应读取包含 `_SUCCESS` 的正式目录。

运行过程中，脚本先写入隐藏的 `.export-<id>.partial` 目录。全部数据和
`manifest.json` 写入成功后，脚本创建 `_SUCCESS`，再将整个目录重命名为正式的
`export-<id>` 目录。

如果读取或写文件中途失败，命令会返回非零退出码，并保留 `.partial` 目录用于
排查。用户可直接重新执行相同命令；新运行会生成新的 export ID 和新的固定 ID
清单，不会自动续写或消费旧的 `.partial` 目录。
