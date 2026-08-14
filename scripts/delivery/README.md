# Serving Data Delivery

[中文说明](README_CN.md)

`export_serving_data.py` is the read-only command delivered to external users
for exporting rows from a serving table. It defaults to the production
`wind_tunnel_serving` table.

`count_dataset.py` counts rows by the dataset name in `job_id`.

The command is intentionally stateless. It exports exactly the ID manifest
captured for the supplied filter. Callers own incremental filter construction,
cursor storage, and deduplication between separate invocations.

## Configuration

Run the command in a dedicated Python 3.10-3.12 environment containing the WT
Data Platform SDK and its compatible dldb/Lance dependencies. Activate the
environment provided by your platform administrator. For example, when the
provided Conda environment is named `wt-dldb-v1`:

```bash
conda activate wt-dldb-v1
```

The environment name, environment manager, and installation location are not
part of the script contract. Users with another compatible environment should
activate it instead and run the command with that environment's `python`
executable. If the environment or `conda` command is unavailable, obtain the
environment setup instructions from the platform administrator before running
the export.

Load the SDK's normal S3 environment variables before running the command. The
command pins the logical table to `wind_tunnel_serving` by default; an
environment-provided table-name override cannot silently redirect it.

```bash
set -a && source .env && set +a
```

## Usage

```bash
python scripts/delivery/export_serving_data.py \
  --filter "dataset_type = 'RL' AND serving_updated_at > 1786377600000" \
  --columns "id,job_id,serving_updated_at,chosen_trace,meta_json,tags" \
  --output-dir ./exports \
  --rows-per-file 1000
```

For real integration validation against the test table, select it explicitly:

```bash
python scripts/delivery/export_serving_data.py \
  --table serving_test \
  --filter "job_id = 'integration-job-id'" \
  --output-dir ./test-exports
```

Only `wind_tunnel_serving` and `serving_test` are accepted; arbitrary and
landing table names are rejected.

When `--columns` is omitted, the command uses the reviewed external-delivery
column set. `search_text` is excluded by default because it is an internal
frontend-search field. A caller may still request it explicitly.

The fixed output format is UTF-8 JSONL: one JSON object per line. JSON payload
columns such as `messages`, `chosen_trace`, and `meta_json` are decoded into
normal nested JSON values. Literal Unicode line/paragraph separator characters
(`U+2028` and `U+2029`) are written using their standard JSON escape sequences
so editors do not mistake them for JSONL record boundaries. Standard JSON
parsers restore the original character values without changing the delivered
data.

## Count Rows by Dataset

```bash
python scripts/delivery/count_dataset.py \
  --table wind_tunnel_serving \
  --concurrency 5 \
  --verbose
```

- `--table`: target table; defaults to `wind_tunnel_serving`.
- `--concurrency`: concurrent query limit; defaults to `5`.
- `--verbose`: print the executed query commands; disabled by default.

Use lower concurrency for large tables; active writes can cause small timing differences.

## Recommended Incremental Usage

The command does not keep a checkpoint. To pull only newly published serving
rows on a later invocation, callers should include both `id` and
`serving_updated_at` in `--columns` and persist the maximum
`serving_updated_at` found across all part files from the completed export.

For example, obtain the maximum timestamp from one specific successful export:

```bash
jq -s 'map(.serving_updated_at) | max' \
  ./exports/export-20260811T103015000000Z-a3f92c01/part-*.jsonl
```

If the returned value is `1786377600000`, use it as an inclusive lower bound
in the next invocation:

```bash
python scripts/delivery/export_serving_data.py \
  --filter "job_id = 'job-001' AND serving_updated_at >= 1786377600000" \
  --columns "id,job_id,serving_updated_at,chosen_trace,meta_json,tags" \
  --output-dir ./exports
```

Use `>=` rather than `>` because multiple records may share the same
millisecond timestamp. This deliberately re-exports records on the boundary;
the caller should deduplicate them by `id`. Always compute the maximum from all
part files in one completed directory, and do not combine part files from
different export directories.

## Output and Failure Handling

A successful invocation publishes a new directory:

```text
exports/
  export-20260811T103015000000Z-a3f92c01/
    part-00000.jsonl
    part-00001.jsonl
    manifest.json
    _SUCCESS
```

Consumers should read only directories containing `_SUCCESS`.

The command writes first to a hidden `.export-<id>.partial` directory. On
success, the whole directory is renamed to its final name. If any read or write
fails, the command exits nonzero and leaves the partial directory in place for
inspection. Re-running the same command creates a new export ID and captures a
new matching ID manifest; incomplete partial directories are never resumed or
consumed automatically.
