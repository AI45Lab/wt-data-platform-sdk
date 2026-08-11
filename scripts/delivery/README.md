# Serving Data Delivery

`export_serving_data.py` is the read-only command delivered to external users
for exporting rows from a serving table. It defaults to the production
`wind_tunnel_serving` table.

The command is intentionally stateless. It exports exactly the ID manifest
captured for the supplied filter. Callers own incremental filter construction,
cursor storage, and deduplication between separate invocations.

## Configuration

Load the SDK's normal S3 environment variables before running the command. The
command pins the logical table to `wind_tunnel_serving` by default; an
environment-provided table-name override cannot silently redirect it.

```bash
set -a && source .env && set +a
```

## Usage

```bash
.venv-dldb-v1/bin/python scripts/delivery/export_serving_data.py \
  --filter "dataset_type = 'RL' AND serving_updated_at > 1786377600000" \
  --columns "id,job_id,serving_updated_at,chosen_trace,meta_json,tags" \
  --output-dir ./exports \
  --rows-per-file 1000
```

For real integration validation against the test table, select it explicitly:

```bash
.venv-dldb-v1/bin/python scripts/delivery/export_serving_data.py \
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
normal nested JSON values.

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
