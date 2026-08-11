# Operational Scripts

`scripts/` contains manually invoked operational tools. They are not part of
the SDK's supported import API.

- `ops/`: table initialization, cleanup, index maintenance, and table management.
- `inspect/`: read-only inspection helpers for data, schemas, indexes, tags, and duplicate IDs.
- `delivery/`: read-only, stateless serving-data export commands intended for external users.
- `dev/`: helpers for initializing disposable test tables.
- ETL runtime, commands, tools, and tests all live in
  [`wt_sdk/etl/`](../wt_sdk/etl/); do not add ETL entry points back under `scripts/`.
- `migrations/`: completed, one-time migrations retained for operational history.

Use the same maintenance entry point for the two production and two test
tables. The exact table name selects the landing or serving index set; the
script creates missing per-bucket indexes and runs dldb optimize so appended
data enters existing indexes:

```bash
python scripts/ops/maintain_table_indexes.py \
  --table wind_tunnel_landing --all-partitions
python scripts/ops/maintain_table_indexes.py \
  --table wind_tunnel_serving --all-partitions
```

Load the integrating service's environment configuration before invoking a
script. For local development:

```bash
set -a && source .env && set +a
python scripts/inspect/query_data.py --table landing_test --count
```

External users can export production serving rows as sharded JSONL files with
the stateless delivery command. It targets `wind_tunnel_serving` by default,
writes 1,000 rows per file, and excludes the frontend-only `search_text` column
unless it is explicitly requested. Pass `--table serving_test` for integration
validation:

```bash
python scripts/delivery/export_serving_data.py \
  --filter "dataset_type = 'RL'" \
  --columns "id,job_id,serving_updated_at,chosen_trace,meta_json,tags" \
  --output-dir ./exports
```

See [`delivery/README.md`](delivery/README.md) for output layout, stateless
incremental semantics, and failure handling.

`scripts/inspect/query_data.py --query` accepts a standard SQL `WHERE`
predicate without the leading `WHERE`. For example:

```bash
python scripts/inspect/query_data.py \
  --table wind_tunnel_landing \
  --query "job_id LIKE '%panjia%' AND is_trainable = true" \
  --columns "id,job_id,session_id,step_id" \
  --output ./artifacts/panjia_rows.json
```

For the separate environment-config table, specify only the table name; the
script automatically uses `WT_SDK_ENV_CONFIG_DB_URI` and reads the latest
snapshot:

```bash
python scripts/inspect/query_data.py \
  --table evaluation_env_config \
  --query "job_id = 'job-001'" \
  --columns "id,job_id,env_id,env_name,group_id,finished"

python scripts/inspect/query_data.py \
  --table evaluation_env_config \
  --query "job_id = 'job-001'" \
  --output ./artifacts/job_001_env_configs.json
```

Production-changing commands require their own explicit confirmation flags.

Use `cleanup_data.py` for filtered deletes. `evaluation_env_config`
automatically uses `WT_SDK_ENV_CONFIG_DB_URI`; landing and serving tables use
`WT_SDK_DB_URI` unless `--db-uri` is supplied:

```bash
python scripts/ops/cleanup_data.py \
  --table evaluation_env_config \
  --query "job_id = 'gateway'" \
  --dry-run

python scripts/ops/cleanup_data.py \
  --table landing_test \
  --query "job_id = 'gateway'" \
  --dry-run
```

Use `update_table_rows.py` for a filtered operational patch against one of the
four active tables. The profile and role resolve the exact table; custom and
legacy table names are intentionally unsupported:

```bash
python scripts/ops/update_table_rows.py \
  --profile test --table landing \
  --query "job_id = 'job-123' AND session_id = 'session-1'" \
  --updates '{"is_session_completed": true}' --dry-run
```

Remove `--dry-run` to apply the patch. The command skips no-op rows, refreshes
`source_updated_at` for landing or `serving_updated_at` for serving, requires
exact-table confirmation unless `--yes` is supplied, and reports the count
verified through a latest-snapshot read.
