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

The separate, unpartitioned environment-config table has its own maintenance
command and automatically resolves `WT_SDK_ENV_CONFIG_DB_URI` (default
`s3://wind-tunnel-env-config`):

```bash
python scripts/ops/maintain_env_config_indexes.py --dry-run
python scripts/ops/maintain_env_config_indexes.py
```

It creates missing indexes configured in
`wt_sdk/core/evaluation_env_schema.py`, then calls dldb's full `optimize()` to
compact fragments, clean old versions according to dldb's retention policy,
and refresh index coverage. Pass `--no-optimize` only to create missing indexes
without the full maintenance pass.

Load the integrating service's environment configuration before invoking a
script. For local development:

```bash
set -a && source .env && set +a
python scripts/inspect/query_data.py --table landing_test --count
```

### HASH Partition Fragment and Index Status

Inspect dldb's aggregate fragment statistics and per-index row coverage for
selected HASH buckets, or scan all logical buckets. The command is read-only;
expected index names and types come from `wt_sdk/core/schemas.py`:

```bash
python scripts/inspect/show_partition_status.py \
  --table landing_test --partition 34 --partition 94
python scripts/inspect/show_partition_status.py \
  --table wind_tunnel_landing --all-partitions
```

Add `--show-all-indexes` to print the coverage of every existing index. Without
it, the command prints details only for missing indexes, unexpected indexes,
and indexes with an unindexed tail. Results are flushed one bucket at a time so
a long `--all-partitions` scan remains visible in a terminal or redirected log.

The summary columns are:

| Column | Meaning |
| --- | --- |
| `BUCKET` | HASH bucket number, calculated as `stable_hash(job_id) % partitions`. |
| `STATE` | Combined bucket health classification described below. |
| `ROWS` | Number of live rows in the latest physical-table version. |
| `VER` | Current Lance version number for the physical bucket table. |
| `BYTES` | Current partition size reported by Lance. It is not total S3 usage including historical versions. |
| `FRAGS` | Number of fragments referenced by the current version. |
| `SMALL` | Number of fragments Lance classifies as small. |
| `MIN` | Row count of the smallest fragment. |
| `P50` | Median fragment row count. |
| `P99` | 99th-percentile fragment row count. |
| `MAX` | Row count of the largest fragment. |
| `IDX` | Actual index count divided by the SDK-expected index count, for example `8/8`. |
| `TAIL` | Number of existing indexes that do not fully cover the latest rows. |

`STATE` can be one of the following values, or a `+`-joined combination of
the applicable problem states:

| State | Meaning | Typical action |
| --- | --- | --- |
| `ok` | The non-empty bucket has every expected index, no index tail, and no actionable multi-fragment condition. | None. |
| `unmaterialized` | No physical table currently exists for this logical bucket, normally because it has never received data. | None unless a write to this bucket is unexpectedly failing. |
| `empty_shell` | The physical bucket table exists but its latest version has zero live rows. | Usually none. |
| `missing_idx` | A non-empty bucket is missing at least one index configured in `wt_sdk/core/schemas.py`. | Create the missing indexes. |
| `index_tail` | At least one existing index does not cover all current rows. Unindexed rows remain queryable through scanning. | Refresh or optimize the indexes. |
| `fragmented` | The bucket has more than one fragment and at least one is classified as small. | Consider fragment compaction. |
| `stats_unavailable` | dldb found the physical bucket but returned no table/fragment statistics. | Inspect the bucket and dldb logs. |
| `error` | `partition_status()` failed for this bucket; the next line contains the exception. | Investigate the reported error. |

For example, `missing_idx+index_tail+fragmented` means all three maintenance
conditions are present. `FRAGS=1 SMALL=1` is not classified as `fragmented`:
although Lance calls the only fragment small, there is no second fragment to
merge with it. `unmaterialized` is normally an unused bucket rather than a
maintenance problem.

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
For the four active landing/serving tables, exact `job_id = '...'` filters use
SDK HASH bucket pruning. Filtered queries and filtered `--count` skip the full
table count by default; add `--with-total-count` only when needed.
Use `--distinct` when you need the number and values of unique fields or field
combinations.

```bash
python scripts/inspect/query_data.py \
  --table wind_tunnel_landing \
  --query "job_id LIKE '%panjia%' AND is_trainable = true" \
  --columns "id,job_id,session_id,step_id" \
  --output ./artifacts/panjia_rows.json

python scripts/inspect/query_data.py \
  --table wind_tunnel_landing \
  --distinct job_id

python scripts/inspect/query_data.py \
  --table wind_tunnel_landing \
  --query "job_id = 'job-001'" \
  --distinct session_id \
  --count
```

Use `count_job_prefix_delivery.py` to update the recurring dataset progress
table from the production serving table. The default key is the first four
`job_id` components: `dataset#harness#model#task`. For `cybergym` only, the
script additionally splits rows by the later `level*` component, so a job such
as `cybergym#opencode#kimi-k3#find#20260817#jz#level1#1-500-01` is reported
under `cybergym#opencode#kimi-k3#find#level1`.

It reports two read-only serving counts:

- `成功条数`: serving rows in the group whose `reward > 0`.
- `总条数`: all serving rows in the group.

For a fixed report, pass the prefixes you care about. The command prints a
Markdown-style table to the console and reads only narrow `job_id` and `reward`
columns instead of wide JSON payload columns. Prefix filters cannot use HASH
partition pruning, so this mode may still need to scan across serving buckets:

```bash
python scripts/inspect/count_job_prefix_delivery.py \
  --profile production \
  --prefix 'cybergym#opencode#kimi-k3#find#level1' \
  --prefix 'cybergym#opencode#kimi-k3#find#level2' \
  --prefix 'vulhub#opencode#kimi-k3#exploit' \
  --prefix 'vulhub#codex#kimi-k3#exploit' \
  --prefix 'vulhub#claude-code#kimi-k3#exploit' \
  --task-label zh
```

For faster checks when the exact job IDs are known, pass `--job-id` or
`--job-id-file`. Exact job IDs let the SDK prune HASH buckets:

```bash
python scripts/inspect/count_job_prefix_delivery.py \
  --profile production \
  --job-id 'cybergym#opencode#kimi-k3#find#20260817#jz#level1#1-500-01' \
  --job-id 'vulhub#codex#kimi-k3#exploit#20260821222106#lml' \
  --task-label zh
```

For a longer fixed list, put one prefix per line in a file:

```text
cybergym#opencode#kimi-k3#find#level1
cybergym#opencode#kimi-k3#find#level2
cvefactory#opencode#kimi-k3#mining-patch
vulhub#opencode#kimi-k3#exploit
vulhub#codex#kimi-k3#exploit
vulhub#claude-code#kimi-k3#exploit
```

Then run:

```bash
python scripts/inspect/count_job_prefix_delivery.py \
  --profile production \
  --prefix-file ./artifacts/job_prefixes.txt \
  --task-label zh
```

If you also have a job-id file, prefer it for performance:

```bash
python scripts/inspect/count_job_prefix_delivery.py \
  --profile production \
  --job-id-file ./artifacts/job_ids.txt \
  --task-label zh
```

If no prefix is supplied, the script scans serving and reports all valid
serving groups it finds. Prefer explicit prefixes for routine status updates
because the output is stable and easier to paste into the tracking table.

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
`WT_SDK_DB_URI` unless `--db-uri` is supplied. For the four active
landing/serving tables, exact `job_id = '...'` filters use SDK HASH bucket
pruning and dry-runs read only lightweight preview/count columns:

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
