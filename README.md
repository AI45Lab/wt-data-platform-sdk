# WT Data Platform SDK

Python SDK for writing, querying, and managing agent trajectory data on the Wind Tunnel data platform. All database operations use dldb, which manages logical partitions on top of LanceDB.

## Install

dldb is currently installed from the repository declared in pyproject.toml.

```bash
python -m pip install -e .
python -m pip install -e ".[dev]"  # Development and test dependencies
```

## Configure Before Integrating

The SDK reads process environment variables when a client is created. Existing application code can keep using WTGatewayClient().

### Use .env locally

Copy [.env.example](.env.example) into the integrating service as .env, replace the placeholders, and keep the real file out of version control.

```bash
# .env
WT_SDK_DB_URI=s3://your-dldb-bucket
WT_SDK_ENV_CONFIG_DB_URI=s3://your-env-config-bucket
WT_SDK_S3_ENDPOINT=http://your-s3-endpoint:8060
WT_SDK_S3_ALLOW_HTTP=true
AWS_ACCESS_KEY_ID=replace-with-your-access-key
AWS_SECRET_ACCESS_KEY=replace-with-your-secret-key
AWS_EC2_METADATA_DISABLED=true
WT_SDK_PROFILE=production
```

Load it before starting the application:

```bash
set -a && source .env && set +a
python your_service.py
```

### Export variables directly

For CI, systemd, Kubernetes, or another deployment system, inject the same variables directly:

```bash
export WT_SDK_DB_URI=s3://your-dldb-bucket
export WT_SDK_ENV_CONFIG_DB_URI=s3://your-env-config-bucket
export WT_SDK_S3_ENDPOINT=http://your-s3-endpoint:8060
export WT_SDK_S3_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=replace-with-your-access-key
export AWS_SECRET_ACCESS_KEY=replace-with-your-secret-key
export AWS_EC2_METADATA_DISABLED=true
export WT_SDK_PROFILE=production
python your_service.py
```

Docker Compose can reuse the same .env:

```yaml
services:
  app:
    env_file:
      - .env
```

### Table Profiles

WT_SDK_PROFILE selects default logical tables. Explicit GatewayConfig values and method arguments such as search(table="...") take precedence.

| Profile | Landing table | Serving table |
| --- | --- | --- |
| production or omitted | wind_tunnel_landing | wind_tunnel_serving |
| test | landing_test | serving_test |

Set WT_SDK_PROFILE=test to use test tables without changing application code.

`EnvConfigManager` uses the separate `WT_SDK_ENV_CONFIG_DB_URI` database for
`evaluation_env_config`; it is not affected by `WT_SDK_PROFILE`. The endpoint
and AWS credentials above are shared by both databases. An explicit `db_uri=`
passed to `EnvConfigManager` takes precedence.

## Quick Start

Landing uses HASH(job_id) with 128 buckets. Include job_id in landing reads and updates so the SDK can prune to one physical bucket.

```python
import time

from wt_sdk import ChatMessage, ContentItem, LandingRecord, WTGatewayClient

record = LandingRecord(
    id="trajectory-step-001",
    dataset_type="RL",
    job_id="job-001",
    session_id="session-001",
    step_id=1,
    created_at=int(time.time()),
    is_terminal=False,
    messages=[ChatMessage(role="user", content=[ContentItem(type="text", text="Solve the task")])],
    response=ChatMessage(role="assistant", content=[ContentItem(type="text", text="Working on it")]),
)

with WTGatewayClient() as client:
    client.ingest_landing(record)
    steps = client.query_landing(
        "job_id = 'job-001' AND session_id = 'session-001'",
        order_by="step_id",
    )
```

id and created_at are caller-provided. dt and blob_manifest are derived during record conversion. Within one session, step_id should be unique and monotonically increasing.

## Client Interface

### Landing Data

| Method | Purpose |
| --- | --- |
| ingest_landing(record) | Write one LandingRecord. |
| ingest_landing_batch(records) | Write a list of records or LandingRecordBatch. |
| query_landing(filter_query, ...) | Query landing records, or return a DataFrame with as_dataframe=True. |
| update_landing(filter_query, updates, ...) | Update matching records. id, created_at, and job_id are protected. |
| count_landing(partition=None) | Count rows, optionally in one raw job_id or hash bucket. |
| delete_landing(filter_query) | Delete matching landing records. |

query_landing() and update_landing() accept a raw partition="job-id" for compatibility. On HASH tables the SDK converts it to the bucket and adds a job_id predicate. Prefer putting job_id in filter_query explicitly.

For an ordered or limited landing query, include job_id in filter_query. Cross-bucket order_by + limit is not a global merge sort in the current dldb partition model.

```python
with WTGatewayClient() as client:
    latest = client.query_landing(
        "job_id = 'job-001' AND session_id = 'session-001'",
        order_by="step_id",
        ascending=False,
        limit=1,
    )
    result = client.update_landing(
        "job_id = 'job-001' AND session_id = 'session-001' AND step_id = 1",
        {"is_terminal": True, "is_trainable": True},
    )
```

update_landing() returns an execution acknowledgement. dldb does not yet return exact matched or updated counts across logical partitions.

### Serving, Search, and Pagination

| Method | Purpose |
| --- | --- |
| ingest_serving(record) / ingest_serving_batch(records) | Write processed serving records. |
| count_serving(partition=None) / delete_serving(filter_query) | Operate on serving data. |
| search(query, ...) | SQL-like keyword search. Defaults to the serving table. |
| get_tags_distribution() | Return serving tag frequencies. |
| get_by_id(record_id) | Check serving, then landing, for an ID. |
| pull_data(...) / fetch_data(...) | Read landing data with cursor pagination or batches. |
| get_max_created_at(where_sql) / extract_cursor(df) | Build cursor-based readers. |

Vector search is not currently exposed by dldb. search() accepts keyword queries; stream=True returns an iterator containing the current result frame.

### Index Maintenance

Landing scalar indexes are configured for dataset_type, is_terminal, and is_trainable. Keep indexing out of the synchronous writer path:

```python
with WTGatewayClient() as client:
    client.maintain_landing_indexes(all_partitions=True)
```

maintain_landing_indexes() creates missing per-bucket indexes and optionally runs dldb optimize.

## Timing and Metrics

```bash
WT_SDK_DLDB_MODEL=metrics
WT_SDK_LOG_DLDB_TIMING=1
WT_SDK_DLDB_METRICS_LOG=./wt_metrics_log.jsonl
```

In metrics mode, client.close() returns the dldb session summary and appends per-call events plus a summary event to the configured JSONL file.

## Run Tests

### Unit tests

Unit tests use fakes and do not connect to S3 or dldb:

```bash
pytest -q
```

The timing test writes a readable JSONL example to the ignored
`tests/artifacts/metrics_log.txt`.

### Real DLDB/S3 integration tests

Integration tests write a few rows to the existing `landing_test` table and clean them up. They always target `landing_test`, independent of `WT_SDK_PROFILE`; `WT_SDK_DB_URI` chooses the database. The table must use the current HASH(job_id) schema.

```bash
set -a && source .env && set +a
WT_SDK_RUN_INTEGRATION=1 python -m pytest -q tests/integration
```

WT_SDK_RUN_INTEGRATION=1 is a pytest safety switch, not an SDK runtime setting.

## Operational Scripts

Load the same .env before running scripts:

```bash
set -a && source .env && set +a
```

### Table Management

```bash
# List logical tables
python scripts/ops/table_manager.py list

# List another dldb database
python scripts/ops/table_manager.py list --db-uri s3://my-dldb-bucket

# Inspect schema, partition metadata, and scalar indexes
python scripts/ops/table_manager.py show-schema wind_tunnel_landing

# List physical dldb/Lance tables behind a logical table
python scripts/ops/table_manager.py show-physical landing_test

# Inspect the separate environment-config table
python scripts/ops/table_manager.py show-schema evaluation_env_config \
  --db-uri "$WT_SDK_ENV_CONFIG_DB_URI"

# Environment-config initialization is destructive; preview or explicitly confirm it
python scripts/ops/init_evaluation_env_table.py --dry-run
python scripts/ops/init_evaluation_env_table.py --confirm-recreate

# Interactive delete: type the exact table name, then DROP
python scripts/ops/table_manager.py drop landing_test

# Non-interactive delete: repeat the exact target
python scripts/ops/table_manager.py drop landing_test \
  --force --confirm-table landing_test

# Delete one VALUE partition (uses the same two confirmations)
python scripts/ops/table_manager.py drop serving_test --partition SFT
```

### Query and Inspect Data

```bash
# Count rows
python scripts/inspect/query_data.py --table wind_tunnel_landing --count

# Query selected columns
python scripts/inspect/query_data.py --table landing_test \
  --query "job_id = 'job-001'" --columns "id,session_id,step_id,is_terminal"

# Inspect nested values without display truncation
python scripts/inspect/query_data.py --table landing_test --limit 1 \
  --show-nested --no-truncate

# Show expected versus existing scalar indexes by partition
python scripts/inspect/show_table_indexes.py landing_test

# Scan a logical table for duplicate IDs
python scripts/inspect/scan_duplicate_id.py --table landing_test --max-output 100

# Locate HASH buckets and candidate rows with unreadable nested fields
python scripts/inspect/scan_landing_nested_decode.py --table landing_test

# Inspect serving tags or serving text search behavior
python scripts/inspect/get_unique_tags.py --table wind_tunnel_serving
python scripts/inspect/check_search_text.py --table wind_tunnel_serving
```

### Cleanup and Indexes

```bash
# Preview matching landing data before deletion
python scripts/ops/cleanup_data.py --table landing_test \
  --query "job_id = 'job-001'" --dry-run

# Delete matching test data
python scripts/ops/cleanup_data.py --table landing_test \
  --query "job_id = 'job-001'"

# Create missing indexes and optimize every existing landing bucket
python scripts/ops/maintain_landing_indexes.py \
  --table wind_tunnel_landing --all-partitions

# General missing-index maintenance for a logical table
python scripts/ops/add_missing_indexes.py wind_tunnel_serving
```

scripts/dev contains disposable test-table setup helpers. scripts/migrations contains completed historical migrations and is not routine setup. See [scripts/README.md](scripts/README.md) for the full layout.

## Repository Layout

```text
wt_sdk/                 Public SDK package
scripts/ops/            Operational commands
scripts/inspect/        Read-only diagnostics
scripts/dev/            Disposable test-table setup
scripts/migrations/     Historical one-time migrations
tests/unit/              Default hermetic test suite
tests/integration/       Explicit real DLDB/S3 tests
```

## License

MIT
