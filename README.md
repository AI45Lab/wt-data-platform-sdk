# WT Data Platform SDK

<p align="center">
    <a href="README_CN.md">中文</a> &nbsp; | &nbsp; English
</p>

Python SDK for writing, querying, and managing agent trajectory data on the Wind Tunnel data platform. All database operations use dldb, which manages logical partitions on top of LanceDB.

## Install

dldb is installed from the public
[DeepLink-org/Persisting](https://github.com/DeepLink-org/Persisting) repository
declared in `pyproject.toml`. The supported Python versions are 3.10 through
3.12.

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

## End-to-End Best Practice

The following workflow models one agent evaluation job from raw trajectory
capture to training consumption and searchable serving data. Load credentials
and the table profile through environment variables, then reuse one client
within each process.

### 1. Capture and Finalize a Trajectory

Use `ingest_landing()` for an event that must be persisted immediately and
`ingest_landing_batch()` for buffered events. Generate `id` and `created_at`
upstream; keep `step_id` unique and increasing within a session.

```python
import json
import time
import uuid

from wt_sdk import ChatMessage, ContentItem, LandingRecord, WTGatewayClient


def message(role: str, text: str) -> ChatMessage:
    return ChatMessage(role=role, content=[ContentItem(type="text", text=text)])


job_id = "evaluation-run-001"       # One evaluation run and HASH partition key
session_id = str(uuid.uuid4())      # One agent trajectory
created_at = int(time.time())


def make_record(step_id: int, terminal: bool = False) -> LandingRecord:
    return LandingRecord(
        id=f"{session_id}:{step_id}",  # Caller-provided and globally unique
        dataset_type="RL",
        job_id=job_id,
        session_id=session_id,
        step_id=step_id,
        created_at=created_at + step_id,
        is_terminal=terminal,
        is_session_completed=terminal,
        messages=[message("user", f"task input for step {step_id}")],
        response=message("assistant", f"result for step {step_id}"),
        meta_json=json.dumps(
            {"task_id": "benchmark-task-42", "group_id": "group-a"}
        ),
    )


first = make_record(1)
buffered = [make_record(2), make_record(3, terminal=True)]
scope = f"job_id = '{job_id}' AND session_id = '{session_id}'"

with WTGatewayClient() as client:
    client.ingest_landing(first)             # Low-latency single write
    client.ingest_landing_batch(buffered)    # Higher-throughput buffered write

    # Reward may arrive after the terminal event has been stored.
    client.update_landing(
        f"{scope} AND step_id = 3",
        {"reward": 1.0, "step_reward": 1.0, "is_trainable": True},
    )

    trajectory = client.query_landing(
        scope,
        order_by="step_id",
        checkout_latest=True,
    )
    job_row_count = client.count_landing(partition=job_id)

    # Prefer this pruned lookup when both job_id and id are known.
    exact_event = client.query_landing(
        f"job_id = '{job_id}' AND id = '{buffered[-1].id}'",
        limit=1,
    )

    # Use only when the globally unique id is known but its job_id is not.
    fallback_event = client.get_by_id(buffered[-1].id)
```

`get_by_id()` checks serving first and then landing. Because an ID alone cannot
identify a landing HASH bucket, it is a convenient fallback rather than the
preferred hot-path lookup. When `job_id` is available, use `query_landing()`
with both values for physical partition pruning.

### 2. Consume Completed Events

A long-running trainer pulls one page at a time and saves the cursor only after
that page is processed successfully. `pull_data()` adds the `dataset_type`
filter; keep `job_id` in `where_sql` for HASH pruning.

```python
job_filter = "job_id = 'evaluation-run-001' AND is_terminal = True"
stored_cursor = None  # Load from the consumer's durable checkpoint.

with WTGatewayClient() as client:
    page = client.pull_data(
        dataset_type="RL",
        where_sql=job_filter,
        cursor=stored_cursor,
        limit=1000,
        checkout_latest=True,
    )
    if not page.empty:
        # Process page successfully, then persist the new checkpoint.
        next_cursor = client.extract_cursor(page)

    latest_record = client.get_max_created_at(
        where_sql=f"dataset_type = 'RL' AND {job_filter}",
    )
```

For an offline export or backfill, `fetch_data()` manages the `created_at`
cursor and yields DataFrame batches:

```python
with WTGatewayClient() as client:
    for batch in client.fetch_data(
        dataset_type="RL",
        where_sql="job_id = 'evaluation-run-001'",
        chunk_size=1000,
    ):
        print(f"received {len(batch)} rows")
```

### 3. Publish Searchable Data

After ETL or training selection, publish enriched records to serving. Keyword
search operates on `search_text`; vector search is not currently exposed.

```python
from wt_sdk import ServingRecord

serving_data = buffered[-1].model_dump()
serving_data.update(reward=1.0, step_reward=1.0, is_trainable=True)
serving_record = ServingRecord(
    **serving_data,
    search_text="benchmark task final successful response",
    tags=["trainable", "successful"],
)

with WTGatewayClient() as client:
    client.ingest_serving(serving_record)
    matches = client.search(
        "successful",
        dataset_type="RL",
        tags=["trainable"],
        limit=20,
    )
```

### 4. Maintain Incremental Indexes

Do not build indexes in the synchronous writer path. After a job finishes, or
from a background operations process, refresh only the bucket touched by that
job:

```python
with WTGatewayClient() as client:
    summary = client.maintain_landing_indexes(
        partitions=["evaluation-run-001"],
    )
```

This creates missing configured indexes and runs dldb optimize so appended rows
enter existing indexes. Use `all_partitions=True` only for scheduled full-table
maintenance. The context manager closes the dldb session and emits its final
metrics summary when enabled.

### Choosing a Read API

| Need | Recommended API | Why |
| --- | --- | --- |
| Reconstruct one trajectory | `query_landing(job_id + session_id)` | Ordered, HASH-pruned trajectory read |
| Read one event with its job known | `query_landing(job_id + id)` | Precise and HASH-pruned |
| Read one event when only its ID is known | `get_by_id(id)` | Convenient fallback across serving and landing |
| Poll newly completed events | `pull_data()` + `extract_cursor()` | One checkpointed page at a time |
| Export or backfill a job | `fetch_data()` | Iterates through DataFrame batches |
| Inspect a consumer watermark | `get_max_created_at()` | Returns the latest matching record |
| Count rows for one job | `count_landing(partition=job_id)` | Resolves and filters the correct HASH bucket |
| Search enriched output | `search()` | Queries serving `search_text` and tags |

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

Landing scalar indexes are configured for `dataset_type`, `is_terminal`, and
`is_trainable`. `maintain_landing_indexes()` accepts raw job IDs, maps them to
HASH buckets, creates missing indexes, and optionally runs dldb optimize. See
the end-to-end workflow above for the recommended background usage.

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

# Write full nested results as pretty JSON without flooding the console
python scripts/inspect/query_data.py --table landing_test --limit 1 \
  --output ./artifacts/landing_sample.json

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
