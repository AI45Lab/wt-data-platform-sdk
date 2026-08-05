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

from wt_sdk import LandingRecord, WTGatewayClient


def message(role: str, text: str) -> dict:
    return {"role": role, "content": text}


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
        # Trajectory payload columns are opaque JSON strings at the SDK boundary.
        messages=json.dumps([message("user", f"task input for step {step_id}")]),
        response=json.dumps(message("assistant", f"result for step {step_id}")),
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
    )  # source_updated_at is refreshed automatically.

    trajectory = client.query_data(
        scope,
        order_by="step_id",
        checkout_latest=True,
        exclude_none=True,  # Default: omit null table columns from each result.
        deserialize_json=True,  # Return JSON columns as dict/list values.
    )
    first_step_id = trajectory[0]["step_id"]  # query_data() returns List[dict].
    job_row_count = client.count_landing(partition=job_id)

    # Prefer this pruned lookup when both job_id and id are known.
    exact_event = client.query_data(
        f"job_id = '{job_id}' AND id = '{buffered[-1].id}'",
        limit=1,
    )
```

`query_data()` returns plain dictionaries and omits null table columns by
default; pass `exclude_none=False` when a complete schema-shaped payload is
required. JSON columns remain JSON strings by default and are never modified by
`exclude_none`; pass `deserialize_json=True` to return their documents as Python
`dict`/`list` values. Deserialization does not remove nulls inside JSON.

```python
deserialize_json=False  # LanceDB-native output: JSON strings (default).
deserialize_json=True   # SDK applies json.loads(): Python dict/list values.
```

`messages`, `response`, `chosen_trace`, `rejected_trace`, and `meta_json` use
Arrow `json<string>` in both landing and serving. Callers serialize the entire
JSON document with `json.dumps()` before ingestion. A trace is therefore a JSON
array encoded as one string, not an Arrow `list<struct>`. The SDK deliberately
does not enforce an OpenAI or provider-specific document shape; landing may keep
provider-native raw data in `meta_json`, while ETL writes normalized OpenAI-style
payload JSON into serving.

### SDK-managed timestamps

| Field | Unit | Meaning |
| --- | --- | --- |
| `created_at` | Existing caller-defined unit | Original trajectory creation time; immutable after ingestion. |
| `source_updated_at` | Unix epoch milliseconds | Last substantive source change that may affect ETL/serving output. Automatically initialized and refreshed by landing updates. |
| `serving_updated_at` | Unix epoch milliseconds | Last successful serving ingest/upsert publication. Always null in landing. |

Callers may provide `source_updated_at` when replaying or migrating records;
otherwise `LandingRecord`/`ServingRecord` initializes it. Serving writes preserve
that source value and stamp a fresh `serving_updated_at` on an internal copy, so
the caller's model is not mutated.

### 2. Consume Completed Events

A long-running trainer pulls one page at a time and saves the cursor only after
that page is processed successfully. `pull_data()` adds the `dataset_type`
filter; keep `job_id` in `where_sql` for HASH pruning. The placeholder
`load_checkpoint()`, `process_page()`, and `save_checkpoint()` calls below belong
to the consuming application; scope each durable checkpoint by consumer and
query/table identity.

```python
job_filter = "job_id = 'evaluation-run-001' AND is_terminal = True"
page_size = 1000
stored_cursor = load_checkpoint()  # Return None when no checkpoint exists.

with WTGatewayClient() as client:
    while True:
        page = client.pull_data(
            dataset_type="RL",
            where_sql=job_filter,
            cursor=stored_cursor,
            limit=page_size,
            checkout_latest=True,
            deserialize_json=True,
        )
        if page.empty:
            break

        process_page(page)
        next_cursor = client.extract_cursor(page)
        save_checkpoint(next_cursor)  # Persist only after processing succeeds.
        stored_cursor = next_cursor

        if len(page) < page_size:
            break

    # Optional: inspect the current high-water mark for this pull scope.
    latest_record = client.get_max_created_at(
        where_sql=f"dataset_type = 'RL' AND {job_filter}",
    )
```

`get_max_created_at()` is a cursor/watermark helper for `pull_data()` workflows,
not a separate data-retrieval mode. It is useful for initialization, monitoring,
or recovery checks; normal page advancement should still persist the cursor from
`extract_cursor(page)` only after the page is processed successfully.

For a convenient one-run scan or backfill, `iter_data_batches()` manages the
`created_at` cursor and yields DataFrame batches lazily:

```python
with WTGatewayClient() as client:
    for batch in client.iter_data_batches(
        dataset_type="RL",
        where_sql="job_id = 'evaluation-run-001'",
        chunk_size=1000,
        deserialize_json=True,
    ):
        print(f"received {len(batch)} rows")
```

`pull_data()` and `iter_data_batches()` keep landing as their default. External
consumers can pass `table=client.config.tables.serving_table`. Consumers that
need Python payload objects should also pass `deserialize_json=True` and handle
ordinary `dict/list` values rather than the former Arrow nested containers. Both
APIs use a `created_at` cursor, so they are not the formal export path when
multiple rows can share a timestamp.

### 3. Publish Enriched Data

After ETL or training selection, publish enriched records to serving. Landing
and serving use the same schema; fields that have not been enriched yet remain
null in landing. Vector search is not currently exposed.

```python
from wt_sdk import ServingRecord

serving_data = buffered[-1].model_dump()
serving_data.update(
    reward=1.0,
    step_reward=1.0,
    is_trainable=True,
    search_text="benchmark task final successful response",
    tags=["trainable", "successful"],
)
serving_record = ServingRecord(**serving_data)

with WTGatewayClient() as client:
    # ETL publication should be retryable and race-free.
    client.upsert_serving(serving_record)
    published = client.get_by_id(
        serving_record.id,
        exclude_none=True,
        deserialize_json=True,
    )
    assert published and published["id"] == serving_record.id

    matches = client.search(
        "successful",
        dataset_type="RL",
        tags=["trainable"],
        limit=20,
        deserialize_json=True,
    )
```

`get_by_id()` queries serving by default, or exactly the named `table`; it does
not fall back across the internal/external table boundary. Because an ID alone
cannot identify a HASH bucket, prefer `query_data()` with both `job_id` and `id`
on hot landing paths. Like `query_data()`, it returns a plain dictionary and
omits null table columns by default without modifying JSON strings.

ETL should publish complete records through `upsert_serving()` or
`upsert_serving_batch()`. These methods call dldb's native upsert path with
`columns=["id"]`; do not implement a query-then-ingest/update sequence. Repeated
upserts converge to the same business content, while `serving_updated_at`
records the most recent successful publication and may therefore change on a
retry. Every upsert record requires a non-empty, immutable `job_id`. dldb HASH
tables do not provide a uniqueness constraint across buckets, so callers must
keep IDs globally unique and must never move an existing ID to another
`job_id`. The append/add `ingest_serving(_batch)` methods remain available and
also stamp `serving_updated_at`.

For a formal offline export after serving data has been published, use
`export_data_batches()`. It defaults to serving, captures a complete unique-ID
manifest before yielding the first batch, and then validates every exact-ID
batch. Rows appended after manifest capture are excluded; duplicate IDs or source
rows deleted/changed so they no longer match cause a hard failure instead of
silent loss:

`export_data_batches()` requires a Python runtime built with standard-library
`sqlite3` support. No external SQLite service or additional pip package is
required.

```python
with WTGatewayClient() as client:
    for batch in client.export_data_batches(
        filter_query="dataset_type = 'RL' AND is_trainable = True",
        batch_size=5000,
        columns=["id", "job_id", "chosen_trace", "tags", "meta_json"],
    ):
        write_to_temporary_export(batch)

    publish_completed_export()
```

Treat serving rows selected for an export as immutable until iteration completes,
and publish a file only after the iterator is exhausted successfully. dldb does
not expose one atomic snapshot across all physical HASH buckets, so this manifest
and validation protocol provides a stable row set but cannot preserve pre-update
field values if a selected row is modified during export.

### 4. Maintain Incremental Indexes

Do not build indexes in the synchronous writer path. After a job finishes, or
from a background operations process, refresh only the bucket touched by that
job:

```python
with WTGatewayClient() as client:
    summary = client.maintain_table_indexes(
        "wind_tunnel_landing",
        partitions=["evaluation-run-001"],
    )
```

This creates missing configured indexes and runs dldb optimize so appended rows
enter existing indexes. Use `all_partitions=True` only for scheduled full-table
maintenance. The context manager closes the dldb session and emits its final
metrics summary when enabled.

### Choosing a Data Read API

| API | Accepted parameters | Return type | Pagination control | Default table | Best suited for |
| --- | --- | --- | --- | --- | --- |
| `query_data()` | `filter_query`, `limit`, `columns`, `partition`, `order_by`, `ascending`, `checkout_latest`, `table`, `exclude_none`, `deserialize_json` | One `List[dict]` | No cursor management; one query with optional `limit` | Landing | Interactive filtering, trajectory/detail lookup, Dashboard lists, small bounded result sets |
| `get_by_id()` | `record_id`, `table`, `exclude_none`, `deserialize_json` | One `dict`, or `None` | None; scans the selected table because ID cannot locate a HASH bucket | Serving | Occasional lookup when only a globally unique ID is known |
| `pull_data()` | `dataset_type`, `where_sql`, `start_time`, `end_time`, `cursor`, `order_by`, `ascending`, `limit`, `checkout_latest`, `table`, `deserialize_json` | One DataFrame page | Caller supplies, extracts, and persists the `created_at` cursor | Landing | Incremental consumers, polling, retryable processing, durable checkpoints |
| `iter_data_batches()` | `dataset_type`, `where_sql`, `start_time`, `end_time`, `chunk_size`, `order_by`, `ascending`, `table`, `deserialize_json` | Iterator yielding one DataFrame per batch | SDK advances the `created_at` cursor internally until exhausted | Landing | Convenient one-run scans, backfills, and offline processing where timestamp ties are acceptable |
| `export_data_batches()` | `filter_query`, `batch_size`, `columns`, `table`, `deserialize_json` | Iterator yielding one validated DataFrame per manifest batch | SDK first captures a complete unique-ID manifest, then fetches and verifies exact IDs | Serving | Formal offline exports requiring a fixed row set, duplicate-ID detection, and no timestamp-cursor gaps |
| `search()` | `query`, `limit`, `tags`, `where_sql`, `dataset_type`, `stream`, `table`, `search_fields`, `deserialize_json` | One DataFrame, or a one-frame iterator with `stream=True` | One bounded search | Serving | Dashboard keyword search over `search_text`, tags, and scalar filters |

`query_data()`, `pull_data()`, and `iter_data_batches()` default to landing and
accept `table=client.config.tables.serving_table`. `get_by_id()`, `search()`, and
`export_data_batches()` default to serving. Include `job_id` in filters whenever
possible for HASH bucket pruning.

`query_data()` and `get_by_id()` omit null table columns by default. Pass
`exclude_none=False` to retain them. All row-returning read APIs keep JSON as
strings by default; `deserialize_json=True` returns Python values while
preserving JSON-internal nulls. Malformed JSON remains unchanged as a string so
a presentation option cannot make an otherwise readable row fail.

## Client Interface

### Landing Data

| Method | Purpose |
| --- | --- |
| ingest_landing(record) | Write one LandingRecord. |
| ingest_landing_batch(records) | Write a list of records or LandingRecordBatch. |
| query_data(filter_query, ..., table=None, exclude_none=True, deserialize_json=False) | Query landing by default, or a named table; always return `List[dict]`. |
| update_landing(filter_query, updates, ..., touch_source_updated_at=True) | Patch matching rows and refresh `source_updated_at` by default. SDK timestamps, `id`, `created_at`, and `job_id` are protected. |
| count_landing(partition=None) | Count rows, optionally in one raw job_id or hash bucket. |
| delete_landing(filter_query) | Delete matching landing records. |

query_data() and update_landing() accept a raw partition="job-id" for compatibility. On HASH tables the SDK converts it to the bucket and adds a job_id predicate. Prefer putting job_id in filter_query explicitly.

For an ordered or limited landing query, include job_id in filter_query. Cross-bucket order_by + limit is not a global merge sort in the current dldb partition model.

```python
with WTGatewayClient() as client:
    latest = client.query_data(
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

Existing `update_landing()` calls require no changes: the new keyword-only
`touch_source_updated_at` defaults to `True`. Updates to `is_trainable`, payloads,
rewards, `meta_json`, `agent_model`, or any enrichment consumed by serving must
keep that default, including in-place landing ETL. Use
`touch_source_updated_at=False` only for a purely operational/diagnostic patch
that cannot affect any downstream result. The ETL engine should avoid calling
update when values did not actually change. `update_landing()` returns caller
fields in `updated_fields` and submitted fields in `effective_updated_fields`;
dldb does not yet return exact matched or updated counts across logical
partitions.

### Serving, Search, and Pagination

| Method | Purpose |
| --- | --- |
| ingest_serving(record) / ingest_serving_batch(records) | Append processed serving records and stamp `serving_updated_at`. |
| upsert_serving(record) / upsert_serving_batch(records) | ETL publication by globally unique `id`; preserve `source_updated_at` and refresh `serving_updated_at`. |
| query_data(filter_query, ..., table=serving_table, exclude_none=True, deserialize_json=False) | Query serving with the same filtering and HASH pruning behavior; always return `List[dict]`. |
| count_serving(partition=None) / delete_serving(filter_query) | Operate on serving data. |
| search(query, ..., deserialize_json=False) | Search serving `search_text`, tags/SQL, or explicit scalar string fields. |
| get_tags_distribution() | Return serving tag frequencies. |
| get_by_id(record_id, table=None, exclude_none=True, deserialize_json=False) | Return one compact dictionary from serving by default, or exactly one named table. |
| pull_data(..., table=None, deserialize_json=False) / iter_data_batches(..., table=None, deserialize_json=False) | Read landing by default, or a named table, with manual-page or automatic-batch iteration. |
| export_data_batches(filter_query="", ..., table=None, deserialize_json=False) | Reliably export a fixed ID manifest from serving by default; validates each exact-ID batch. |

Vector search is not currently exposed by dldb. Keyword search defaults to
`search_text`; pass explicit scalar `search_fields` to search other string
columns. Opaque JSON traces are queried through the ETL-generated `search_text`,
normal SQL filters, or tags. `stream=True` returns an iterator containing the
current result frame.

### Index Maintenance

`maintain_table_indexes()` accepts one of the two production or two test table
names. The exact table name selects the landing or serving index definitions.
Callers must provide raw job IDs/HASH bucket integers or `all_partitions=True`;
the method creates missing indexes and runs dldb optimize by default.

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

Integration tests write a few uniquely scoped rows to the existing `landing_test`
and `serving_test` tables, then clean and verify them in `finally`. They target
these explicit test tables independently of `WT_SDK_PROFILE`; `WT_SDK_DB_URI`
chooses the database. Both tables must use the current `HASH(job_id)` schema.

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

# Delete one HASH bucket from a disposable test table
python scripts/ops/table_manager.py drop serving_test --partition 42
```

### Query and Inspect Data

```bash
# Count rows
python scripts/inspect/query_data.py --table wind_tunnel_landing --count

# Query selected columns
python scripts/inspect/query_data.py --table landing_test \
  --query "job_id = 'job-001'" --columns "id,session_id,step_id,is_terminal"

# Decode and inspect JSON payload columns without display truncation
python scripts/inspect/query_data.py --table landing_test --limit 1 \
  --show-nested --no-truncate

# Write results as pretty JSON, expanding JSON payload columns
python scripts/inspect/query_data.py --table landing_test --limit 1 \
  --output ./artifacts/landing_sample.json

# Show expected versus existing scalar indexes by partition
python scripts/inspect/show_table_indexes.py landing_test

# Scan a logical table for duplicate IDs
python scripts/inspect/scan_duplicate_id.py --table landing_test --max-output 100

# Legacy diagnostic for payload decode failures in pre-JSON-schema data
python scripts/inspect/scan_landing_nested_decode.py --table landing_test

# Inspect serving tags and ETL-generated search text
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
python scripts/ops/maintain_table_indexes.py \
  --table wind_tunnel_landing --all-partitions

# Create missing indexes and optimize every existing serving bucket
python scripts/ops/maintain_table_indexes.py \
  --table wind_tunnel_serving --all-partitions
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
