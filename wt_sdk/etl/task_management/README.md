# ETL Task Management

This package is the job-level orchestration layer for ETL v1. It is separate
from the pipeline execution engine:

- `models.py` defines task states and the control-table schema.
- `store.py` persists one latest task row per `job_id` through dldb.
- `discovery.py` scans env completion state in bounded batches.
- `service.py` implements idempotent discovery/enqueue and bootstrap planning.
- `worker.py` runs one job at a time by invoking the existing ETL CLI for the
  enrichment and serving pipelines.

The command entry point is `python -m wt_sdk.etl.cli.tasks` (or the installed
`wt-etl-tasks` script). See
[`../ETL_TASK_MANAGEMENT_DESIGN.md`](../ETL_TASK_MANAGEMENT_DESIGN.md) for the
state machine, bootstrap procedure, and report layout.

The production task table must be initialized explicitly before starting the
worker:

```bash
python -m wt_sdk.etl.cli.tasks \
  --profile production \
  init --confirm-create
```

The state database is resolved from `WT_SDK_ETL_STATE_DB_URI` unless
`--state-db-uri` is provided. The worker is intentionally single-writer and
single-job-at-a-time in v1. It scans the env table every three hours by default,
enqueues every ready job found in that scan, and drains the entire queue in
FIFO order without waiting for the next scan.

Task timestamps retain their epoch-millisecond columns for ordering and also
store `*_at_text` display columns formatted as `YYYY-MM-DD HH:MM` in
`Asia/Shanghai`.

The task table's dldb metadata indexes are intentionally small: BTREE indexes
on `id`, `job_id`, `created_at_ms`, and `updated_at_ms`, plus a BITMAP index on
`status`. They are created or repaired by the explicit `init` command; they are
not represented inside the Arrow schema itself.

Start the long-running scheduler with:

```bash
python -m wt_sdk.etl.cli.tasks \
  --profile production \
  --state-db-uri s3://wind-tunnel-etl \
  worker --poll-seconds 10800
```

Run it in a process supervisor or tmux if it must survive an SSH disconnect.
Press `Ctrl-C` in the worker terminal, or send it `SIGTERM`, to request a
graceful stop: the active job finishes, and no further discovery or queued job
is started. `worker --once` performs one discovery and drains the resulting
queue before exiting.
