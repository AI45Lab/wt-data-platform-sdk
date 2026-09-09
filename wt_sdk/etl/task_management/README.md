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
single-job-at-a-time in v1.
