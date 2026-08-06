# Operational Scripts

`scripts/` contains manually invoked operational tools. They are not part of
the SDK's supported import API.

- `ops/`: table initialization, cleanup, index maintenance, and table management.
- `inspect/`: read-only inspection helpers for data, schemas, indexes, tags, and duplicate IDs.
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

Production-changing commands require their own explicit confirmation flags.

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
