# Operational Scripts

`scripts/` contains manually invoked operational tools. They are not part of
the SDK's supported import API.

- `ops/`: table initialization, cleanup, index maintenance, and table management.
- `inspect/`: read-only inspection helpers for data, schemas, indexes, tags, and duplicate IDs.
- `dev/`: helpers for initializing disposable test tables.
- ETL runtime, commands, tools, tests, and archived historical ETL all live in
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
