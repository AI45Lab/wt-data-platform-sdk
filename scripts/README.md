# Operational Scripts

`scripts/` contains manually invoked operational tools. They are not part of
the SDK's supported import API.

- `ops/`: table initialization, cleanup, index maintenance, and table management.
- `inspect/`: read-only inspection helpers for data, schemas, indexes, tags, and duplicate IDs.
- `dev/`: helpers for initializing disposable test tables.
- `etl/`: supported ETL entry points and their documentation.
- `migrations/`: completed, one-time migrations retained for operational history.

Load the integrating service's environment configuration before invoking a
script. For local development:

```bash
set -a && source .env && set +a
python scripts/inspect/query_data.py --table landing_test --count
```

Production-changing commands require their own explicit confirmation flags.
