# Tests

- `unit/` contains hermetic tests using fakes and is the default `pytest` target.
- `integration/` accesses DLDB/S3 and may write disposable test data. Run it
  only with an explicitly configured environment:

```bash
set -a && source .env && set +a
WT_SDK_RUN_INTEGRATION=1 pytest tests/integration
```
