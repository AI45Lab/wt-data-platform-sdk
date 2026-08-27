# Tests

- `unit/` contains hermetic tests using fakes and is the default `pytest` target.
- `integration/` accesses DLDB/S3 and may write disposable test data. Landing
  and serving tests use the test tables; the environment-config checkout-latest
  test writes one uniquely named row to `env_config_test` and deletes it
  in cleanup. Run it only with an explicitly configured environment:

```bash
set -a && source .env && set +a
WT_SDK_RUN_INTEGRATION=1 pytest tests/integration
```
