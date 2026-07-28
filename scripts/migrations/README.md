# Completed Migrations

These scripts document completed operational migrations. They are not normal
SDK setup commands and must not be run against a live environment without an
independent migration review.

- `landing_test_2026_05/`: historical test-table partition migrations.
- `landing_schema_2026_06/`: historical landing schema adjustment.
- `production_landing_2026_07/`: archive and rebuild procedure used to move
  production landing to `HASH(job_id)`.
