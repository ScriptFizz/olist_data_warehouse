# Testing and continuous integration

The test strategy places checks at the cheapest layer that can establish the
required behavior.

## Local checks

```bash
uv lock --check
uv run ruff check src tests
uv run mypy src tests
uv run pytest -m "not postgres"
uv run dbt parse --project-dir dbt --profiles-dir dbt --target postgres_dev
uv run mkdocs build --strict
```

These checks need no database credentials beyond the placeholder password used
to render the dbt profile.

## PostgreSQL integration tests

With the Compose database running:

```bash
RUN_POSTGRES_TESTS=1 uv run pytest -m postgres -v
```

Tests create isolated schemas and clean them up. They verify behavior that
cannot be established reliably with mocks:

- bulk staging and multi-table publication;
- rollback after a mid-batch failure;
- idempotent CLI reruns and table identity preservation;
- independently durable ingestion audit records;
- incremental dbt facts after a late correction.

## CI jobs

The GitHub Actions workflow has three independent jobs:

1. static analysis, unit tests, dbt parsing, documentation, Compose validation,
   and Python package construction;
2. PostgreSQL and dbt integration tests against an ephemeral service container;
3. application image construction and runtime import/dbt checks.

Normal pull-request CI intentionally excludes BigQuery integration. This keeps
feedback fast, avoids distributing cloud credentials to ordinary changes, and
keeps warehouse cost at zero. A future credentialed scheduled workflow could
exercise BigQuery separately without weakening the default CI boundary.
