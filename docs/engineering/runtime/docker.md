# Reproducible container execution

The project provides a reusable application image containing the packaged
Python pipeline and dbt adapters. PostgreSQL runs as a separate Compose service.

BigQuery remains external and is not containerized.

## Services

### `postgres`

Runs PostgreSQL 16 and persists warehouse data in the `postgres_data` named
volume.

- From the host: `localhost:5433`
- From another Compose container: `postgres:5432`

### `pipeline`

A short-lived command environment containing:

- the installed `olist_dw` Python package;
- the ETL command-line applications;
- dbt Core;
- the PostgreSQL and BigQuery dbt adapters.

The service belongs to the `tools` Compose profile because it is used for
one-off commands rather than as a continuously running process.

## Initial setup

Create the local environment file:

```bash
cp .env.example .env
```

Set at least:

```dotenv
POSTGRES_DB=olist
POSTGRES_USER=olist
POSTGRES_PASSWORD=replace-with-a-local-password
POSTGRES_PORT=5433

KAGGLE_USERNAME=your-kaggle-username
KAGGLE_KEY=your-kaggle-api-key

KESTRA_DB_PASSWORD=replace-with-a-local-database-password
KESTRA_USERNAME=admin@localhost
KESTRA_PASSWORD=replace-with-a-local-admin-password
```

Do not commit `.env` or real credentials.

On Linux, export the host user's numeric identifiers so files created through
bind mounts remain owned by the host user:

```bash
export LOCAL_UID="$(id -u)"
export LOCAL_GID="$(id -g)"
```

These values may also be added to `.env`.

## Build and validate the runtime

Validate the Compose configuration:

```bash
docker compose config --quiet
```

Build the pipeline image:

```bash
docker compose build pipeline
```

Run the lightweight container smoke test:

```bash
./scripts/container_smoke_test.sh
```

The smoke test verifies:

- Compose configuration;
- image construction;
- installation of the Python package;
- installation of dbt;
- PostgreSQL readiness;
- connectivity from the pipeline container.

It intentionally does not download the Kaggle dataset.

## Start Kestra and deploy flows

Kestra flow definitions are stored under `kestra/flows`. Start the local
control plane and create or update all repository-managed flows with:

```bash
./scripts/deploy_kestra_flows.sh
```

The deployment is a separate step so flows using external plugins are parsed
only after the Kestra server and its bundled plugins are fully initialized.
The local UI is available at <http://localhost:8080>.

The deployment command authenticates using `KESTRA_USERNAME` and
`KESTRA_PASSWORD` from the untracked `.env` file. These credentials are passed
through the Kestra container environment and are not stored in flow YAML.

## Run the complete local pipeline

Start PostgreSQL:

```bash
docker compose up -d postgres
```

Download the raw source files:

```bash
docker compose run --rm --no-deps pipeline \
  olist-extract \
  --out-dir data/raw
```

Transform and validate the source data:

```bash
docker compose run --rm --no-deps pipeline \
  olist-transform \
  --raw-data-dir data/raw \
  --processed-data-dir data/processed
```

Load the validated data into PostgreSQL:

```bash
docker compose run --rm pipeline \
  olist-load-postgres \
  --processed-dir data/processed
```

Build and test the dbt warehouse:

```bash
docker compose run --rm pipeline \
  dbt build \
  --project-dir dbt \
  --profiles-dir dbt \
  --target postgres_dev
```

The `pipeline` service receives PostgreSQL connection settings through
environment variables and connects over the Compose network.

## Safe re-execution

The commands are designed to be run again safely:

- transformation rewrites deterministic processed outputs;
- raw PostgreSQL publication uses table-specific loading strategies;
- a failed multi-table publication is rolled back;
- ingestion attempts are recorded in the audit schema;
- unchanged upsert records are not rewritten;
- dbt facts update incrementally from source watermarks.

Re-running the complete workflow should not duplicate source records.

## Inspect the result

List tables:

```bash
docker compose exec postgres \
  psql \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "\dt raw.*"
```

Inspect dbt relations:

```bash
docker compose exec postgres \
  psql \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "\dt dbt_dev.*"
```

Inspect ingestion runs:

```bash
docker compose exec postgres \
  psql \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "select * from pipeline_metadata.ingestion_runs order by started_at desc limit 10;"
```

## Cleanup

Stop the containers while preserving PostgreSQL data:

```bash
docker compose down
```

Removing the named volume also deletes the local PostgreSQL warehouse:

```bash
docker compose down --volumes
```

Use the second command only when a completely clean database is intentional.

## BigQuery target

The same dbt project retains a `bigquery_dev` target. BigQuery credentials are
not included in the image and must be supplied at runtime.

PostgreSQL is the default local development target because it provides an
inexpensive and reproducible development environment. BigQuery remains the
optional cloud deployment target.
