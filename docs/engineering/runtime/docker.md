# Reproducible container execution

The project provides a reusable application image containing the packaged
Python pipeline and dbt adapters. PostgreSQL runs as a separate Compose service.

BigQuery remains an external warehouse and is not containerized.

## Services

### `postgres`

Provides the local PostgreSQL 16 warehouse and persists its data in the
`postgres_data` named volume.

The host connects through `localhost:5433` by default.

### `pipeline`

A short-lived command environment containing:

- the `olist_dw` Python package;
- Python ETL console commands;
- dbt Core;
- PostgreSQL and BigQuery dbt adapters.

It belongs to the `tools` Compose profile because it is not a continuously
running service. Commands are executed using `docker compose run --rm`.

Inside the Compose network, the pipeline connects to PostgreSQL using
`postgres:5432`. The host port mapping is not used between containers.

## Initial setup

Copy the environment template:

```bash
cp .env.example .env