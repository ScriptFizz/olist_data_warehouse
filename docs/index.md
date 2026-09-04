# Olist Analytics Engineering Warehouse

This documentation describes the executable architecture of the Olist
warehouse: a Python ingestion boundary, PostgreSQL for local development, dbt
for analytical transformations, and Kestra for orchestration.

## Engineering objective

The project prioritizes predictable reruns and explicit data contracts over
infrastructure scale. The Olist source is static, but the implementation treats
it as an initial snapshot followed by possible update batches. That makes
failure recovery and late-data behavior concrete without pretending the source
is a streaming system.

## Where to start

- [Architecture](engineering/architecture.md) explains component boundaries
  and the end-to-end data flow.
- [PostgreSQL ingestion](engineering/etl/postgres_ingestion.md) explains batch
  identity, loading strategies, transactions, and recovery.
- [dbt warehouse](engineering/warehouse.md) defines model layers and grain.
- [Docker](engineering/runtime/docker.md) covers reproducible manual execution.
- [Kestra](engineering/orchestration/kestra.md) covers orchestration and its
  local security boundary.
- [Testing and CI](engineering/testing.md) explains the verification layers.
- [Architecture decisions](engineering/decisions/001-postgres-raw-loading-strategy.md)
  record the reasoning behind choices and their consequences.

The repository
[README](https://github.com/ScriptFizz/olist_data_warehouse#readme) provides the
shortest setup and execution path.
