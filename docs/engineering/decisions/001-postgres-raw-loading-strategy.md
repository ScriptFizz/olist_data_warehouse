# ADR 001: PostgreSQL raw loading strategy

## Status

Accepted. Implementation in progress.

## Context

The Olist source is distributed as a static collection of complete CSV files,
but the portfolio pipeline should also demonstrate credible incremental batch
behavior.

Applying one loading strategy to every table would be incorrect:

- some tables have reliable business keys and mutable records;
- reviews do not have a trustworthy unique source key;
- geolocation and translation behave like reference snapshots;
- an absent row in a delta batch does not necessarily represent deletion.

## Decision

PostgreSQL raw tables use table-specific loading strategies.

| Table | Strategy | Identity |
|---|---|---|
| customers | upsert | customer_id |
| orders | upsert | order_id |
| order_items | upsert | order_id, order_item_id |
| payments | upsert | order_id, sequential |
| products | upsert | product_id |
| sellers | upsert | seller_id |
| reviews | append and deduplicate | deterministic record hash |
| geolocation | snapshot replacement | complete delivered snapshot |
| translation | snapshot replacement | complete delivered snapshot |

The original Olist CSV collection is treated as a bootstrap snapshot. Later
loads may contain new or changed records for upsert-managed tables.

Every published row will contain:

- `_ingestion_batch_id`;
- `_ingested_at`;
- `_record_hash`;
- `_warehouse_updated_at`.

Each run will be recorded in an ingestion audit table.

## Idempotency

For upsert tables:

- unseen business keys are inserted;
- existing keys with changed record hashes are updated;
- existing keys with unchanged hashes are no-ops.

For append-deduplicated tables:

- unseen record hashes are inserted;
- previously loaded hashes are ignored.

For snapshot-replacement tables:

- the complete table is replaced transactionally.

Replaying an identical batch must not create duplicates or change
`_warehouse_updated_at`.

## Failure recovery

All staging, audit, and publication operations occur in one PostgreSQL
transaction. If any table fails:

- no target table changes are committed;
- the ingestion run is recorded as failed in a separate failure-handling step;
- staging relations created by the failed transaction disappear on rollback.

## Deletions

Incremental batches do not infer deletion from absence.

Deletion requires either:

- an explicit source tombstone;
- or reconciliation against a later complete snapshot.

The initial implementation supports inserts and updates. Tombstone processing
is future work.

## dbt impact

Incremental dbt models must use affected business keys or
`_warehouse_updated_at`; they must not rely only on the maximum business event
date. This allows late changes to historical orders to be processed.

## Consequences

Benefits:

- loading semantics reflect each table's real grain;
- retries are idempotent;
- late updates can propagate downstream;
- ingestion runs and row changes are observable.

Costs:

- schema metadata and merge logic are more complex;
- snapshot deletion and delta deletion require distinct handling;
- reviews require hash-based identity rather than a conventional primary key.