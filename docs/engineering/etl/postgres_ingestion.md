# PostgreSQL ingestion and publication

This document explains how a validated group of Olist tables becomes a safe,
auditable PostgreSQL raw-layer batch. It is a learning guide to the current
implementation rather than a list of future intentions.

## The main idea

A load is treated as one batch containing all nine processed Olist tables.
The pipeline first validates every table, assigns the batch an identity, stages
all rows, and only then changes the public raw tables.

```text
processed CSV files
        |
        v
Pandera and dataset validation
        |
        v
IngestionBatch + RUNNING audit row
        |
        v
COPY every table into private staging tables
        |
        v
publish every table in one transaction
   |                         |
 success                   failure
   |                         |
 commit raw tables         rollback raw tables
   |                         |
 SUCCEEDED audit row       FAILED audit row
```

The audit writes use independent database transactions. This separation is
important: if the raw transaction fails, its rollback must not erase the record
that explains that an ingestion attempt failed.

## Responsibilities by component

### `olist_tables.py`: the table registry

`TABLES` is the central declaration of what each Olist dataset means to the
pipeline. Each `TableConfig` connects a filename, its Pandera schemas, its
transformation function, and its publication strategy.

This prevents the CLI and database loader from maintaining separate lists of
tables and gradually disagreeing with one another.

### `tables.py`: loading policy types

`LoadStrategy` defines the three supported publication behaviors.
`TableConfig` validates policy at program startup. For example, an UPSERT table
must declare a business key, while a snapshot table must not declare one.

These checks turn configuration mistakes into early Python errors instead of
incorrect database loads.

### `ingestion_batch.py`: batch and record identity

`IngestionBatch` describes one execution attempt:

- `batch_id` is a new UUID for every attempt;
- `source_fingerprint` identifies the input content;
- `started_at` supplies a consistent UTC timestamp;
- row and table counts describe the input volume.

Two retries of identical files have different batch IDs but the same source
fingerprint. That distinction answers two different questions: “Which run did
this?” and “Was its input the same?”

`compute_record_hashes()` creates a SHA-256 identity from each row's source
values. Metadata columns are excluded so a retry produces the same record hash.
`compute_batch_fingerprint()` combines sorted table names and sorted record
hashes, making the result independent of dataframe and table iteration order.

### `postgres_audit.py`: the durable run ledger

The `ingestion_runs` table records `running`, `succeeded`, or `failed` for each
batch. `start_ingestion_run()` commits `running` before raw publication begins.
`finish_ingestion_run()` later records completion and, for failures, only the
exception type. It intentionally avoids storing exception messages that could
contain credentials or sensitive source values.

### `postgres.py`: physical database loading

`load_tables_to_postgres()` owns database mechanics, not source-file reading or
business validation. It:

1. checks that dataframes and registry entries match;
2. acquires a transaction-scoped advisory lock, preventing overlapping writers
   from publishing to the same raw schema;
3. creates target tables and required unique indexes;
4. adds `_batch_id`, `_ingested_at`, and `_record_hash` to copied data;
5. uses PostgreSQL `COPY` to load uniquely named staging tables efficiently;
6. publishes each staging table according to its configured strategy;
7. drops staging tables and commits everything together.

The context-managed Psycopg connection commits when the block succeeds and
rolls back when an exception escapes. Staging tables are created in that same
transaction, so a rollback removes them too.

### `load_postgres_cli.py`: application workflow

The CLI coordinates the components. It reads configuration and files, performs
Pandera and cross-table validation, creates the batch, manages the audit
lifecycle, and invokes the database loader. It does not contain SQL publication
rules; those remain testable inside the loader.

## The three publication strategies

### UPSERT

Customers, orders, order items, payments, products, and sellers have reliable
business keys. New keys are inserted. Existing keys are updated only when the
record hash changed. Records absent from a delta batch remain in the target.

This supports late corrections without interpreting absence as deletion.

### Append and deduplicate

Reviews lack a sufficiently reliable source key, so the complete record hash
acts as technical identity. New hashes are appended and existing hashes are
ignored. An exact retry therefore creates no duplicate review.

This policy does not claim that two distinct review versions represent an
update; both can remain as source observations, and dbt selects the analytical
review version deterministically downstream.

### Snapshot replacement

Geolocation and category translation are reference snapshots. Their staging
contents replace the target contents with transactional `TRUNCATE + INSERT`.
The table itself is not dropped, so its database identity and downstream
dependencies remain stable.

## Why staging is necessary

Publishing one table immediately after reading it could leave a mixed state:
new customers but old orders, for example. Here, all `COPY` operations finish
before publication begins, and every publication occurs in one transaction.

If table nine fails, changes planned for tables one through eight are rolled
back. The previous coherent raw dataset remains visible.

## What idempotency means here

Safe re-execution depends on strategy:

- unchanged UPSERT rows are no-ops because their record hashes match;
- append-managed rows conflict on `_record_hash` and are ignored;
- snapshot tables are replaced with the same content;
- every attempt still receives its own audit row.

Idempotency means repeated input does not corrupt or duplicate business data.
It does not mean executions become invisible: the audit ledger deliberately
records every attempt.

## Current deletion boundary

An incremental file that omits an order does not prove that the order was
deleted. The loader therefore retains absent UPSERT records. A future source
would need explicit tombstones or an intentionally declared complete-snapshot
reconciliation process before deletions could be applied safely.

## Local schema compatibility

The metadata columns changed the physical raw-table shape. A local raw schema
created by the older snapshot-only loader should be recreated once before using
this implementation. This is a development migration, not a command the loader
should silently perform against an unknown environment.

After that one-time reset, subsequent loads preserve target tables and are safe
to rerun. Integration tests avoid this issue by creating isolated schemas for
every test and dropping them afterward.

## Incremental dbt facts

The PostgreSQL loader incrementally maintains the raw layer, while dbt
incrementally publishes the two fact tables.

Each fact row contains `warehouse_updated_at`, calculated from the ingestion
timestamps of every source record that can affect that row.

During an incremental run, dbt compares each candidate row with the existing
fact row having the same natural key:

- missing keys are inserted;
- keys with newer source lineage are updated;
- unchanged or older candidates are ignored.

`fct_orders` merges on `order_id`. `fct_order_items` merges on the composite
key `(order_id, order_item_id)`.

This supports late changes to historical orders because selection is based on
source ingestion time rather than order purchase date.

The current intermediate models are views, so PostgreSQL still evaluates their
source queries during an incremental run. The optimization primarily reduces
fact-table writes rather than guaranteeing that the warehouse scans only new
raw records.