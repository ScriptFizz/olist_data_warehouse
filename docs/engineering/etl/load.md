# Warehouse loading

## PostgreSQL

PostgreSQL is the fully tested local loading path:

```bash
uv run olist-load-postgres --processed-dir data/processed
```

The command revalidates every processed file, checks cross-table integrity,
creates batch metadata, starts the audit lifecycle, and invokes the
transactional bulk loader. See [PostgreSQL ingestion](postgres_ingestion.md)
for its publication strategies and rollback behavior.

## BigQuery

An optional raw-layer loader remains available:

```bash
uv run olist-load-bigquery \
  --processed-dir data/processed \
  --project-id YOUR_PROJECT \
  --dataset-id YOUR_RAW_DATASET
```

The BigQuery path validates the complete dataset, attaches the same batch and
record lineage columns expected by dbt, converts Pandera schemas to BigQuery
fields, and loads each raw table with `WRITE_TRUNCATE`. It is therefore a
full-snapshot bootstrap path, not equivalent to PostgreSQL's incremental
publication and durable audit ledger.

It requires Google application credentials and is not exercised by normal CI.
Warehouse transformations should then run through dbt's `bigquery_dev` target;
the retired hand-ordered SQL layer runner must not be reintroduced.
