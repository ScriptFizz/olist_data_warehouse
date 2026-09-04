# Python ingestion pipeline

The Python package owns the boundary between external files and source-aligned
warehouse tables. It deliberately stops before analytical modeling, which is
owned by dbt.

## Stages

1. `olist-extract` downloads and unpacks the Olist files from Kaggle.
2. `olist-transform` validates every raw dataframe, normalizes fields, validates
   every processed dataframe, then checks relationships across the dataset.
3. `olist-load-postgres` revalidates processed files, creates ingestion
   metadata, and publishes all tables atomically.
4. `olist-load-bigquery` is retained as an optional raw-layer cloud loader.

The central table registry connects each dataset to its raw and processed
filenames, Pandera contracts, transform function, business key, and PostgreSQL
publication strategy. This prevents separate stages from maintaining divergent
table lists.

See [PostgreSQL ingestion](postgres_ingestion.md) for transaction and retry
semantics.
