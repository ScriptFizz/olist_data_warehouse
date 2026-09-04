# Architecture

## Data flow

```text
Kaggle CSV files
    -> Python extraction
    -> Pandera raw contracts
    -> Python normalization
    -> Pandera processed contracts and cross-table validation
    -> validated processed CSV files
    -> PostgreSQL raw publication (local) or BigQuery raw load (optional)
    -> dbt staging views
    -> dbt intermediate views
    -> dimensional marts
    -> notebooks, dashboards, and analytical consumers
```

Kestra invokes these existing Python and dbt commands in order. It does not
reimplement any validation, publication, or transformation logic.

## Responsibility boundaries

| Component | Owns | Does not own |
|---|---|---|
| Python | Files, Pandera contracts, cross-table validation, batch metadata, raw publication | Analytical joins and business marts |
| PostgreSQL/BigQuery raw layer | Durable source-aligned rows and ingestion metadata | BI metrics |
| dbt | SQL dependencies, staging, reusable joins, dimensional marts, warehouse tests | Source downloads or file parsing |
| Kestra | Ordering, parameters, retries, timeouts, execution state and logs | Business transformation logic |
| Docker Compose | Reproducible local process and network boundaries | A simulated cloud warehouse |

## Storage layers

The filesystem contains raw downloaded CSVs and deterministic processed CSVs.
PostgreSQL then separates operational ingestion state from analytical state:

- `raw`: source-aligned tables with `_batch_id`, `_ingested_at`, and
  `_record_hash`;
- `pipeline_metadata`: the durable `ingestion_runs` audit ledger;
- dbt-generated staging, intermediate, reference, and mart schemas, prefixed by
  the configured target schema.

The BigQuery path is optional. BigQuery itself is not containerized; the same
dbt project can target it through environment-provided credentials.

## Reproducibility and cost

Python dependencies are locked by uv, the application image is built from that
lock, and PostgreSQL/Kestra versions are pinned in Compose. Local services run
only when requested. Normal CI uses an ephemeral PostgreSQL service and never
requires paid cloud infrastructure.
