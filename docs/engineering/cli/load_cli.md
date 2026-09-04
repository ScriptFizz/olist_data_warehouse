# Database commands

Check PostgreSQL connectivity without displaying the password:

```bash
olist-check-postgres
```

Load a validated batch into PostgreSQL:

```bash
olist-load-postgres --processed-dir data/processed
```

Load processed source tables into an optional BigQuery raw dataset:

```bash
olist-load-bigquery \
  --processed-dir data/processed \
  --project-id YOUR_PROJECT \
  --dataset-id YOUR_RAW_DATASET
```

Analytical models are built with `dbt build`; there is no separate layer-loading
CLI.
