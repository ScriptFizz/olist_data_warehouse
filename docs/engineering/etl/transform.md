# Transformation and validation

`olist-transform` processes all nine registered datasets as one coherent input.

For every table it:

1. reads the expected raw CSV;
2. validates the raw dataframe with its Pandera contract;
3. applies the registered normalization function;
4. validates the processed dataframe with its second contract.

Only after all tables pass does it validate cross-table relationships for
orders, items, payments, reviews, customers, products, and sellers. Processed
CSV files are written after dataset validation succeeds, preventing an invalid
relationship from being silently sent to the loader.

```bash
uv run olist-transform \
  --raw-data-dir data/raw \
  --processed-data-dir data/processed
```

This stage performs programmatic cleanup and file-level validation. Analytical
joins and business measures remain in dbt.
