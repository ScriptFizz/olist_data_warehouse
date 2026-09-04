# Extract command

```text
olist-extract [--out-dir PATH] [--dataset-name OWNER/DATASET]
```

If options are omitted, values come from `settings.yaml`. Kaggle credentials
come from environment variables or `.env`.

The command downloads and unpacks the source files. It does not transform or
publish them.
