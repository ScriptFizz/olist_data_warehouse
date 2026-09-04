# Extraction

`olist-extract` invokes the Kaggle CLI for the configured
`olistbr/brazilian-ecommerce` dataset and unpacks it into the raw directory.
It requires `KAGGLE_USERNAME` and `KAGGLE_KEY` at runtime.

```bash
uv run olist-extract --out-dir data/raw
```

Extraction is intentionally optional in Kestra. The source is static, and a
routine retry should not depend on Kaggle or replace already available files.
Enable extraction only for an intentional source refresh.

Credentials belong in the untracked `.env` file or the runtime secret provider;
they are never baked into the application image or flow YAML.
