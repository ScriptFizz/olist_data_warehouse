# Kestra orchestration

Kestra orchestrates the existing containerized pipeline; it does not contain
data-transformation or loading logic. Every runnable stage starts the same
`olist-data-warehouse-pipeline:local` image used for manual execution.

## Flow

The `olist.pipeline.olist_postgres_pipeline` flow runs these stages in order:

1. verify PostgreSQL connectivity;
2. optionally download the static Kaggle source;
3. transform and validate all source tables;
4. publish the raw PostgreSQL batch transactionally;
5. build and test the dbt warehouse.

Extraction defaults to disabled so routine reruns and recovery do not depend
on Kaggle. Enable `run_extraction` only when intentionally refreshing the raw
files.

## Execution safety

The flow permits one active execution. Additional manual or scheduled runs are
queued because executions share the repository's `data` and `dbt` bind mounts.

Only transient boundary checks and the external Kaggle download are retried.
Transformation, publication, and dbt errors are treated as deterministic and
fail immediately with their original diagnostics.

PostgreSQL publication remains atomic and records its own ingestion audit
lifecycle. A failure rolls back the entire publication batch. Restarting the
failed Kestra task is safe because loading strategies and record hashes make
publication idempotent, while dbt facts merge on their declared unique keys.

## Local configuration

`OLIST_PROJECT_DIR` must contain the absolute host path to the repository. The
Docker daemon, rather than the Kestra container, resolves bind-mount source
paths.

Kestra's Docker runner assigns each task a temporary working directory. Task
containers therefore receive `OLIST_PROJECT_ROOT=/app`, allowing the Python
configuration loader to resolve the mounted `/app/settings.yaml` independently
of the current working directory. Flow commands likewise use absolute
`/app/data` and `/app/dbt` paths so input and output resolution does not depend
on Kestra's temporary working directory.

Passwords and API keys are supplied through the untracked `.env` file. Kestra
receives Base64-encoded `SECRET_*` environment values and injects decoded
secrets into disposable task containers. Secret values must never be committed
to flow YAML. `KESTRA_KAGGLE_KEY_BASE64` is optional for routine executions but
must be configured before running the flow with `run_extraction` enabled.

## Deployment

After changing a flow, deploy all repository-managed definitions:

```bash
./scripts/deploy_kestra_flows.sh
```

The Kestra UI is available locally at <http://localhost:8080>.

## Local security boundary

Kestra accesses `/var/run/docker.sock`, which grants effective control over the
local Docker host. This configuration is restricted to a trusted, loopback-only
development environment. A shared deployment should move execution to a
dedicated worker or external container platform.
