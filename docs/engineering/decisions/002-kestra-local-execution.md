# ADR 002: Execute local pipeline tasks through the Docker daemon

## Status

Accepted for local development.

## Context

The project already provides a reproducible pipeline image containing the
Python ETL package and dbt adapters. Kestra needs to orchestrate these
capabilities without duplicating transformation or loading logic in flow YAML.

The project is a trusted, single-user portfolio environment and is not intended
to expose Kestra publicly.

## Decision

The local Kestra service will access the host Docker daemon through
`/var/run/docker.sock` and launch the existing
`olist-data-warehouse-pipeline:local` image.

Each orchestration task will invoke an existing project CLI or dbt command.
Kestra flow definitions will contain task order, parameters, retries, timeouts,
and failure handling, but not business transformation logic.

The Kestra UI is bound to `127.0.0.1`.

Kestra Open Source requires Basic Authentication. Local credentials are read
from the untracked `.env` file and used by both the server and flow deployment
command; they are never stored in flow definitions.

Repository-managed flows are deployed explicitly after the Kestra server has
started. Startup flow preloading is not used because the pinned Kestra release
parses preloaded flows before external task plugins are registered, causing
flows containing Docker tasks to be rejected even though the Docker plugin is
bundled in the image.

## Consequences

### Positive

- Manual and orchestrated execution use the same pipeline image.
- Python and dbt dependency versions remain reproducible.
- Each task runs in a disposable container.
- Kestra records task status, duration, attempts, and logs.
- No additional cloud execution infrastructure is required.
- Flow deployment is explicit and repeatable after all plugins are available.

### Negative

- Docker socket access grants the Kestra container effective control over the
  Docker host.
- This design is unsuitable for untrusted flows, multiple untrusted users, or
  a publicly accessible deployment.
- Host volume and Docker network names must be passed explicitly to task
  containers.
- Concurrent executions require controls around shared data directories.
- Local flow changes must be deployed using the repository deployment script.

## Safeguards

- Bind the Kestra UI only to the loopback interface.
- Keep flow definitions under version control.
- Do not accept untrusted flow definitions.
- Pin container image versions.
- Do not run pipeline containers as privileged.
- Mount only the paths required by each task.
- Prevent overlapping executions of the main pipeline.
- Never place credentials directly in flow YAML.

## Future evolution

For a shared or production environment, separate the Kestra control plane from
task execution. A dedicated worker or external container platform should run
pipeline jobs using restricted identities without exposing the control plane to
the host Docker socket.
