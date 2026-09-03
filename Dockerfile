# syntax=docker/dockerfile:1.7

FROM python:3.11.15-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.12.9 \
    /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# Install locked third-party dependencies in a separately cached layer
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --locked \
        --no-default-groups \
        --group dbt \
        --no-install-project

# Copy only files required by the runtime.
COPY README.md settings.yaml ./
COPY src ./src
COPY dbt ./dbt

# Install the project itself as a non-editable package.

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --locked \
        --no-default-groups \
        --group dbt \
        --no-editable

RUN mkdir -p \
    /app/data/raw \
    /app/data/processed \
    /app/logs

CMD ["olist-check-postgres"]