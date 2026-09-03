#!/usr/bin/env bash

set -euo pipefail

compose=(docker compose)

echo "Validating Compose configuration..."
"${compose[@]}" config --quiet

echo "Building pipeline image..."
"${compose[@]}" build pipeline

echo "Checking installed Python package..."
"${compose[@]}" run --rm --no-deps pipeline \
    python -c "import olist_dw; print(olist_dw.__file__)"

echo "Checking dbt installation..."
"${compose[@]}" run --rm --no-deps pipeline \
    dbt --version

echo "Starting PostgreSQL..."
"${compose[@]}" up -d postgres

echo "Checking PostgreSQL connectivity..."
"${compose[@]}" run --rm pipeline \
    olist-check-postgres

echo "Container smoke tests passed."