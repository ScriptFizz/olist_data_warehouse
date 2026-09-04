#!/usr/bin/env bash

set -euo pipefail

compose=(docker compose)

echo "Starting the Kestra control plane..."
"${compose[@]}" up -d kestra-postgres kestra

echo "Deploying repository-managed Kestra flows..."
for attempt in {1..15}; do
    output="$("${compose[@]}" run --rm --no-deps \
        --entrypoint /bin/sh \
        kestra \
        -c 'exec java -jar /app/kestra flow updates --server http://kestra:8080 --user="${OLIST_KESTRA_USERNAME}:${OLIST_KESTRA_PASSWORD}" /app/flows' \
        2>&1)" && status=0 || status=$?

    if (( status == 0 )); then
        printf '%s\n' "${output}"
        echo "Kestra flows deployed successfully."
        exit 0
    fi

    if [[ "${output}" != *"Connection refused"* ]]; then
        printf '%s\n' "${output}" >&2
        echo "Kestra flow deployment failed." >&2
        exit "${status}"
    fi

    if (( attempt == 15 )); then
        printf '%s\n' "${output}" >&2
        echo "Kestra flow deployment failed after ${attempt} attempts." >&2
        exit "${status}"
    fi

    echo "Kestra API is not ready; retrying in 2 seconds (${attempt}/15)..."
    sleep 2
done
