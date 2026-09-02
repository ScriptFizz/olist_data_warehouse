from datetime import UTC, datetime
from enum import StrEnum

import psycopg
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.ingestion_batch import IngestionBatch


class IngestionStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def ensure_ingestion_audit_table(
    settings: PostgresSettings,
) -> None:
    """Create the ingestion audit schema and table if absent."""
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(settings.audit_schema)
            )
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.ingestion_runs (
                    batch_id UUID PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('running', 'succeeded', 'failed')
                    ),
                    started_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ,
                    table_count INTEGER NOT NULL,
                    input_row_count BIGINT NOT NULL,
                    error_type TEXT
                )
                """
            ).format(sql.Identifier(settings.audit_schema))
        )


def start_ingestion_run(
    *,
    settings: PostgresSettings,
    batch: IngestionBatch,
) -> None:
    """Record an ingestion attempt before publication begins."""
    ensure_ingestion_audit_table(settings)

    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {}.ingestion_runs (
                    batch_id,
                    source_name,
                    source_fingerprint,
                    status,
                    started_at,
                    table_count,
                    input_row_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
            ).format(sql.Identifier(settings.audit_schema)),
            (
                batch.batch_id,
                batch.source_name,
                batch.source_fingerprint,
                IngestionStatus.RUNNING.value,
                batch.started_at,
                batch.table_count,
                batch.input_row_count,
            ),
        )


def finish_ingestion_run(
    *,
    settings: PostgresSettings,
    batch: IngestionBatch,
    status: IngestionStatus,
    error_type: str | None = None,
) -> None:
    """Complete an existing audit record."""
    if status is IngestionStatus.RUNNING:
        raise ValueError("Cannot finish a run with running status")

    if status is IngestionStatus.SUCCEEDED and error_type is not None:
        raise ValueError("Successful runs cannot contain an error type")

    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            sql.SQL(
                """
                UPDATE {}.ingestion_runs
                SET
                    status = %s,
                    completed_at = %s,
                    error_type = %s
                WHERE batch_id = %s
                """
            ).format(sql.Identifier(settings.audit_schema)),
            (
                status.value,
                datetime.now(UTC),
                error_type,
                batch.batch_id,
            ),
        )

        if cursor.rowcount != 1:
            raise RuntimeError(
                f"Ingestion audit record not found for batch "
                f"{batch.batch_id}"
            )