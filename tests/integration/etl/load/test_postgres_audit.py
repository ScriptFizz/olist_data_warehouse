from typing import Any

import psycopg
import pytest
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.ingestion_batch import IngestionBatch
from olist_dw.etl.load.postgres_audit import (
    IngestionStatus,
    finish_ingestion_run,
    start_ingestion_run,
)

pytestmark = pytest.mark.postgres


def fetch_audit_row(
    settings: PostgresSettings,
    batch: IngestionBatch,
) -> tuple[Any, ...]:
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            sql.SQL(
                """
                SELECT
                    source_name,
                    source_fingerprint,
                    status,
                    table_count,
                    input_row_count,
                    error_type,
                    completed_at
                FROM {}.ingestion_runs
                WHERE batch_id = %s
                """
            ).format(sql.Identifier(settings.audit_schema)),
            (batch.batch_id,),
        )
        row = cursor.fetchone()

    if row is None:
        raise AssertionError("Audit row was not created")

    return row


def test_successful_ingestion_audit_lifecycle(
    postgres_settings: PostgresSettings,
) -> None:
    import pandas as pd

    batch = IngestionBatch.create(
        source_name="integration-test",
        tables={"example": pd.DataFrame({"id": [1, 2]})},
    )

    start_ingestion_run(
        settings=postgres_settings,
        batch=batch,
    )

    running = fetch_audit_row(postgres_settings, batch)
    assert running[2] == "running"
    assert running[6] is None

    finish_ingestion_run(
        settings=postgres_settings,
        batch=batch,
        status=IngestionStatus.SUCCEEDED,
    )

    completed = fetch_audit_row(postgres_settings, batch)
    assert completed[0] == "integration-test"
    assert completed[1] == batch.source_fingerprint
    assert completed[2] == "succeeded"
    assert completed[3] == 1
    assert completed[4] == 2
    assert completed[5] is None
    assert completed[6] is not None


def test_failed_ingestion_records_error_type(
    postgres_settings: PostgresSettings,
) -> None:
    import pandas as pd

    batch = IngestionBatch.create(
        source_name="integration-test",
        tables={"example": pd.DataFrame({"id": [1]})},
    )

    start_ingestion_run(
        settings=postgres_settings,
        batch=batch,
    )
    finish_ingestion_run(
        settings=postgres_settings,
        batch=batch,
        status=IngestionStatus.FAILED,
        error_type="NotNullViolation",
    )

    completed = fetch_audit_row(postgres_settings, batch)

    assert completed[2] == "failed"
    assert completed[5] == "NotNullViolation"
    assert completed[6] is not None