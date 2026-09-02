import os
from collections.abc import Iterator
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings


@pytest.fixture
def postgres_settings() -> Iterator[PostgresSettings]:
    if os.getenv("RUN_POSTGRES_TESTS") != "1":
        pytest.skip("Set RUN_POSTGRES_TESTS=1 to run PostgreSQL integration tests")

    base_settings = PostgresSettings.from_env()
    # test_schema = f"test_raw_{uuid4().hex[:12]}"
    # settings = replace(base_settings, schema=test_schema)
    test_suffix = uuid4().hex[:12]
    settings = replace(
        base_settings,
        schema=f"test_raw_{test_suffix}",
        audit_schema=f"test_audit_{test_suffix}",
    )

    try:
        yield settings
    finally:
        with (
            psycopg.connect(**settings.connection_kwargs()) as connection,
            connection.cursor() as cursor,
        ):
            for schema_name in (
                settings.schema,
                settings.audit_schema,
            ):
                cursor.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema_name)
                    )
                )
