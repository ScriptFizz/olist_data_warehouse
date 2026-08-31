# import os
# from collections.abc import Iterator
# from dataclasses import replace
from typing import Any, cast

# from uuid import uuid4
import pandas as pd
import pandera as pa
import psycopg
import pytest
from pandera.typing import Series
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.postgres import load_tables_to_postgres


class AccountsSchema(pa.SchemaModel):
    account_id: Series[int]
    account_name: Series[str]

    class Config:
        strict = True


class EventsSchema(pa.SchemaModel):
    event_id: Series[int]
    amount: Series[float]

    class Config:
        strict = True


pytestmark = pytest.mark.postgres


# @pytest.fixture
# def postgres_settings() -> Iterator[PostgresSettings]:
#     if os.getenv("RUN_POSTGRES_TESTS") != "1":
#         pytest.skip("Set RUN_POSTGRES_TESTS=1 to run PostgreSQL integration tests")

#     base_settings = PostgresSettings.from_env()
#     test_schema = f"test_raw_{uuid4().hex[:12]}"
#     settings = replace(base_settings, schema=test_schema)

#     try:
#         yield settings
#     finally:
#         with (
#             psycopg.connect(**settings.connection_kwargs()) as connection,
#             connection.cursor() as cursor,
#         ):
#             cursor.execute(
#                 sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
#                     sql.Identifier(test_schema)
#                 )
#             )


def schemas() -> dict[str, type[pa.SchemaModel]]:
    return {
        "accounts": AccountsSchema,
        "events": EventsSchema,
    }


def baseline_tables() -> dict[str, pd.DataFrame]:
    return {
        "accounts": pd.DataFrame(
            {
                "account_id": [1, 2],
                "account_name": ["first", "second"],
            }
        ),
        "events": pd.DataFrame(
            {
                "event_id": [10, 20],
                "amount": [100.5, 200.5],
            }
        ),
    }


def fetch_rows(
    settings: PostgresSettings,
    table_name: str,
) -> list[tuple[Any, ...]]:
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            sql.SQL("SELECT * FROM {}.{} ORDER BY 1").format(
                sql.Identifier(settings.schema),
                sql.Identifier(table_name),
            )
        )
        return cursor.fetchall()


def table_oid(
    settings: PostgresSettings,
    table_name: str,
) -> int:
    qualified_name = f"{settings.schema}.{table_name}"

    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT to_regclass(%s)::oid",
            (qualified_name,),
        )
        row = cursor.fetchone()

    if row is None or row[0] is None:
        raise AssertionError(f"Table does not exist: {qualified_name}")

    return cast(int, row[0])


def staging_tables(settings: PostgresSettings) -> list[str]:
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND starts_with(table_name, %s)
            ORDER BY table_name
            """,
            (settings.schema, "__load_"),
        )
        return [str(row[0]) for row in cursor.fetchall()]


def test_loader_publishes_all_tables(
    postgres_settings: PostgresSettings,
) -> None:
    result = load_tables_to_postgres(
        tables=baseline_tables(),
        schemas=schemas(),
        settings=postgres_settings,
    )

    assert result.schema == postgres_settings.schema
    assert result.row_counts == {
        "accounts": 2,
        "events": 2,
    }

    assert fetch_rows(postgres_settings, "accounts") == [
        (1, "first"),
        (2, "second"),
    ]
    assert fetch_rows(postgres_settings, "events") == [
        (10, 100.5),
        (20, 200.5),
    ]
    assert staging_tables(postgres_settings) == []


def test_loader_replaces_rows_without_recreating_target_table(
    postgres_settings: PostgresSettings,
) -> None:
    load_tables_to_postgres(
        tables=baseline_tables(),
        schemas=schemas(),
        settings=postgres_settings,
    )
    original_oid = table_oid(postgres_settings, "accounts")

    replacement = {
        "accounts": pd.DataFrame(
            {
                "account_id": [3],
                "account_name": ["replacement"],
            }
        ),
        "events": pd.DataFrame(
            {
                "event_id": [30],
                "amount": [300.5],
            }
        ),
    }

    load_tables_to_postgres(
        tables=replacement,
        schemas=schemas(),
        settings=postgres_settings,
    )

    assert fetch_rows(postgres_settings, "accounts") == [
        (3, "replacement"),
    ]
    assert fetch_rows(postgres_settings, "events") == [
        (30, 300.5),
    ]
    assert table_oid(postgres_settings, "accounts") == original_oid
    assert staging_tables(postgres_settings) == []


def test_loader_rolls_back_entire_batch_when_copy_fails(
    postgres_settings: PostgresSettings,
) -> None:
    original = baseline_tables()

    load_tables_to_postgres(
        tables=original,
        schemas=schemas(),
        settings=postgres_settings,
    )

    original_accounts = fetch_rows(postgres_settings, "accounts")
    original_events = fetch_rows(postgres_settings, "events")

    failing_batch = {
        "accounts": pd.DataFrame(
            {
                "account_id": [99],
                "account_name": ["must-not-be-published"],
            }
        ),
        "events": pd.DataFrame(
            {
                "event_id": [99],
                # amount is NOT NULL according to EventsSchema.
                "amount": [None],
            }
        ),
    }

    with pytest.raises(psycopg.errors.NotNullViolation):
        load_tables_to_postgres(
            tables=failing_batch,
            schemas=schemas(),
            settings=postgres_settings,
        )

    assert fetch_rows(postgres_settings, "accounts") == original_accounts
    assert fetch_rows(postgres_settings, "events") == original_events
    assert staging_tables(postgres_settings) == []
