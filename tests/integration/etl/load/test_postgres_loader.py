from collections.abc import Mapping
from typing import Any, cast

import pandas as pd
import pandera as pa
import psycopg
import pytest
from pandera.typing import Series
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.ingestion_batch import IngestionBatch
from olist_dw.etl.load.postgres import load_tables_to_postgres
from olist_dw.etl.registry.tables import LoadStrategy, TableConfig


class AccountsSchema(pa.SchemaModel):
    account_id: Series[int] = pa.Field(unique=True)
    account_name: Series[str]

    class Config:
        strict = True


class EventsSchema(pa.SchemaModel):
    event_id: Series[int] = pa.Field(unique=True)
    amount: Series[float]

    class Config:
        strict = True


class MessagesSchema(pa.SchemaModel):
    message_id: Series[int]
    body: Series[str]

    class Config:
        strict = True


pytestmark = pytest.mark.postgres


def identity_transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    return dataframe


def table_configs() -> dict[str, TableConfig]:
    return {
        "accounts": TableConfig(
            name="accounts",
            raw_filename="accounts.csv",
            processed_filename="accounts.csv",
            raw_schema=AccountsSchema,
            processed_schema=AccountsSchema,
            transform=identity_transform,
            load_strategy=LoadStrategy.UPSERT,
            business_key=("account_id",),
        ),
        "events": TableConfig(
            name="events",
            raw_filename="events.csv",
            processed_filename="events.csv",
            raw_schema=EventsSchema,
            processed_schema=EventsSchema,
            transform=identity_transform,
            load_strategy=LoadStrategy.SNAPSHOT_REPLACE,
        ),
        "messages": TableConfig(
            name="messages",
            raw_filename="messages.csv",
            processed_filename="messages.csv",
            raw_schema=MessagesSchema,
            processed_schema=MessagesSchema,
            transform=identity_transform,
            load_strategy=LoadStrategy.APPEND_DEDUPLICATE,
        ),
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
            {"event_id": [10, 20], "amount": [100.5, 200.5]}
        ),
        "messages": pd.DataFrame(
            {"message_id": [100, 200], "body": ["hello", "world"]}
        ),
    }


def load_batch(
    settings: PostgresSettings,
    tables: Mapping[str, pd.DataFrame],
) -> IngestionBatch:
    batch = IngestionBatch.create(
        source_name="integration-test",
        tables=tables,
    )
    load_tables_to_postgres(
        tables=tables,
        table_configs=table_configs(),
        settings=settings,
        batch=batch,
    )
    return batch


def fetch_rows(
    settings: PostgresSettings,
    table_name: str,
    columns: tuple[str, ...],
) -> list[tuple[Any, ...]]:
    selected_columns = sql.SQL(", ").join(
        sql.Identifier(column) for column in columns
    )

    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            sql.SQL("SELECT {} FROM {}.{} ORDER BY 1").format(
                selected_columns,
                sql.Identifier(settings.schema),
                sql.Identifier(table_name),
            )
        )
        return cursor.fetchall()


def table_oid(settings: PostgresSettings, table_name: str) -> int:
    qualified_name = f"{settings.schema}.{table_name}"

    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT to_regclass(%s)::oid", (qualified_name,))
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


def test_loader_applies_all_three_publication_strategies(
    postgres_settings: PostgresSettings,
) -> None:
    first_batch = load_batch(postgres_settings, baseline_tables())
    original_event_oid = table_oid(postgres_settings, "events")

    second_tables = {
        "accounts": pd.DataFrame(
            {
                "account_id": [2, 3],
                "account_name": ["second-updated", "third"],
            }
        ),
        "events": pd.DataFrame({"event_id": [30], "amount": [300.5]}),
        "messages": pd.DataFrame(
            {"message_id": [200, 300], "body": ["world", "new"]}
        ),
    }
    second_batch = load_batch(postgres_settings, second_tables)

    assert fetch_rows(
        postgres_settings,
        "accounts",
        ("account_id", "account_name"),
    ) == [(1, "first"), (2, "second-updated"), (3, "third")]
    assert fetch_rows(
        postgres_settings,
        "events",
        ("event_id", "amount"),
    ) == [(30, 300.5)]
    assert table_oid(postgres_settings, "events") == original_event_oid
    assert fetch_rows(
        postgres_settings,
        "messages",
        ("message_id", "body"),
    ) == [(100, "hello"), (200, "world"), (300, "new")]

    account_batches = fetch_rows(
        postgres_settings,
        "accounts",
        ("account_id", "_batch_id"),
    )
    assert str(account_batches[0][1]) == str(first_batch.batch_id)
    assert str(account_batches[1][1]) == str(second_batch.batch_id)

    message_batches = fetch_rows(
        postgres_settings,
        "messages",
        ("message_id", "_batch_id"),
    )
    assert str(message_batches[1][1]) == str(first_batch.batch_id)
    assert staging_tables(postgres_settings) == []


def test_identical_rerun_is_safe(
    postgres_settings: PostgresSettings,
) -> None:
    tables = baseline_tables()
    first_batch = load_batch(postgres_settings, tables)
    second_batch = IngestionBatch.create(
        source_name="integration-test",
        tables=tables,
    )
    result = load_tables_to_postgres(
        tables=tables,
        table_configs=table_configs(),
        settings=postgres_settings,
        batch=second_batch,
    )

    assert first_batch.source_fingerprint == second_batch.source_fingerprint
    assert result.input_row_counts == {
        "accounts": 2,
        "events": 2,
        "messages": 2,
    }
    assert result.affected_row_counts["messages"] == 0
    assert result.affected_row_counts["accounts"] == 0
    assert len(
        fetch_rows(
            postgres_settings,
            "messages",
            ("message_id", "body"),
        )
    ) == 2


def test_loader_rolls_back_entire_batch_when_copy_fails(
    postgres_settings: PostgresSettings,
) -> None:
    load_batch(postgres_settings, baseline_tables())
    original_accounts = fetch_rows(
        postgres_settings,
        "accounts",
        ("account_id", "account_name"),
    )
    original_events = fetch_rows(
        postgres_settings,
        "events",
        ("event_id", "amount"),
    )

    failing_tables = {
        "accounts": pd.DataFrame(
            {"account_id": [2], "account_name": ["must-not-publish"]}
        ),
        "events": pd.DataFrame({"event_id": [99], "amount": [None]}),
        "messages": pd.DataFrame(
            {"message_id": [999], "body": ["must-not-publish"]}
        ),
    }
    batch = IngestionBatch.create(
        source_name="integration-test",
        tables=failing_tables,
    )

    with pytest.raises(psycopg.errors.NotNullViolation):
        load_tables_to_postgres(
            tables=failing_tables,
            table_configs=table_configs(),
            settings=postgres_settings,
            batch=batch,
        )

    assert fetch_rows(
        postgres_settings,
        "accounts",
        ("account_id", "account_name"),
    ) == original_accounts
    assert fetch_rows(
        postgres_settings,
        "events",
        ("event_id", "amount"),
    ) == original_events
    assert staging_tables(postgres_settings) == []
