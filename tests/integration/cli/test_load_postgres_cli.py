from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from typer.testing import CliRunner

import olist_dw.cli.load_postgres_cli as postgres_cli
from olist_dw.config.postgres import PostgresSettings

runner = CliRunner()

pytestmark = pytest.mark.postgres

FIXTURE_DIRECTORY = Path(__file__).resolve().parents[2] / "fixtures" / "processed_olist"

EXPECTED_TABLES = {
    "customers",
    "orders",
    "order_items",
    "payments",
    "products",
    "sellers",
    "geolocation",
    "translation",
    "reviews",
}


def table_row_counts(
    settings: PostgresSettings,
) -> dict[str, int]:
    counts: dict[str, int] = {}

    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        for table_name in EXPECTED_TABLES:
            cursor.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(settings.schema),
                    sql.Identifier(table_name),
                )
            )
            row = cursor.fetchone()

            if row is None:
                raise AssertionError(f"No count returned for {table_name}")

            counts[table_name] = int(row[0])

    return counts


def test_load_postgres_cli_loads_complete_fixture_idempotently(
    monkeypatch: pytest.MonkeyPatch,
    postgres_settings: PostgresSettings,
) -> None:
    monkeypatch.setattr(
        postgres_cli.PostgresSettings,
        "from_env",
        lambda: postgres_settings,
    )

    first_result = runner.invoke(
        postgres_cli.app,
        ["--processed-dir", str(FIXTURE_DIRECTORY)],
    )

    assert first_result.exit_code == 0, first_result.output
    assert "tables=9" in first_result.output
    assert "rows=9" in first_result.output
    assert table_row_counts(postgres_settings) == {
        table_name: 1 for table_name in EXPECTED_TABLES
    }

    second_result = runner.invoke(
        postgres_cli.app,
        ["--processed-dir", str(FIXTURE_DIRECTORY)],
    )

    assert second_result.exit_code == 0, second_result.output
    assert table_row_counts(postgres_settings) == {
        table_name: 1 for table_name in EXPECTED_TABLES
    }
