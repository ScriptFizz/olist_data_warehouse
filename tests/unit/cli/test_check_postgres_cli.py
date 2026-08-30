import psycopg
import pytest
from typer.testing import CliRunner

import olist_dw.cli.check_postgres_cli as postgres_cli
from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.postgres import PostgresConnectionInfo

runner = CliRunner()


def test_connection_failure_does_not_expose_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "never-display-this-password"

    settings = PostgresSettings(
        host="localhost",
        port=5999,
        database="olist",
        user="olist",
        schema="raw",
        password=secret,
    )

    monkeypatch.setattr(
        postgres_cli.PostgresSettings,
        "from_env",
        lambda: settings,
    )

    def fail_connection(
        _settings: PostgresSettings,
    ) -> PostgresConnectionInfo:
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(
        postgres_cli,
        "check_postgres_connection",
        fail_connection,
    )

    result = runner.invoke(postgres_cli.app)

    assert result.exit_code == 1
    assert secret not in result.output
    assert "Traceback" not in result.output
    assert "PostgreSQL connection failed" in result.output
