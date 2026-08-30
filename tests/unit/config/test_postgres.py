import pytest

from olist_dw.config.postgres import (
    PostgresConfigurationError,
    PostgresSettings,
)

POSTGRES_ENVIRONMENT = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5433",
    "POSTGRES_DB": "olist",
    "POSTGRES_USER": "olist",
    "POSTGRES_PASSWORD": "local-secret",
    "POSTGRES_SCHEMA": "raw",
}


def set_postgres_environment(
    monkeypatch: pytest.MonkeyPatch,
    **overrides: str,
) -> None:
    values = POSTGRES_ENVIRONMENT | overrides

    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_postgres_settings_load_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_postgres_environment(monkeypatch)

    settings = PostgresSettings.from_env()

    assert settings == PostgresSettings(
        host="localhost",
        port=5433,
        database="olist",
        user="olist",
        password="local-secret",
        schema="raw",
    )


def test_connection_kwargs_use_psycopg_parameter_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_postgres_environment(monkeypatch)

    settings = PostgresSettings.from_env()

    assert settings.connection_kwargs() == {
        "host": "localhost",
        "port": 5433,
        "dbname": "olist",
        "user": "olist",
        "password": "local-secret",
    }


def test_postgres_settings_rejects_empty_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_postgres_environment(
        monkeypatch,
        POSTGRES_PASSWORD="",
    )

    with pytest.raises(
        PostgresConfigurationError,
        match="POSTGRES_PASSWORD is missing",
    ):
        PostgresSettings.from_env()


@pytest.mark.parametrize("port", ["not-a-number", "0", "65536"])
def test_postgres_settings_reject_invalid_port(
    monkeypatch: pytest.MonkeyPatch,
    port: str,
) -> None:
    set_postgres_environment(monkeypatch, POSTGRES_PORT=port)

    with pytest.raises(PostgresConfigurationError, match="POSTGRES_PORT"):
        PostgresSettings.from_env()


@pytest.mark.parametrize(
    "schema",
    [
        "raw-data",
        "raw data",
        "raw; drop schema public",
        "123raw",
    ],
)
def test_postgres_settings_reject_invalid_schema(
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    set_postgres_environment(monkeypatch, POSTGRES_SCHEMA=schema)

    with pytest.raises(
        PostgresConfigurationError,
        match="POSTGRES_SCHEMA",
    ):
        PostgresSettings.from_env()
