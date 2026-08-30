import psycopg
import typer

from olist_dw.config.postgres import PostgresConfigurationError, PostgresSettings
from olist_dw.etl.load.postgres import check_postgres_connection

app = typer.Typer(
    help="Verify PostgreSQL configuration and connectivity.",
    pretty_exceptions_show_locals=False,
)


@app.command()
def run() -> None:
    try:
        settings = PostgresSettings.from_env()
    except PostgresConfigurationError as exc:
        typer.echo(f"PostgreSQL configuration error: {exc}", err=True)
        raise typer.Exit(code=2) from None

    try:
        connection_info = check_postgres_connection(settings)
    except psycopg.Error:
        typer.echo(
            "PostgreSQL connection failed: "
            f"host={settings.host}, "
            f"port={settings.port}, "
            f"database={settings.database}, "
            f"user={settings.user}. "
            "Verify that the service is running and the credentials are correct.",
            err=True,
        )
        raise typer.Exit(code=1) from None

    typer.echo(
        "PostgreSQL connection successful: "
        f"database={connection_info.database}, "
        f"user={connection_info.user}, "
        f"version={connection_info.server_version}"
    )


if __name__ == "__main__":
    app()
