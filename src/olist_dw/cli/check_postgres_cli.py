import typer

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.postgres import check_postgres_connection

app = typer.Typer(help="Verify PostgreSQL configuration and connectivity.")


@app.command()
def run() -> None:
    settings = PostgresSettings.from_env()
    connection_info = check_postgres_connection(settings)

    typer.echo(
        "PostgreSQL connection successful: "
        f"database={connection_info.database}, "
        f"user={connection_info.user}, "
        f"version={connection_info.server_version}"
    )


if __name__ == "__main__":
    app()
