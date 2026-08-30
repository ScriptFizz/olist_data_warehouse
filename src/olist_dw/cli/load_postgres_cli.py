import pandas as pd
import psycopg
import typer

from olist_dw.config.logconfig import setup_logging
from olist_dw.config.postgres import (
    PostgresConfigurationError,
    PostgresSettings,
)
from olist_dw.etl.load.postgres import load_tables_to_postgres
from olist_dw.etl.registry.olist_tables import TABLES
from olist_dw.etl.transform.dataset_contracts import (
    DatasetContractError,
    validate_referential_integrity,
)
from olist_dw.etl.transform.raw_schemas import validate
from olist_dw.etl.utils.utils_methods import load_csv, load_params

setup_logging()


app = typer.Typer(
    help="Load validated processed datasets into PostgreSQL",
    pretty_exceptions_show_locals=False,
)


@app.command()
def run(
    processed_dir: str | None = typer.Option(
        None,
        help="Directory containing processed CSV files.",
    ),
) -> None:
    params = load_params()
    resolved_processed_dir = processed_dir or params["paths"]["processed_data_dir"]

    try:
        settings = PostgresSettings.from_env()

        tables: dict[str, pd.DataFrame] = {}

        for table_config in TABLES.values():
            dataframe = load_csv(
                name=table_config.processed_filename,
                _dir=resolved_processed_dir,
            )
            tables[table_config.name] = validate(
                dataframe,
                table_config.processed_schema,
            )

        validate_referential_integrity(tables)

        result = load_tables_to_postgres(
            tables=tables,
            schemas={
                name: table_config.processed_schema
                for name, table_config in TABLES.items()
            },
            settings=settings,
        )

    except PostgresConfigurationError as exc:
        typer.echo(f"PostgreSQL configuration error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    except DatasetContractError as exc:
        typer.echo(f"Dataset contract error: {exc}", err=True)
        raise typer.Exit(code=3) from None
    except psycopg.Error:
        typer.echo(
            "PostgreSQL load failed; the transaction was rolled back. "
            "Previously published raw tables remain unchanged.",
            err=True,
        )
        raise typer.Exit(code=1) from None

    typer.echo(
        f"PostgreSQL load completed: schema={result.schema}, "
        f"tables={len(result.row_counts)}, "
        f"rows={sum(result.row_counts.values())}"
    )


if __name__ == "__main__":
    app()
