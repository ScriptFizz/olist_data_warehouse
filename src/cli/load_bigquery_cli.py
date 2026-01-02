import logging
from typing import Annotated

import typer

from config.logconfig import setup_logging
from etl.load.load_bigquery import load_dataset_to_bq
from etl.registry.olist_tables import TABLES
from etl.utils.utils_methods import load_csv, load_params

setup_logging()
logger = logging.getLogger(__name__)


app = typer.Typer()


@app.command()
def run(
    processed_dir: Annotated[
        str | None, typer.Option(help="Location where the processed data is stored.")
    ] = None,
    project_id: Annotated[
        str | None, typer.Option(help="ID of the bigquery project.")
    ] = None,
    dataset_id: Annotated[
        str | None, typer.Option(help="ID of the dataset to use for table IDs.")
    ] = None,
) -> None:
    """
    Load processed data into bigquery cloud storage.

    Args:
    processed_dir (str): Location where the processed data will be stored.
    project_id (str): ID of the bigquery project.
    dataset (str): Name of the dataset to use for table IDs.
    """

    logger.info("Loading processed data into BigQuery")
    params = load_params()

    processed_dir = processed_dir or params["paths"]["processed_data_dir"]
    project_id = project_id or params["bigquery"]["project_id"]
    dataset_id = dataset_id or params["bigquery"]["dataset_id"]

    typer.echo("Storing data to BigQuery dataset...")
    for table_conf in TABLES.values():
        df = load_csv(name=table_conf.processed_filename, _dir=processed_dir)
        load_dataset_to_bq(
            df=df,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_conf.name,
            schema_model=table_conf.processed_schema,
        )

    logger.info("Data upload succesful!")


if __name__ == "__main__":
    run()
