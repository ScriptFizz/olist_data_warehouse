import logging
from typing import Annotated

import typer

from config.logconfig import setup_logging
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

    datafiles = params["data"]["processed"]

    typer.echo("Storing data to BigQuery dataset...")
    for table_name, table_filename in datafiles.items():
        print(f"table_name: {table_name} - table_filename: {table_filename}")
        df = load_csv(name=table_filename, _dir=processed_dir)
        print(f"df.shape: {df.shape}")
        # load_dataset_to_bq(df = df, project_id = project_id, dataset_id = dataset_id)

    logger.info("Data upload succesful!")


if __name__ == "__main__":
    run()
