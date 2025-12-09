from typing import Annotated

import pandas as pd
import typer
from google.cloud import bigquery

from config.logconfig import logger


def load_dataset_to_bq(
    df: Annotated[pd.DataFrame, typer.Option(help="Pandas dataframe to load.")],
    table_id: Annotated[str, typer.Option(help="ID of the bigquery table to create.")],
    project_id: Annotated[str, typer.Option(help="ID of the bigquery project.")],
) -> None:
    """
    Load a dataframe into a BigQuery cloud storage.

    Args:
            df (pd.DataFrame): Pandas dataframe to store remotely.
            table_id (str): ID of the bigquery table to create.
            project_id (str): ID of the bigquery project.

    Returns:
            None:
    """
    client = bigquery.Client(project=project_id)
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    logger.info(f"Loaded dataset into table {table_id}.")


# def run(
# processed_dir: Annotated[
# str, typer.Option(help="Location where the processed data will be stored.")
# ],
# project_id: Annotated[str, typer.Option(help="ID of the bigquery project.")],
# dataset: Annotated[
# str, typer.Option(help="Name of the dataset to use for table IDs.")
# ],
# ) -> None:
# """
# Load processed data into bigquery cloud storage.

# Args:
# processed_dir (str): Location where the processed data will be stored.
# project_id (str): ID of the bigquery project.
# dataset (str): Name of the dataset to use for table IDs.
# """
# logger.info("Loading processed data into BigQuery")
# pass
