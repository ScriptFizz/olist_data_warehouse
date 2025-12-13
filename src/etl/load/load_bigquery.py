import logging
from typing import Annotated

import pandas as pd
import typer
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def load_dataset_to_bq(
    df: Annotated[pd.DataFrame, typer.Option(help="Pandas dataframe to load.")],
    project_id: Annotated[str, typer.Option(help="ID of the bigquery project.")],
    dataset_id: Annotated[str, typer.Option(help="ID of the bigquery dataset.")],
    table_id: Annotated[str, typer.Option(help="ID of the bigquery table to create.")],
) -> None:
    """
    Load a dataframe into a BigQuery cloud storage.

    Args:
            df (pd.DataFrame): Pandas dataframe to store remotely.
            project_id (str): ID of the bigquery project.
            dataset_id (Str): ID of the bigquery dataset.
            table_id (str): ID of the bigquery table to create.

    Returns:
            None:
    """
    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    logger.info(f"Loaded dataset into table {full_table_id}.")
