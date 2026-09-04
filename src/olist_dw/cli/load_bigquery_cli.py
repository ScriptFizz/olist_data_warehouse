import logging
from typing import Annotated

import pandas as pd
import typer

from olist_dw.config.logconfig import setup_logging
from olist_dw.etl.load.ingestion_batch import (
    IngestionBatch,
    attach_ingestion_metadata,
)
from olist_dw.etl.load.load_bigquery import load_dataset_to_bq
from olist_dw.etl.registry.olist_tables import TABLES
from olist_dw.etl.transform.dataset_contracts import validate_referential_integrity
from olist_dw.etl.transform.raw_schemas import validate
from olist_dw.etl.utils.utils_methods import load_csv, load_params

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

    tables: dict[str, pd.DataFrame] = {}
    for table_config in TABLES.values():
        dataframe = load_csv(
            name=table_config.processed_filename,
            _dir=processed_dir,
        )
        tables[table_config.name] = validate(
            dataframe,
            table_config.processed_schema,
        )

    validate_referential_integrity(tables)
    batch = IngestionBatch.create(
        source_name="processed_csv",
        tables=tables,
    )

    typer.echo("Storing data to BigQuery dataset...")
    for table_conf in TABLES.values():
        load_dataset_to_bq(
            df=attach_ingestion_metadata(tables[table_conf.name], batch),
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_conf.name,
            schema_model=table_conf.processed_schema,
        )

    logger.info("Data upload succesful!")


if __name__ == "__main__":
    run()
