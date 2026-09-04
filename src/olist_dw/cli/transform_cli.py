import logging
from typing import Annotated

import pandas as pd
import typer

from olist_dw.config.logconfig import setup_logging
from olist_dw.etl.registry.olist_tables import TABLES
from olist_dw.etl.transform.dataset_contracts import (
    validate_referential_integrity,
)
from olist_dw.etl.transform.raw_schemas import validate
from olist_dw.etl.transform.transform_data import save_processed
from olist_dw.etl.utils.utils_methods import load_csv, load_params

setup_logging()
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def run(
    raw_data_dir: Annotated[
        str | None, typer.Option(help="location where the raw data is stored.")
    ] = None,
    processed_data_dir: Annotated[
        str | None, typer.Option(help="location where processed data will be stored.")
    ] = None,
) -> None:
    """
    Process data stored in a directory and store the results in a specified directory.

    Args:
        raw_data_dir (str): location where the raw data is stored.
        processed_data_dir (str): location where processed data will be stored.

    Returns:
        None:
    """

    params = load_params()

    raw_data_dir = raw_data_dir or params["paths"]["raw_data_dir"]
    processed_data_dir = processed_data_dir or params["paths"]["processed_data_dir"]

    logger.info("Transforming data...")
    processed_tables: dict[str, pd.DataFrame] = {}

    for table_conf in TABLES.values():
        df = load_csv(name=table_conf.raw_filename, _dir=raw_data_dir)

        df = validate(df=df, schema=table_conf.raw_schema)

        df_transformed = table_conf.transform(df)

        df_transformed = validate(df=df_transformed, schema=table_conf.processed_schema)

        processed_tables[table_conf.name] = df_transformed

    logger.info("Validating relationships across processed datasets...")
    validate_referential_integrity(processed_tables)

    logger.info("Saving validated processed datasets...")
    for table_name, dataframe in processed_tables.items():
        save_processed(df=dataframe, name=table_name, processed_dir=processed_data_dir)

    logger.info(f"Processed data saved in {processed_data_dir}!")


if __name__ == "__main__":
    run()
