import logging
from pathlib import Path
from typing import Annotated

import typer

from config.logconfig import setup_logging
from etl.registry.olist_tables import TABLES

# from etl.transform.transform_data import (
# save_processed,
# transform_customers,
# transform_geolocation,
# transform_order_items,
# transform_orders,
# transform_payments,
# transform_products,
# transform_reviews,
# transform_sellers,
# transform_translation,
# )
from etl.registry.tables import TransformContext
from etl.transform.raw_schemas import validate
from etl.transform.transform_data import save_processed
from etl.utils.utils_methods import load_csv, load_dict, load_params

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
    exchange_rate = load_dict(Path(raw_data_dir) / "exchange_rate.json")
    ctx = TransformContext(exchange_rate=exchange_rate)

    logger.info("Transforming data...")
    for table_conf in TABLES.values():
        df = load_csv(name=table_conf.raw_filename, _dir=raw_data_dir)

        df = validate(df=df, schema=table_conf.raw_schema)

        kwargs = table_conf.kwargs_factory(ctx) if table_conf.kwargs_factory else {}
        df_transformed = table_conf.transform(df, **kwargs)

        df_transformed = validate(df=df_transformed, schema=table_conf.processed_schema)

        save_processed(
            df=df_transformed, name=table_conf.name, processed_dir=processed_data_dir
        )

    logger.info(f"Processed data saved in {processed_data_dir}!")


if __name__ == "__main__":
    run()
