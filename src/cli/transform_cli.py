import logging
from pathlib import Path
from typing import Annotated

import typer

from config.logconfig import setup_logging
from etl.transform.transform_data import (
    save_processed,
    transform_customers,
    transform_geolocation,
    transform_order_items,
    transform_orders,
    transform_payments,
    transform_products,
    transform_reviews,
    transform_sellers,
    transform_translation,
)
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

    logger.info("Loading data...")
    customers = load_csv(name="olist_customers_dataset.csv", _dir=raw_data_dir)
    order_items = load_csv(name="olist_order_items_dataset.csv", _dir=raw_data_dir)
    orders = load_csv(name="olist_orders_dataset.csv", _dir=raw_data_dir)
    payments = load_csv(name="olist_order_payments_dataset.csv", _dir=raw_data_dir)
    sellers = load_csv(name="olist_sellers_dataset.csv", _dir=raw_data_dir)
    geolocation = load_csv(name="olist_geolocation_dataset.csv", _dir=raw_data_dir)
    translation = load_csv(
        name="product_category_name_translation.csv", _dir=raw_data_dir
    )
    products = load_csv(name="olist_products_dataset.csv", _dir=raw_data_dir)
    reviews = load_csv(name="olist_order_reviews_dataset.csv", _dir=raw_data_dir)

    typer.echo("Running transformation...")
    customers_transformed = transform_customers(customers)
    order_items_transformed = transform_order_items(
        order_items=order_items, exchange_rate=exchange_rate
    )
    orders_transformed = transform_orders(orders)
    payments_transformed = transform_payments(payments)
    products_transformed = transform_products(products)
    sellers_transformed = transform_sellers(sellers)
    geolocation_transformed = transform_geolocation(geolocation)
    translation_transformed = transform_translation(translation)
    reviews_transformed = transform_reviews(reviews)

    save_processed(
        df=customers_transformed, name="customers", processed_dir=processed_data_dir
    )
    save_processed(
        df=order_items_transformed, name="order_items", processed_dir=processed_data_dir
    )
    save_processed(
        df=orders_transformed, name="orders", processed_dir=processed_data_dir
    )
    save_processed(
        df=products_transformed, name="products", processed_dir=processed_data_dir
    )
    save_processed(
        df=payments_transformed, name="payments", processed_dir=processed_data_dir
    )
    save_processed(
        df=sellers_transformed, name="sellers", processed_dir=processed_data_dir
    )
    save_processed(
        df=geolocation_transformed, name="geolocation", processed_dir=processed_data_dir
    )
    save_processed(
        df=translation_transformed, name="translation", processed_dir=processed_data_dir
    )
    save_processed(
        df=reviews_transformed, name="reviews", processed_dir=processed_data_dir
    )

    logger.info(f"Processed data saved in {processed_data_dir}!")


if __name__ == "__main__":
    run()
