import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from etl.transform.processed_schemas import (
    CustomersProcessedSchema,
    GeolocationProcessedSchema,
    OrderItemsProcessedSchema,
    OrdersProcessedSchema,
    PaymentsProcessedSchema,
    ProductsProcessedSchema,
    SellersProcessedSchema,
    TranslationProcessedSchema,
)
from etl.transform.raw_schemas import (
    CustomersSchema,
    GeolocationSchema,
    OrderItemsSchema,
    OrdersSchema,
    PaymentsSchema,
    ProductsSchema,
    SellersSchema,
    TranslationSchema,
    validate,
)

logger = logging.getLogger(__name__)
# def load_raw_csv(name: str, raw_dir: str) -> pd.DataFrame:
# """
# Load raw data from csv file and return a Pandas dataframe.

# Args:
# name (str): name of the file to load data from.
# raw_dir (str): name of the directory the raw data is stored.

# Returns:
# pd.DataFrame: pandas dataframe with the file data.
# """

# path = Path(raw_dir) / name
# try:
# df = pd.read_csv(path)
# except FileNotFoundError:
# logger.error(f"Missing file: {path}")
# raise
# except pd.errors.ParserError:
# logger.error(f"CSV parsing error in file: {path}")
# raise
# return df


def save_processed(df: pd.DataFrame, name: str, processed_dir: str) -> None:
    """
    Save processed data from a Pandas dataframe to a specified directory.

    Args:
                    df (pd.DataFrame): pandas DataFrame to store.
                    name (str): name of the file to save.
                    processed_dir (str): name of the directory the processed data will be stored.

    Returns:
                    None:
    """

    try:
        path = Path(processed_dir)
        path.mkdir(parents=True, exist_ok=True)
        df.to_csv(path / f"{name}.csv", index=False)
    except OSError as e:
        logger.error(f"Error saving file {name}: {e}")
        raise
    logger.info(f"Saved processed file: {name}")


def transform_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the customer dataset.

    Args:
                    customers (pd.DataFrame): pandas DataFrame with customers data.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed customer data.
    """

    validate(df=customers, schema=CustomersSchema)
    customers_transformed = customers.rename(
        columns={
            "customer_id": "customer_id",
            "customer_unique_id": "customer_uid",
            "customer_city": "city",
            "customer_zip_code_prefix": "zipcode",
            "customer_state": "state",
        }
    )
    validate(df=customers_transformed, schema=CustomersProcessedSchema)
    return customers_transformed


def transform_order_items(
    order_items: pd.DataFrame, exchange_rate: dict
) -> pd.DataFrame:
    """
    Transform the order items dataset.

    Args:
                    order_items (pd.DataFrame): pandas DataFrame with order items data.
                    exchange_rate (Dict): structured data about currencies exchange rates.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed order items data.
    """

    validate(df=order_items, schema=OrderItemsSchema)
    rate = exchange_rate["rates"]["USD"]
    order_items_transformed = order_items.copy(deep=True)
    order_items_transformed["shipping_limit_date"] = pd.to_datetime(
        order_items["shipping_limit_date"]
    )
    order_items_transformed["price"] = order_items["price"] * rate
    validate(df=order_items_transformed, schema=OrderItemsProcessedSchema)
    return order_items_transformed


def transform_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the orders dataset.

    Args:
                    order_items (pd.DataFrame): pandas DataFrame with orders data.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed orders data.
    """

    validate(df=orders, schema=OrdersSchema)
    orders_transformed = orders.copy(deep=True)

    orders_transformed["order_purchase_timestamp"] = pd.to_datetime(
        orders["order_purchase_timestamp"]
    )
    orders_transformed["order_approved_at"] = pd.to_datetime(
        orders["order_approved_at"]
    )
    orders_transformed["order_delivered_carrier_date"] = pd.to_datetime(
        orders["order_delivered_carrier_date"]
    )
    orders_transformed["order_delivered_customer_date"] = pd.to_datetime(
        orders["order_delivered_customer_date"]
    )
    orders_transformed["order_estimated_delivery_date"] = pd.to_datetime(
        orders["order_estimated_delivery_date"]
    )

    orders_transformed = orders_transformed.rename(
        columns={
            "order_purchase_timestamp": "purchase_ts",
            "order_approved_at": "approval_ts",
            "order_delivered_carrier_date": "delivery_carrier_ts",
            "order_delivered_customer_date": "delivery_customer_ts",
            "order_estimated_delivery_date": "estimated_delivery_ts",
        }
    )
    validate(df=orders_transformed, schema=OrdersProcessedSchema)
    return orders_transformed


def transform_products(products: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the products dataset.

    Args:
                    products (pd.DataFrame): pandas DataFrame with products data.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed products data.
    """

    validate(df=products, schema=ProductsSchema)
    products_transformed = products.copy(deep=True)
    for cname in products_transformed.select_dtypes(include=["float"]):
        col = products_transformed[cname].dropna()
        if np.all(np.isclose(col % 1, 0)):
            col_transformed: pd.Series[Any] = products_transformed[cname].astype(
                "Int64"
            )
            products_transformed[cname] = col_transformed  # type: ignore[call-overload]

    products_transformed = products_transformed.rename(
        columns={
            "product_id": "product_id",
            "product_category_name": "name",
            "product_name_lenght": "name_length",
            "product_description_lenght": "description_length",
            "product_photos_qty": "photos_qty",
            "product_weight_g": "weight_g",
            "product_length_cm": "length_cm",
            "product_height_cm": "height_cm",
            "product_width_cm": "width_cm",
        }
    )
    validate(df=products_transformed, schema=ProductsProcessedSchema)
    return products_transformed


def transform_sellers(sellers: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the sellers dataset.

    Args:
                    sellers (pd.DataFrame): pandas DataFrame with sellers data.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed sellers data.
    """

    validate(df=sellers, schema=SellersSchema)
    sellers_transformed = sellers.rename(
        columns={
            "seller_id": "seller_id",
            "seller_city": "city",
            "seller_zip_code_prefix": "zipcode",
            "seller_state": "state",
        }
    )
    validate(df=sellers_transformed, schema=SellersProcessedSchema)
    return sellers_transformed


def transform_geolocation(geolocation: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the geolocation dataset.

    Args:
                    geolocation (pd.DataFrame): pandas DataFrame with geolocation data.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed geolocation data.
    """

    validate(df=geolocation, schema=GeolocationSchema)
    geolocation_transformed = geolocation.rename(
        columns={
            "geolocation_lat": "lat",
            "geolocation_lng": "lng",
            "geolocation_city": "city",
            "geolocation_zip_code_prefix": "zipcode",
            "geolocation_state": "state",
        }
    )
    validate(df=geolocation_transformed, schema=GeolocationProcessedSchema)
    return geolocation_transformed


def transform_payments(payments: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the payments dataset.

    Args:
                    geolocation (pd.DataFrame): pandas DataFrame with payments data.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed payments data.
    """

    validate(df=payments, schema=PaymentsSchema)
    payments_transformed = payments.rename(
        columns={
            "order_id": "order_id",
            "payment_sequential": "sequential",
            "payment_type": "type",
            "payment_installments": "installments",
            "payment_value": "value",
        }
    )
    validate(df=payments_transformed, schema=PaymentsProcessedSchema)
    return payments_transformed


def transform_translation(translation: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the translation dataset.

    Args:
                    translation (pd.DataFrame): pandas DataFrame with translation data.

    Returns:
                    pd.DataFrame: pandas DataFrame of the transformed translation data.
    """

    validate(df=translation, schema=TranslationSchema)
    translation_transformed = translation.rename(
        columns={
            "product_category_name": "name_brz",
            "product_category_name_english": "name_eng",
        }
    )
    validate(df=translation_transformed, schema=TranslationProcessedSchema)
    return translation_transformed
