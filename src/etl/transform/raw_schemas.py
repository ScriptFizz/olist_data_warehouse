import pandas as pd
import pandera as pa
from pandera.typing import Series

from config.logconfig import logger


def validate(df: pd.DataFrame, schema: pa.SchemaModel) -> None:
    """
    Validate a given pandas DataFrame according to the schema provided.

    Args:
        df (pd.DataFrame): pandas DataFrame to validate.
        schema (pa.SchemaModel): pandera Schema Model that the dataframe has to obey.

    Returns:
        None:
    """

    try:
        return schema.validate(df)
    except pa.errors.SchemaError as e:
        logger.error(e)
        raise


class CustomersSchema(pa.SchemaModel):
    customer_id: Series[str]
    customer_unique_id: Series[str]
    customer_zip_code_prefix: Series[str] = pa.Field(nullable=True)
    customer_city: Series[str]
    customer_state: Series[str]

    class Config:
        coerce = True


class OrdersSchema(pa.SchemaModel):
    order_id: Series[str]
    customer_id: Series[str]
    order_status: Series[str]

    order_purchase_timestamp: Series[pa.DateTime]
    order_approved_at: Series[pa.DateTime] = pa.Field(nullable=True)
    order_delivered_carrier_date: Series[pa.DateTime] = pa.Field(nullable=True)
    order_delivered_customer_date: Series[pa.DateTime] = pa.Field(nullable=True)
    order_estimated_delivery_date: Series[pa.DateTime]

    class Config:
        coerce = True


class OrderItemsSchema(pa.SchemaModel):
    order_id: Series[str]
    order_item_id: Series[int]
    product_id: Series[str]
    seller_id: Series[str]
    shipping_limit_date: Series[pa.DateTime]
    price: Series[float]
    freight_value: Series[float]

    class Config:
        coerce = True


class PaymentsSchema(pa.SchemaModel):
    order_id: Series[str]
    payment_sequential: Series[int]
    payment_type: Series[str]
    payment_installments: Series[int]
    payment_value: Series[float]

    class Config:
        coerce = True


class ProductsSchema(pa.SchemaModel):
    product_id: Series[str]
    product_category_name: Series[str] = pa.Field(nullable=True)
    product_name_lenght: Series[int] = pa.Field(nullable=True)
    product_description_lenght: Series[int] = pa.Field(nullable=True)
    product_photos_qty: Series[int] = pa.Field(nullable=True)
    product_weight_g: Series[int] = pa.Field(nullable=True)
    product_length_cm: Series[int] = pa.Field(nullable=True)
    product_height_cm: Series[int] = pa.Field(nullable=True)
    product_width_cm: Series[int] = pa.Field(nullable=True)

    class Config:
        coerce = True


class GeolocationSchema(pa.SchemaModel):
    geolocation_zip_code_prefix: Series[str]
    geolocation_lat: Series[float] = pa.Field(nullable=True)
    geolocation_lng: Series[float] = pa.Field(nullable=True)
    geolocation_city: Series[str]
    geolocation_state: Series[str]

    class Config:
        coerce = True


class SellersSchema(pa.SchemaModel):
    seller_id: Series[str]
    seller_zip_code_prefix: Series[str]
    seller_city: Series[str]
    seller_state: Series[str]

    class Config:
        coerce = True


class TranslationSchema(pa.SchemaModel):
    product_category_name: Series[str]
    product_category_name_english: Series[str]

    class Config:
        coerce = True
