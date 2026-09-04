import logging
from typing import cast

import pandas as pd
import pandera as pa
from pandera.typing import Series

logger = logging.getLogger(__name__)


OLIST_ORDER_STATUSES = frozenset(
    {
        "approved",
        "canceled",
        "created",
        "delivered",
        "invoiced",
        "processing",
        "shipped",
        "unavailable",
    }
)


BRAZIL_STATE_CODES = frozenset(
    {
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    }
)


def validate(df: pd.DataFrame, schema: type[pa.SchemaModel]) -> pd.DataFrame:
    """
    Validate a given pandas DataFrame according to the schema provided.

    Args:
        df (pd.DataFrame): pandas DataFrame to validate.
        schema (pa.SchemaModel): pandera Schema Model that the dataframe has to obey.

    Returns:
        pd.DataFrame: Pandas DataFrame validated.
    """

    try:
        validated = schema.validate(df, lazy=False)
        return cast(pd.DataFrame, validated)
    except pa.errors.SchemaError as e:
        logger.error("Dataframe validation failed: %s", e)
        raise


class CustomersSchema(pa.SchemaModel):
    customer_id: Series[str] = pa.Field(unique=True)
    customer_unique_id: Series[str]
    customer_zip_code_prefix: Series[str] = pa.Field(nullable=True)
    customer_city: Series[str]
    customer_state: Series[str] = pa.Field(isin=BRAZIL_STATE_CODES)

    class Config:
        coerce = True
        strict = True


class OrdersSchema(pa.SchemaModel):
    order_id: Series[str] = pa.Field(unique=True)
    customer_id: Series[str]
    order_status: Series[str] = pa.Field(isin=OLIST_ORDER_STATUSES)

    order_purchase_timestamp: Series[pa.DateTime]
    order_approved_at: Series[pa.DateTime] = pa.Field(nullable=True)
    order_delivered_carrier_date: Series[pa.DateTime] = pa.Field(nullable=True)
    order_delivered_customer_date: Series[pa.DateTime] = pa.Field(nullable=True)
    order_estimated_delivery_date: Series[pa.DateTime]

    class Config:
        coerce = True
        strict = True


class OrderItemsSchema(pa.SchemaModel):
    order_id: Series[str]
    order_item_id: Series[int] = pa.Field(ge=1)
    product_id: Series[str]
    seller_id: Series[str]
    shipping_limit_date: Series[pa.DateTime]
    price: Series[float] = pa.Field(ge=0)
    freight_value: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
        strict = True
        unique = ["order_id", "order_item_id"]


class PaymentsSchema(pa.SchemaModel):
    order_id: Series[str]
    payment_sequential: Series[int]
    payment_type: Series[str]
    payment_installments: Series[int]
    payment_value: Series[float]

    class Config:
        coerce = True
        strict = True
        unique=["order_id", "payment_sequential"]


class ProductsSchema(pa.SchemaModel):
    product_id: Series[str] = pa.Field(unique=True)
    product_category_name: Series[str] = pa.Field(nullable=True)
    product_name_lenght: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    product_description_lenght: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    product_photos_qty: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    product_weight_g: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    product_length_cm: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    product_height_cm: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    product_width_cm: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = True


class GeolocationSchema(pa.SchemaModel):
    geolocation_zip_code_prefix: Series[str]
    geolocation_lat: Series[float] = pa.Field(ge=-90, le=90, nullable=True)
    geolocation_lng: Series[float] = pa.Field(ge=-180, le=180, nullable=True)
    geolocation_city: Series[str]
    geolocation_state: Series[str] = pa.Field(isin=BRAZIL_STATE_CODES)

    class Config:
        coerce = True
        strict = True


class SellersSchema(pa.SchemaModel):
    seller_id: Series[str] = pa.Field(unique=True)
    seller_zip_code_prefix: Series[str]
    seller_city: Series[str]
    seller_state: Series[str] = pa.Field(isin=BRAZIL_STATE_CODES)

    class Config:
        coerce = True
        strict = True


class TranslationSchema(pa.SchemaModel):
    product_category_name: Series[str] = pa.Field(unique=True)
    product_category_name_english: Series[str]

    class Config:
        coerce = True
        strict = True


class ReviewsSchema(pa.SchemaModel):
    review_id: Series[str]
    order_id: Series[str]
    review_score: Series[int] = pa.Field(ge=1, le=5)
    review_comment_title: Series[str] = pa.Field(nullable=True)
    review_comment_message: Series[str] = pa.Field(nullable=True)
    review_creation_date: Series[pa.DateTime] = pa.Field(nullable=True)
    review_answer_timestamp: Series[pa.DateTime] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = True
