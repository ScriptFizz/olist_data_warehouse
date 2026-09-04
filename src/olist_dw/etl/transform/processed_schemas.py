import pandas as pd
import pandera as pa
from pandera.typing import Series

from olist_dw.etl.transform.raw_schemas import (
    BRAZIL_STATE_CODES,
    OLIST_ORDER_STATUSES,
)


class CustomersProcessedSchema(pa.SchemaModel):
    customer_id: Series[str] = pa.Field(unique=True)
    customer_uid: Series[str]
    zipcode: Series[str] = pa.Field(nullable=True)
    city: Series[str]
    state: Series[str] = pa.Field(isin=BRAZIL_STATE_CODES)

    class Config:
        coerce = True
        strict = True


class OrdersProcessedSchema(pa.SchemaModel):
    order_id: Series[str] = pa.Field(unique=True)
    customer_id: Series[str]
    order_status: Series[str] = pa.Field(isin=OLIST_ORDER_STATUSES)

    purchase_ts: Series[pa.DateTime]
    approval_ts: Series[pa.DateTime] = pa.Field(nullable=True)
    delivery_carrier_ts: Series[pa.DateTime] = pa.Field(nullable=True)
    delivery_customer_ts: Series[pa.DateTime] = pa.Field(nullable=True)
    estimated_delivery_ts: Series[pa.DateTime]

    class Config:
        coerce = True
        strict = True


class OrderItemsProcessedSchema(pa.SchemaModel):
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


class PaymentsProcessedSchema(pa.SchemaModel):
    order_id: Series[str]
    sequential: Series[int]
    type: Series[str]
    installments: Series[int]
    value: Series[float]

    class Config:
        coerce = True
        strict = True
        unique=["order_id", "sequential"]


class ProductsProcessedSchema(pa.SchemaModel):
    product_id: Series[str] = pa.Field(unique=True)
    name: Series[str] = pa.Field(nullable=True)
    name_length: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    description_length: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    photos_qty: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    weight_g: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    length_cm: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    height_cm: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    width_cm: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = True


class GeolocationProcessedSchema(pa.SchemaModel):
    zipcode: Series[str]
    lat: Series[float] = pa.Field(ge=-90, le=90, nullable=True)
    lng: Series[float] = pa.Field(ge=-180, le=180, nullable=True)
    city: Series[str]
    state: Series[str] = pa.Field(isin=BRAZIL_STATE_CODES)

    class Config:
        coerce = True
        strict = True


class SellersProcessedSchema(pa.SchemaModel):
    seller_id: Series[str] = pa.Field(unique=True)
    zipcode: Series[str]
    city: Series[str]
    state: Series[str] = pa.Field(isin=BRAZIL_STATE_CODES)

    class Config:
        coerce = True
        strict = True


class TranslationProcessedSchema(pa.SchemaModel):
    name_brz: Series[str] = pa.Field(unique=True)
    name_eng: Series[str]

    class Config:
        coerce = True
        strict = True


class ReviewsProcessedSchema(pa.SchemaModel):
    review_id: Series[str]
    order_id: Series[str]
    score: Series[int] = pa.Field(ge=1, le=5)
    title: Series[str] = pa.Field(nullable=True)
    message: Series[str] = pa.Field(nullable=True)
    creation_date: Series[pa.DateTime] = pa.Field(nullable=True)
    answer_ts: Series[pa.DateTime] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = True
