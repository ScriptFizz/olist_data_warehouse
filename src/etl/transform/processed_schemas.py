import pandera as pa
from pandera.typing import Series


class CustomersProcessedSchema(pa.SchemaModel):
    customer_id: Series[str]
    customer_uid: Series[str]
    zipcode: Series[str] = pa.Field(nullable=True)
    city: Series[str]
    state: Series[str]

    class Config:
        coerce = True


class OrdersProcessedSchema(pa.SchemaModel):
    order_id: Series[str]
    customer_id: Series[str]
    order_status: Series[str]

    purchase_ts: Series[pa.DateTime]
    approval_ts: Series[pa.DateTime] = pa.Field(nullable=True)
    delivery_carrier_ts: Series[pa.DateTime] = pa.Field(nullable=True)
    delivery_customer_ts: Series[pa.DateTime] = pa.Field(nullable=True)
    estimated_delivery_ts: Series[pa.DateTime]

    class Config:
        coerce = True


class OrderItemsProcessedSchema(pa.SchemaModel):
    order_id: Series[str]
    order_item_id: Series[int]
    product_id: Series[str]
    seller_id: Series[str]
    shipping_limit_date: Series[pa.DateTime]
    price: Series[float]
    freight_value: Series[float]

    class Config:
        coerce = True


class PaymentsProcessedSchema(pa.SchemaModel):
    order_id: Series[str]
    sequential: Series[int]
    type: Series[str]
    installments: Series[int]
    value: Series[float]

    class Config:
        coerce = True


class ProductsProcessedSchema(pa.SchemaModel):
    product_id: Series[str]
    name: Series[str] = pa.Field(nullable=True)
    name_length: Series[int] = pa.Field(nullable=True)
    description_length: Series[int] = pa.Field(nullable=True)
    photos_qty: Series[int] = pa.Field(nullable=True)
    weight_g: Series[int] = pa.Field(nullable=True)
    length_cm: Series[int] = pa.Field(nullable=True)
    height_cm: Series[int] = pa.Field(nullable=True)
    width_cm: Series[int] = pa.Field(nullable=True)

    class Config:
        coerce = True


class GeolocationProcessedSchema(pa.SchemaModel):
    zipcode_prefix: Series[str]
    lat: Series[float] = pa.Field(nullable=True)
    lng: Series[float] = pa.Field(nullable=True)
    city: Series[str]
    state: Series[str]

    class Config:
        coerce = True


class SellersProcessedSchema(pa.SchemaModel):
    seller_id: Series[str]
    zip_code_prefix: Series[str]
    city: Series[str]
    state: Series[str]

    class Config:
        coerce = True


class TranslationProcessedSchema(pa.SchemaModel):
    name_brz: Series[str]
    name_eng: Series[str]

    class Config:
        coerce = True
