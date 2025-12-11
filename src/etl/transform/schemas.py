import pandera as pa
from pandera.typing import Series


class CustomersSchema(pa.SchemaModel):
    customer_id: Series[str]
    customer_unique_id: Series[str]
    customer_zip_code_prefix: Series[str]
    customer_city: Series[str]
    customer_state: Series[str]

    class Config:
        coerce = True


class OrdersSchema(pa.SchemaModel):
    order_id: Series[str]
    customer_id: Series[str]
    order_status: Series[str]

    order_purchase_timestamp: Series[pa.DateTime]
    order_approve_at: Series[pa.DateTime] = pa.Field(nullable=True)
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
    payment_sequential: Series[str]
    payment_type: Series[str]
    payment_installments: Series[int]
    payment_value: Series[float]

    class Config:
        coerce = True


class ProductsSchema(pa.SchemaModel):
    product_id: Series[str]
    product_category_name: Series[str]
    product_name_lenght: Series[int]
    product_description_lenght: Series[int]
    product_photos_qty: Series[int]
    product_weight_g: Series[int]
    product_length_cm: Series[int]
    product_height_cm: Series[int]
    product_width_cm: Series[int]

    class Config:
        coerce = True
