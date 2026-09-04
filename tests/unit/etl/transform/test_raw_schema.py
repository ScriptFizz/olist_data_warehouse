import pandas as pd
import pytest
from pandera.errors import SchemaError

from olist_dw.etl.transform.raw_schemas import (
    CustomersSchema,
    OrderItemsSchema,
    OrdersSchema,
    ReviewsSchema,
    validate,
)


def valid_order() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": ["order-1"],
            "customer_id": ["customer-1"],
            "order_status": ["delivered"],
            "order_purchase_timestamp": ["2017-10-02 10:56:33"],
            "order_approved_at": ["2017-10-02 11:07:15"],
            "order_delivered_carrier_date": ["2017-10-04 19:55:00"],
            "order_delivered_customer_date": ["2017-10-10 21:25:13"],
            "order_estimated_delivery_date": ["2017-10-18 00:00:00"],
        }
    )


def test_orders_schema_accepts_valid_order() -> None:
    result = validate(valid_order(), OrdersSchema)

    assert len(result) == 1


def test_orders_schema_rejects_unknown_status() -> None:
    invalid = valid_order()
    invalid.loc[0, "order_status"] = "partially_delivered"

    with pytest.raises(SchemaError):
        validate(invalid, OrdersSchema)


def test_orders_schema_rejects_duplicate_order_id() -> None:
    invalid = pd.concat([valid_order(), valid_order()], ignore_index=True)

    with pytest.raises(SchemaError):
        validate(invalid, OrdersSchema)


def test_customers_schema_rejects_invalid_state_code() -> None:
    invalid = pd.DataFrame(
        {
            "customer_id": ["customer-1"],
            "customer_unique_id": ["unique-1"],
            "customer_zip_code_prefix": ["01310"],
            "customer_city": ["sao paulo"],
            "customer_state": ["XX"],
        }
    )

    with pytest.raises(SchemaError):
        validate(invalid, CustomersSchema)


def test_raw_schema_rejects_unexpected_column() -> None:
    invalid = valid_order()
    invalid["unexpected_column"] = "unexpected"

    with pytest.raises(SchemaError):
        validate(invalid, OrdersSchema)


def test_order_items_schema_rejects_negative_price() -> None:
    invalid = pd.DataFrame(
        {
            "order_id": ["order-1"],
            "order_item_id": [1],
            "product_id": ["product-1"],
            "seller_id": ["seller-1"],
            "shipping_limit_date": ["2017-10-08 10:00:00"],
            "price": [-1.0],
            "freight_value": [15.5],
        }
    )

    with pytest.raises(SchemaError):
        validate(invalid, OrderItemsSchema)


def test_order_items_schema_rejects_duplicate_composite_key() -> None:
    row = pd.DataFrame(
        {
            "order_id": ["order-1"],
            "order_item_id": [1],
            "product_id": ["product-1"],
            "seller_id": ["seller-1"],
            "shipping_limit_date": ["2017-10-08 10:00:00"],
            "price": [100.0],
            "freight_value": [15.5],
        }
    )
    invalid = pd.concat([row, row], ignore_index=True)

    with pytest.raises(SchemaError):
        validate(invalid, OrderItemsSchema)


@pytest.mark.parametrize("score", [0, 6])
def test_reviews_schema_rejects_score_outside_valid_range(score: int) -> None:
    invalid = pd.DataFrame(
        {
            "review_id": ["review-1"],
            "order_id": ["order-1"],
            "review_score": [score],
            "review_comment_title": [None],
            "review_comment_message": [None],
            "review_creation_date": ["2017-10-11 00:00:00"],
            "review_answer_timestamp": ["2017-10-12 00:00:00"],
        }
    )

    with pytest.raises(SchemaError):
        validate(invalid, ReviewsSchema)
