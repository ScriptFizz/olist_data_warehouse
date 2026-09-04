import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pandera.errors import SchemaError

from olist_dw.etl.transform.raw_schemas import CustomersSchema, validate
from olist_dw.etl.transform.transform_data import (
    transform_customers,
    transform_order_items,
    transform_orders,
)


def test_transform_customers_renames_source_columns() -> None:
    source = pd.DataFrame(
        {
            "customer_id": ["customer-1"],
            "customer_unique_id": ["unique-1"],
            "customer_zip_code_prefix": ["01310"],
            "customer_city": ["sao paulo"],
            "customer_state": ["SP"],
        }
    )

    expected = pd.DataFrame(
        {
            "customer_id": ["customer-1"],
            "customer_uid": ["unique-1"],
            "zipcode": ["01310"],
            "city": ["sao paulo"],
            "state": ["SP"],
        }
    )

    result = transform_customers(source)
    assert_frame_equal(result, expected)


def test_transform_customers_does_not_mutate_source_dataframe() -> None:
    source = pd.DataFrame(
        {
            "customer_id": ["customer-1"],
            "customer_unique_id": ["unique-1"],
            "customer_zip_code_prefix": ["01310"],
            "customer_city": ["sao paulo"],
            "customer_state": ["SP"],
        }
    )
    original = source.copy(deep=True)

    transform_customers(source)

    assert_frame_equal(source, original)


def test_transform_orders_renames_and_converts_timestamps() -> None:
    source = pd.DataFrame(
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

    result = transform_orders(source)

    assert list(result.columns) == [
        "order_id",
        "customer_id",
        "order_status",
        "purchase_ts",
        "approval_ts",
        "delivery_carrier_ts",
        "delivery_customer_ts",
        "estimated_delivery_ts",
    ]

    timestamp_columns = [
        "purchase_ts",
        "approval_ts",
        "delivery_carrier_ts",
        "delivery_customer_ts",
        "estimated_delivery_ts",
    ]

    for column in timestamp_columns:
        assert pd.api.types.is_datetime64_any_dtype(result[column])


def test_transform_orders_preserves_missing_optional_timestamps() -> None:
    source = pd.DataFrame(
        {
            "order_id": ["order-1"],
            "customer_id": ["customer-1"],
            "order_status": ["processing"],
            "order_purchase_timestamp": ["2017-10-02 10:56:33"],
            "order_approved_at": [None],
            "order_delivered_carrier_date": [None],
            "order_delivered_customer_date": [None],
            "order_estimated_delivery_date": ["2017-10-18 00:00:00"],
        }
    )

    result = transform_orders(source)

    assert pd.isna(result.loc[0, "approval_ts"])
    assert pd.isna(result.loc[0, "delivery_carrier_ts"])
    assert pd.isna(result.loc[0, "delivery_customer_ts"])


def test_customer_schema_rejects_missing_required_column() -> None:
    invalid = pd.DataFrame(
        {
            "customer_id": ["customer-1"],
            "customer_unique_id": ["unique-1"],
            "customer_zip_code_prefix": ["01310"],
            "customer_city": ["sao paulo"],
            # customer_state is deliberately missing
        }
    )

    with pytest.raises(SchemaError):
        validate(invalid, CustomersSchema)


def test_transform_order_items_preserves_source_monetary_values() -> None:
    source = pd.DataFrame(
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

    result = transform_order_items(source)

    assert result.loc[0, "price"] == 100.0
    assert result.loc[0, "freight_value"] == 15.5


def test_transform_order_items_converts_shipping_limit_to_timestamp() -> None:
    source = pd.DataFrame(
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

    result = transform_order_items(source)

    assert pd.api.types.is_datetime64_any_dtype(result["shipping_limit_date"])


def test_transform_order_items_does_not_mutate_source_dataframe() -> None:
    source = pd.DataFrame(
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
    original = source.copy(deep=True)

    transform_order_items(source)

    assert_frame_equal(source, original)
