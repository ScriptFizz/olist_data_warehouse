import pandas as pd
import pytest

from olist_dw.etl.transform.dataset_contracts import (
    DatasetContractError,
    validate_referential_integrity,
)


def valid_tables() -> dict[str, pd.DataFrame]:
    return {
        "customers": pd.DataFrame(
            {
                "customer_id": ["customer-1"],
            }
        ),
        "orders": pd.DataFrame(
            {
                "order_id": ["order-1"],
                "customer_id": ["customer-1"],
            }
        ),
        "order_items": pd.DataFrame(
            {
                "order_id": ["order-1"],
                "product_id": ["product-1"],
                "seller_id": ["seller-1"],
            }
        ),
        "products": pd.DataFrame(
            {
                "product_id": ["product-1"],
            }
        ),
        "sellers": pd.DataFrame(
            {
                "seller_id": ["seller-1"],
            }
        ),
        "payments": pd.DataFrame(
            {
                "order_id": ["order-1"],
            }
        ),
        "reviews": pd.DataFrame(
            {
                "order_id": ["order-1"],
            }
        ),
    }


def test_referential_integrity_accepts_valid_relationships() -> None:
    validate_referential_integrity(valid_tables())


def test_referential_integrity_rejects_orphan_customer() -> None:
    tables = valid_tables()
    tables["orders"].loc[0, "customer_id"] = "missing-customer"

    with pytest.raises(
        DatasetContractError,
        match=r"orders\.customer_id contains 1 orphan row",
    ):
        validate_referential_integrity(tables)


def test_referential_integrity_rejects_orphan_order_item_keys() -> None:
    tables = valid_tables()
    tables["order_items"].loc[0, "order_id"] = "missing-order"
    tables["order_items"].loc[0, "product_id"] = "missing-product"

    with pytest.raises(DatasetContractError) as exc_info:
        validate_referential_integrity(tables)

    message = str(exc_info.value)

    assert "order_items.order_id contains 1 orphan row" in message
    assert "order_items.product_id contains 1 orphan row" in message


def test_referential_integrity_rejects_missing_table() -> None:
    tables = valid_tables()
    del tables["products"]

    with pytest.raises(
        DatasetContractError,
        match="Missing required tables: products",
    ):
        validate_referential_integrity(tables)
