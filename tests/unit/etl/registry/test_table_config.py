import pandas as pd
import pandera as pa
import pytest
from pandera.typing import Series

from olist_dw.etl.registry.olist_tables import TABLES
from olist_dw.etl.registry.tables import LoadStrategy, TableConfig


class ExampleSchema(pa.SchemaModel):
    identifier: Series[str]


def identity(dataframe: pd.DataFrame) -> pd.DataFrame:
    return dataframe


def test_registry_declares_expected_loading_semantics() -> None:
    expected = {
        "customers": (
            LoadStrategy.UPSERT,
            ("customer_id",),
        ),
        "orders": (
            LoadStrategy.UPSERT,
            ("order_id",),
        ),
        "order_items": (
            LoadStrategy.UPSERT,
            ("order_id", "order_item_id"),
        ),
        "payments": (
            LoadStrategy.UPSERT,
            ("order_id", "sequential"),
        ),
        "products": (
            LoadStrategy.UPSERT,
            ("product_id",),
        ),
        "sellers": (
            LoadStrategy.UPSERT,
            ("seller_id",),
        ),
        "reviews": (
            LoadStrategy.APPEND_DEDUPLICATE,
            (),
        ),
        "geolocation": (
            LoadStrategy.SNAPSHOT_REPLACE,
            (),
        ),
        "translation": (
            LoadStrategy.SNAPSHOT_REPLACE,
            (),
        ),
    }

    actual = {
        name: (
            configuration.load_strategy,
            configuration.business_key,
        )
        for name, configuration in TABLES.items()
    }

    assert actual == expected


def test_upsert_strategy_requires_business_key() -> None:
    with pytest.raises(
        ValueError,
        match="requires a business key",
    ):
        TableConfig(
            name="example",
            raw_filename="example_raw.csv",
            processed_filename="example.csv",
            raw_schema=ExampleSchema,
            processed_schema=ExampleSchema,
            transform=identity,
            load_strategy=LoadStrategy.UPSERT,
        )


def test_business_key_must_exist_in_processed_schema() -> None:
    with pytest.raises(
        ValueError,
        match="unknown business-key columns",
    ):
        TableConfig(
            name="example",
            raw_filename="example_raw.csv",
            processed_filename="example.csv",
            raw_schema=ExampleSchema,
            processed_schema=ExampleSchema,
            transform=identity,
            load_strategy=LoadStrategy.UPSERT,
            business_key=("missing_column",),
        )


def test_snapshot_strategy_rejects_business_key() -> None:
    with pytest.raises(
        ValueError,
        match="Only UPSERT tables",
    ):
        TableConfig(
            name="example",
            raw_filename="example_raw.csv",
            processed_filename="example.csv",
            raw_schema=ExampleSchema,
            processed_schema=ExampleSchema,
            transform=identity,
            load_strategy=LoadStrategy.SNAPSHOT_REPLACE,
            business_key=("identifier",),
        )