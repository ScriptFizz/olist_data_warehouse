import pandera as pa
from pandera.typing import Series

from olist_dw.etl.load.pandera_to_postgres import (
    PostgresColumn,
    pandera_schema_to_postgres,
)


class ExampleSchema(pa.SchemaModel):
    identifier: Series[str]
    quantity: Series[int]
    amount: Series[float]
    active: Series[bool]
    occurred_at: Series[pa.DateTime] = pa.Field(nullable=True)


def test_pandera_schema_converts_to_postgres_columns() -> None:
    result = pandera_schema_to_postgres(ExampleSchema)

    assert result == (
        PostgresColumn("identifier", "TEXT", False),
        PostgresColumn("quantity", "BIGINT", False),
        PostgresColumn("amount", "DOUBLE PRECISION", False),
        PostgresColumn("active", "BOOLEAN", False),
        PostgresColumn(
            "occurred_at",
            "TIMESTAMP WITHOUT TIME ZONE",
            True,
        ),
    )
