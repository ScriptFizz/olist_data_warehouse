from collections.abc import Mapping
from dataclasses import dataclass

import pandas as pd


class DatasetContractError(ValueError):
    """Raised when relationships across processed datasets are invalid."""


@dataclass(frozen=True)
class Relationship:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str


RELATIONSHIPS = (
    Relationship("orders", "customer_id", "customers", "customer_id"),
    Relationship("order_items", "order_id", "orders", "order_id"),
    Relationship("order_items", "product_id", "products", "product_id"),
    Relationship("order_items", "seller_id", "sellers", "seller_id"),
    Relationship("payments", "order_id", "orders", "order_id"),
    Relationship("reviews", "order_id", "orders", "order_id"),
)


def validate_referential_integrity(
    tables: Mapping[str, pd.DataFrame],
) -> None:
    """
    Validate foreign-key-like relationships across processed datasets.

    Null child keys are ignored here and should be controlled by the table's
    Pandera schema. All violations are collected before the exception is raised.
    """

    required_tables = {relationship.child_table for relationship in RELATIONSHIPS} | {
        relationship.parent_table for relationship in RELATIONSHIPS
    }

    missing_tables = sorted(required_tables.difference(tables))

    if missing_tables:
        names = ", ".join(missing_tables)
        raise DatasetContractError(f"Missing required tables: {names}")

    violations: list[str] = []

    for relationship in RELATIONSHIPS:
        child = tables[relationship.child_table]
        parent = tables[relationship.parent_table]

        missing_columns = [
            f"{relationship.child_table}.{relationship.child_column}"
            if relationship.child_column not in child.columns
            else None,
            f"{relationship.parent_table}.{relationship.parent_column}"
            if relationship.parent_column not in parent.columns
            else None,
        ]

        missing_columns = [name for name in missing_columns if name is not None]

        if missing_columns:
            violations.append(
                "Missing relationship columns: " + ", ".join(missing_columns)
            )
            continue
        child_keys = child[relationship.child_column]
        parent_keys = parent[relationship.parent_column]

        orphan_mask = child_keys.notna() & ~child_keys.isin(parent_keys)

        if orphan_mask.any():
            orphan_values = (
                child.loc[orphan_mask, relationship.child_column]
                .drop_duplicates()
                .astype(str)
                .head(5)
                .tolist()
            )
            violations.append(
                f"{relationship.child_table}.{relationship.child_column} "
                f"contains {int(orphan_mask.sum())} orphan row(s); "
                f"sample values: {orphan_values}; expected matches in "
                f"{relationship.parent_table}.{relationship.parent_column}"
            )

    if violations:
        details = "\n- ".join(violations)
        raise DatasetContractError(
            f"Referential-integrity validation failed:\n- {details}"
        )
