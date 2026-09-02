from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

import pandas as pd
from pandera import SchemaModel


class LoadStrategy(StrEnum):
    """Supported PostgreSQL raw-table publication strategies."""

    UPSERT = "upsert"
    APPEND_DEDUPLICATE = "append_deduplicate"
    SNAPSHOT_REPLACE = "snapshot_replace"


@dataclass(frozen=True)
class TableConfig:
    name: str

    raw_filename: str
    processed_filename: str

    raw_schema: type[SchemaModel]
    processed_schema: type[SchemaModel]

    transform: Callable[[pd.DataFrame], pd.DataFrame]

    load_strategy: LoadStrategy
    business_key: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.load_strategy is LoadStrategy.UPSERT and not self.business_key:
            raise ValueError(
                f"UPSERT table {self.name!r} requires a business key"
            )

        if self.load_strategy is not LoadStrategy.UPSERT and self.business_key:
            raise ValueError(
                f"Only UPSERT tables may declare a business key: {self.name!r}"
            )
        
        processed_columns = set(
            self.processed_schema.to_schema().columns
        )

        unknown_key_columns = sorted(
            set(self.business_key) - processed_columns
        )

        if unknown_key_columns:
            raise ValueError(
                f"Table {self.name!r} has unknown business-key columns: "
                f"{unknown_key_columns}"
            )
