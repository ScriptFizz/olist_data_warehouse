from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
from pandera import SchemaModel


@dataclass(frozen=True)
class TransformContext:
    exchange_rate: dict


@dataclass(frozen=True)
class TableConfig:
    name: str

    raw_filename: str
    processed_filename: str

    raw_schema: type[SchemaModel]
    processed_schema: type[SchemaModel]

    transform: Callable[..., pd.DataFrame]
    kwargs_factory: Callable[[TransformContext], dict] | None = None
