import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IngestionBatch:
    batch_id: UUID
    source_name: str
    source_fingerprint: str
    started_at: datetime
    table_count: int
    input_row_count: int

    @classmethod
    def create(
        cls,
        *,
        source_name: str,
        tables: Mapping[str, pd.DataFrame],
    ) -> "IngestionBatch":
        if not source_name.strip():
            raise ValueError("source_name must not be empty")

        if not tables:
            raise ValueError("At least one table is required")

        return cls(
            batch_id=uuid4(),
            source_name=source_name,
            source_fingerprint=compute_batch_fingerprint(tables),
            started_at=datetime.now(UTC),
            table_count=len(tables),
            input_row_count=sum(len(table) for table in tables.values()),
        )


def compute_record_hashes(
    dataframe: pd.DataFrame,
    *,
    columns: Sequence[str] | None = None,
) -> pd.Series:
    """
    Return deterministic SHA-256 hashes for dataframe records.

    Metadata columns beginning with an underscore are excluded by default.
    """
    selected_columns = (
        list(columns)
        if columns is not None
        else [
            column
            for column in dataframe.columns
            if not column.startswith("_")
        ]
    )

    missing_columns = sorted(
        set(selected_columns) - set(dataframe.columns)
    )
    if missing_columns:
        raise ValueError(
            f"Hash columns not found in dataframe: {missing_columns}"
        )

    hashes: list[str] = []

    for row in dataframe[selected_columns].itertuples(
        index=False,
        name=None,
    ):
        payload = {
            column: _normalize_hash_value(value)
            for column, value in zip(
                selected_columns,
                row,
                strict=True,
            )
        }

        serialized = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        hashes.append(
            hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        )

    return pd.Series(
        hashes,
        index=dataframe.index,
        dtype="string",
        name="_record_hash",
    )


def compute_batch_fingerprint(
    tables: Mapping[str, pd.DataFrame],
) -> str:
    """
    Hash table names and record hashes into an order-independent batch identity.
    """
    if not tables:
        raise ValueError("At least one table is required")

    digest = hashlib.sha256()

    for table_name in sorted(tables):
        digest.update(f"table:{table_name}\n".encode())

        record_hashes = sorted(
            compute_record_hashes(tables[table_name]).tolist()
        )
        for record_hash in record_hashes:
            digest.update(f"row:{record_hash}\n".encode())

    return digest.hexdigest()


def _normalize_hash_value(value: object) -> object:
    if value is None or value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, (float, np.floating)):
        numeric_value = float(value)

        if math.isnan(numeric_value):
            return None

        if not math.isfinite(numeric_value):
            raise ValueError(
                "Infinite numeric values cannot be record-hashed"
            )

        return numeric_value

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, np.generic):
        return value.item()

    if isinstance(value, str | int | bool):
        return value

    raise TypeError(
        f"Unsupported value type for record hashing: {type(value).__name__}"
    )