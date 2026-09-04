from dataclasses import dataclass

import pandera as pa
from pandera import SchemaModel


@dataclass(frozen=True)
class PostgresColumn:
    name: str
    data_type: str
    nullable: bool


def pandera_schema_to_postgres(
    schema_model: type[SchemaModel],
) -> tuple[PostgresColumn, ...]:
    """Convert a Pandera schema into PostgreSQL column definitions."""
    columns: list[PostgresColumn] = []

    for name, column in schema_model.to_schema().columns.items():
        dtype = column.dtype

        if isinstance(dtype, pa.dtypes.String):
            postgres_type = "TEXT"
        elif isinstance(dtype, pa.dtypes.Int):
            postgres_type = "BIGINT"
        elif isinstance(dtype, pa.dtypes.Float):
            postgres_type = "DOUBLE PRECISION"
        elif isinstance(dtype, pa.dtypes.Bool):
            postgres_type = "BOOLEAN"
        elif isinstance(dtype, pa.dtypes.DateTime):
            postgres_type = "TIMESTAMP WITHOUT TIME ZONE"
        else:
            raise ValueError(f"Unsupported Pandera type for column {name}: {dtype}")

        columns.append(
            PostgresColumn(
                name=name,
                data_type=postgres_type,
                nullable=column.nullable,
            )
        )

    return tuple(columns)
