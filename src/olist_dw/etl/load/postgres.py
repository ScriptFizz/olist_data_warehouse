import logging

#################################
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

import numpy as np
import pandas as pd
import pandera as pa
import psycopg
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.pandera_to_postgres import (
    PostgresColumn,
    pandera_schema_to_postgres,
)

##################################

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PostgresConnectionInfo:
    database: str
    user: str
    server_version: str


#################################
@dataclass(frozen=True)
class PostgresLoadResult:
    schema: str
    row_counts: dict[str, int]


################################


def check_postgres_connection(
    settings: PostgresSettings,
) -> PostgresConnectionInfo:
    """Open a database connection and return non-sensitive server information."""
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            """
            SELECT
                current_database(),
                current_user,
                current_setting('server_version')
            """
        )
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError("PostgreSQL connection check returned no result")

    database, user, server_version = cast(tuple[Any, Any, Any], row)

    connection_info = PostgresConnectionInfo(
        database=str(database),
        user=str(user),
        server_version=str(server_version),
    )

    logger.info(
        "Connected to PostgreSQL database=%s user=%s version=%s",
        connection_info.database,
        connection_info.user,
        connection_info.server_version,
    )

    return connection_info


###############################################


def load_tables_to_postgres(
    *,
    tables: Mapping[str, pd.DataFrame],
    schemas: Mapping[str, type[pa.SchemaModel]],
    settings: PostgresSettings,
) -> PostgresLoadResult:
    """
    Replace a complete set of PostgreSQL raw tables in one transaction.

    Data is copied into uniquely named staging tables first. Target tables are
    truncated and repopulated only after every staging load succeeds.
    """
    _validate_loader_inputs(tables=tables, schemas=schemas)
    load_id = uuid4().hex[:12]
    staging_tables = {
        table_name: f"__load_{table_name}_{load_id}" for table_name in tables
    }

    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"{settings.database}.{settings.schema}.raw_load",),
        )

        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(settings.schema)
            )
        )

        for table_name, dataframe in tables.items():
            columns = pandera_schema_to_postgres(schemas[table_name])
            _validate_dataframe_columns(
                table_name=table_name,
                dataframe=dataframe,
                columns=columns,
            )

            _create_target_table(
                cursor=cursor,
                schema_name=settings.schema,
                table_name=table_name,
                columns=columns,
            )

            _create_staging_table(
                cursor=cursor,
                schema_name=settings.schema,
                target_table=table_name,
                staging_table=staging_tables[table_name],
            )

            _copy_dataframe(
                cursor=cursor,
                schema_name=settings.schema,
                table_name=staging_tables[table_name],
                dataframe=dataframe,
                columns=columns,
            )

        # Publication starts only after every staging COPY has succeeded.
        for table_name in tables:
            columns = pandera_schema_to_postgres(schemas[table_name])
            column_names = sql.SQL(", ").join(
                sql.Identifier(column.name) for column in columns
            )

            cursor.execute(
                sql.SQL("TRUNCATE TABLE {}.{}").format(
                    sql.Identifier(settings.schema),
                    sql.Identifier(table_name),
                )
            )
            cursor.execute(
                sql.SQL("INSERT INTO {}.{} ({}) SELECT {} FROM {}.{}").format(
                    sql.Identifier(settings.schema),
                    sql.Identifier(table_name),
                    column_names,
                    column_names,
                    sql.Identifier(settings.schema),
                    sql.Identifier(staging_tables[table_name]),
                )
            )
            cursor.execute(
                sql.SQL("DROP TABLE {}.{}").format(
                    sql.Identifier(settings.schema),
                    sql.Identifier(staging_tables[table_name]),
                )
            )

    row_counts = {
        table_name: len(dataframe) for table_name, dataframe in tables.items()
    }

    logger.info(
        "Published PostgreSQL batch schema=%s row_counts=%s",
        settings.schema,
        row_counts,
    )

    return PostgresLoadResult(
        schema=settings.schema,
        row_counts=row_counts,
    )


def _validate_loader_inputs(
    *,
    tables: Mapping[str, pd.DataFrame],
    schemas: Mapping[str, type[pa.SchemaModel]],
) -> None:
    table_names = set(tables)
    schema_names = set(schemas)

    missing_schemas = sorted(table_names - schema_names)
    unexpected_schemas = sorted(schema_names - table_names)

    if missing_schemas or unexpected_schemas:
        raise ValueError(
            "Table/schema mapping mismatch: "
            f"missing_schemas={missing_schemas}, "
            f"unexpected_schemas={unexpected_schemas}"
        )

    if not tables:
        raise ValueError("At least one table is required for PostgreSQL loading")


def _validate_dataframe_columns(
    *,
    table_name: str,
    dataframe: pd.DataFrame,
    columns: tuple[PostgresColumn, ...],
) -> None:
    expected = [column.name for column in columns]
    actual = list(dataframe.columns)

    if actual != expected:
        raise ValueError(
            f"Column mismatch for table {table_name}: "
            f"expected={expected}, actual={actual}"
        )


def _create_target_table(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    table_name: str,
    columns: tuple[PostgresColumn, ...],
) -> None:
    definitions = sql.SQL(", ").join(
        sql.SQL("{} {}{}").format(
            sql.Identifier(column.name),
            sql.SQL(column.data_type),
            sql.SQL("") if column.nullable else sql.SQL(" NOT NULL"),
        )
        for column in columns
    )

    cursor.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            definitions,
        )
    )


def _create_staging_table(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    target_table: str,
    staging_table: str,
) -> None:
    cursor.execute(
        sql.SQL("CREATE TABLE {}.{} (LIKE {}.{} INCLUDING ALL)").format(
            sql.Identifier(schema_name),
            sql.Identifier(staging_table),
            sql.Identifier(schema_name),
            sql.Identifier(target_table),
        )
    )


def _copy_dataframe(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    table_name: str,
    dataframe: pd.DataFrame,
    columns: tuple[PostgresColumn, ...],
) -> None:
    column_names = sql.SQL(", ").join(sql.Identifier(column.name) for column in columns)

    copy_statement = sql.SQL(" COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        column_names,
    )

    with cursor.copy(copy_statement) as copy:
        for row in dataframe.itertuples(index=False, name=None):
            copy.write_row(tuple(_postgres_value(value) for value in row))


def _postgres_value(value: object) -> object:
    """Convert pandas and NumPy scalar values into Psycopg-compatible values."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, np.generic):
        return value.item()

    return value
