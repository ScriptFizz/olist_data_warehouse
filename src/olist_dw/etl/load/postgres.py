import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast
from uuid import uuid4

import numpy as np
import pandas as pd
import psycopg
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.ingestion_batch import (
    IngestionBatch,
    compute_record_hashes,
)
from olist_dw.etl.load.pandera_to_postgres import (
    PostgresColumn,
    pandera_schema_to_postgres,
)
from olist_dw.etl.registry.tables import LoadStrategy, TableConfig

logger = logging.getLogger(__name__)


METADATA_COLUMNS = (
    PostgresColumn("_batch_id", "UUID", False),
    PostgresColumn(
        "_ingested_at",
        "TIMESTAMP WITH TIME ZONE",
        False,
    ),
    PostgresColumn("_record_hash", "TEXT", False),
)


def _attach_ingestion_metadata(
    dataframe: pd.DataFrame,
    batch: IngestionBatch,
) -> pd.DataFrame:
    result = dataframe.copy()

    result["_batch_id"] = str(batch.batch_id)
    result["_ingested_at"] = batch.started_at
    result["_record_hash"] = compute_record_hashes(dataframe)

    return result


@dataclass(frozen=True)
class PostgresConnectionInfo:
    database: str
    user: str
    server_version: str


@dataclass(frozen=True)
class PostgresLoadResult:
    schema: str
    input_row_counts: dict[str, int]
    affected_row_counts: dict[str, int]


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


def load_tables_to_postgres(
    *,
    tables: Mapping[str, pd.DataFrame],
    table_configs: Mapping[str, TableConfig],
    settings: PostgresSettings,
    batch: IngestionBatch,
) -> PostgresLoadResult:
    """
    Publish a complete ingestion batch to PostgreSQL in one transaction.

    Every dataframe is copied to a uniquely named staging table before any
    target table is changed. Each target is then published according to its
    registry strategy. Any failure rolls back the entire batch.
    """
    _validate_loader_inputs(
        tables=tables,
        table_configs=table_configs,
    )
    load_id = uuid4().hex[:12]
    staging_tables = {
        table_name: f"__load_{table_name}_{load_id}" for table_name in tables
    }
    table_columns: dict[str, tuple[PostgresColumn, ...]] = {}
    affected_row_counts: dict[str, int] = {}

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
            table_config = table_configs[table_name]
            source_columns = pandera_schema_to_postgres(
                table_config.processed_schema
            )
            columns = source_columns + METADATA_COLUMNS
            table_columns[table_name] = columns

            _validate_dataframe_columns(
                table_name=table_name,
                dataframe=dataframe,
                columns=source_columns,
            )

            _create_target_table(
                cursor=cursor,
                schema_name=settings.schema,
                table_name=table_name,
                columns=columns,
            )
            _ensure_conflict_index(
                cursor=cursor,
                schema_name=settings.schema,
                table_config=table_config,
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
                dataframe=_attach_ingestion_metadata(dataframe, batch),
                columns=columns,
            )

        # Publication starts only after every staging COPY has succeeded.
        for table_name in tables:
            affected_row_counts[table_name] = _publish_staging_table(
                cursor=cursor,
                schema_name=settings.schema,
                table_config=table_configs[table_name],
                staging_table=staging_tables[table_name],
                columns=table_columns[table_name],
            )

            cursor.execute(
                sql.SQL("DROP TABLE {}.{}").format(
                    sql.Identifier(settings.schema),
                    sql.Identifier(staging_tables[table_name]),
                )
            )

    input_row_counts = {
        table_name: len(dataframe) for table_name, dataframe in tables.items()
    }

    logger.info(
        "Published PostgreSQL batch batch_id=%s schema=%s "
        "input_row_counts=%s affected_row_counts=%s",
        batch.batch_id,
        settings.schema,
        input_row_counts,
        affected_row_counts,
    )

    return PostgresLoadResult(
        schema=settings.schema,
        input_row_counts=input_row_counts,
        affected_row_counts=affected_row_counts,
    )


def _validate_loader_inputs(
    *,
    tables: Mapping[str, pd.DataFrame],
    table_configs: Mapping[str, TableConfig],
) -> None:
    table_names = set(tables)
    config_names = set(table_configs)

    missing_configs = sorted(table_names - config_names)
    unexpected_configs = sorted(config_names - table_names)

    if missing_configs or unexpected_configs:
        raise ValueError(
            "Table/config mapping mismatch: "
            f"missing_configs={missing_configs}, "
            f"unexpected_configs={unexpected_configs}"
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
        sql.SQL("CREATE TABLE {}.{} (LIKE {}.{})").format(
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


def _ensure_conflict_index(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    table_config: TableConfig,
) -> None:
    if table_config.load_strategy is LoadStrategy.UPSERT:
        conflict_columns = table_config.business_key
    elif (
        table_config.load_strategy
        is LoadStrategy.APPEND_DEDUPLICATE
    ):
        conflict_columns = ("_record_hash",)
    else:
        return

    index_name = f"uq_{table_config.name}_load_key"
    identifiers = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in conflict_columns
    )

    cursor.execute(
        sql.SQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} "
            "ON {}.{} ({})"
        ).format(
            sql.Identifier(index_name),
            sql.Identifier(schema_name),
            sql.Identifier(table_config.name),
            identifiers,
        )
    )


def _publish_staging_table(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    table_config: TableConfig,
    staging_table: str,
    columns: tuple[PostgresColumn, ...],
) -> int:
    if table_config.load_strategy is LoadStrategy.SNAPSHOT_REPLACE:
        return _publish_snapshot_replace(
            cursor=cursor,
            schema_name=schema_name,
            table_name=table_config.name,
            staging_table=staging_table,
            columns=columns,
        )

    if table_config.load_strategy is LoadStrategy.UPSERT:
        return _publish_upsert(
            cursor=cursor,
            schema_name=schema_name,
            table_name=table_config.name,
            staging_table=staging_table,
            columns=columns,
            business_key=table_config.business_key,
        )

    if (
        table_config.load_strategy
        is LoadStrategy.APPEND_DEDUPLICATE
    ):
        return _publish_append_deduplicate(
            cursor=cursor,
            schema_name=schema_name,
            table_name=table_config.name,
            staging_table=staging_table,
            columns=columns,
        )

    raise ValueError(
        f"Unsupported load strategy: "
        f"{table_config.load_strategy}"
    )


def _publish_snapshot_replace(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    table_name: str,
    staging_table: str,
    columns: tuple[PostgresColumn, ...],
) -> int:
    column_names = _column_identifiers(columns)

    cursor.execute(
        sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
    )

    cursor.execute(
        sql.SQL(
            "INSERT INTO {}.{} ({}) "
            "SELECT {} FROM {}.{}"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            column_names,
            column_names,
            sql.Identifier(schema_name),
            sql.Identifier(staging_table),
        )
    )

    return cursor.rowcount


def _publish_upsert(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    table_name: str,
    staging_table: str,
    columns: tuple[PostgresColumn, ...],
    business_key: tuple[str, ...],
) -> int:
    column_names = _column_identifiers(columns)

    conflict_columns = sql.SQL(", ").join(
        sql.Identifier(column)
        for column in business_key
    )

    update_columns = [
        column.name
        for column in columns
        if column.name not in business_key
    ]

    assignments = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(
            sql.Identifier(column),
            sql.Identifier(column),
        )
        for column in update_columns
    )

    cursor.execute(
        sql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {} FROM {}.{}
            ON CONFLICT ({})
            DO UPDATE SET {}
            WHERE {}."_record_hash"
                IS DISTINCT FROM EXCLUDED."_record_hash"
            """
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            column_names,
            column_names,
            sql.Identifier(schema_name),
            sql.Identifier(staging_table),
            conflict_columns,
            assignments,
            sql.Identifier(table_name),
        )
    )

    return cursor.rowcount


def _publish_append_deduplicate(
    *,
    cursor: psycopg.Cursor[Any],
    schema_name: str,
    table_name: str,
    staging_table: str,
    columns: tuple[PostgresColumn, ...],
) -> int:
    column_names = _column_identifiers(columns)

    cursor.execute(
        sql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {} FROM {}.{}
            ON CONFLICT ("_record_hash") DO NOTHING
            """
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            column_names,
            column_names,
            sql.Identifier(schema_name),
            sql.Identifier(staging_table),
        )
    )

    return cursor.rowcount


def _column_identifiers(
    columns: tuple[PostgresColumn, ...],
) -> sql.Composed:
    return sql.SQL(", ").join(
        sql.Identifier(column.name)
        for column in columns
    )
