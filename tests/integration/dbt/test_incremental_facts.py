import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import pytest
from psycopg import sql

from olist_dw.config.postgres import PostgresSettings
from olist_dw.etl.load.ingestion_batch import IngestionBatch
from olist_dw.etl.load.postgres import load_tables_to_postgres
from olist_dw.etl.registry.olist_tables import TABLES
from olist_dw.etl.transform.dataset_contracts import validate_referential_integrity
from olist_dw.etl.transform.raw_schemas import validate

pytestmark = pytest.mark.postgres

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_DIRECTORY = PROJECT_ROOT / "dbt"
DBT_EXECUTABLE = Path(sys.executable).parent / "dbt"
FIXTURE_DIRECTORY = PROJECT_ROOT / "tests" / "fixtures" / "processed_olist"


def load_two_order_fixture() -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}

    for table_config in TABLES.values():
        dataframe = pd.read_csv(
            FIXTURE_DIRECTORY / table_config.processed_filename
        )
        tables[table_config.name] = dataframe

    tables["customers"] = pd.concat(
        [
            tables["customers"],
            tables["customers"].assign(
                customer_id="customer-2",
                customer_uid="customer-unique-2",
            ),
        ],
        ignore_index=True,
    )
    tables["orders"] = pd.concat(
        [
            tables["orders"],
            tables["orders"].assign(
                order_id="order-2",
                customer_id="customer-2",
            ),
        ],
        ignore_index=True,
    )
    tables["order_items"] = pd.concat(
        [
            tables["order_items"],
            tables["order_items"].assign(order_id="order-2"),
        ],
        ignore_index=True,
    )
    tables["payments"] = pd.concat(
        [
            tables["payments"],
            tables["payments"].assign(order_id="order-2"),
        ],
        ignore_index=True,
    )
    tables["reviews"] = pd.concat(
        [
            tables["reviews"],
            tables["reviews"].assign(
                review_id="review-2",
                order_id="order-2",
            ),
        ],
        ignore_index=True,
    )

    validated = {
        name: validate(dataframe, TABLES[name].processed_schema)
        for name, dataframe in tables.items()
    }
    validate_referential_integrity(validated)
    return validated


def publish_raw_batch(
    settings: PostgresSettings,
    tables: Mapping[str, pd.DataFrame],
) -> None:
    batch = IngestionBatch.create(
        source_name="dbt-incremental-integration-test",
        tables=tables,
    )
    load_tables_to_postgres(
        tables=tables,
        table_configs=TABLES,
        settings=settings,
        batch=batch,
    )


def run_dbt(
    settings: PostgresSettings,
    dbt_schema: str,
    *,
    full_refresh: bool,
) -> None:
    environment = os.environ.copy()
    environment.update(
        {
            "POSTGRES_HOST": settings.host,
            "POSTGRES_PORT": str(settings.port),
            "POSTGRES_DB": settings.database,
            "POSTGRES_USER": settings.user,
            "POSTGRES_PASSWORD": settings.password,
            "DBT_RAW_SCHEMA": settings.schema,
            "DBT_POSTGRES_SCHEMA": dbt_schema,
        }
    )

    command = [
        str(DBT_EXECUTABLE),
        "build",
        "--project-dir",
        str(DBT_DIRECTORY),
        "--profiles-dir",
        str(DBT_DIRECTORY),
        "--target",
        "postgres_dev",
        "--exclude-resource-type",
        "unit_test",
    ]
    if full_refresh:
        command.append("--full-refresh")

    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=120,
    )

    if result.returncode != 0:
        raise AssertionError(
            "dbt build failed:\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )


def fetch_fact_rows(
    settings: PostgresSettings,
    mart_schema: str,
    table_name: str,
) -> dict[str, tuple[Any, ...]]:
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            sql.SQL(
                "SELECT order_id, order_status, warehouse_updated_at "
                "FROM {}.{} ORDER BY order_id"
            ).format(
                sql.Identifier(mart_schema),
                sql.Identifier(table_name),
            )
        )
        return {str(row[0]): row for row in cursor.fetchall()}


def table_oid(
    settings: PostgresSettings,
    schema_name: str,
    table_name: str,
) -> int:
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT to_regclass(%s)::oid",
            (f"{schema_name}.{table_name}",),
        )
        row = cursor.fetchone()

    if row is None or row[0] is None:
        raise AssertionError(f"Table does not exist: {schema_name}.{table_name}")

    return int(row[0])


def drop_dbt_schemas(
    settings: PostgresSettings,
    schema_prefix: str,
) -> None:
    with (
        psycopg.connect(**settings.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            "SELECT nspname FROM pg_namespace WHERE starts_with(nspname, %s)",
            (schema_prefix,),
        )
        schema_names = [str(row[0]) for row in cursor.fetchall()]

        for schema_name in schema_names:
            cursor.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )


def test_late_order_update_changes_only_affected_incremental_facts(
    postgres_settings: PostgresSettings,
) -> None:
    suffix = postgres_settings.schema.removeprefix("test_raw_")
    dbt_schema = f"test_dbt_{suffix}"
    mart_schema = f"{dbt_schema}_marts"

    try:
        initial_tables = load_two_order_fixture()
        publish_raw_batch(postgres_settings, initial_tables)
        run_dbt(
            postgres_settings,
            dbt_schema,
            full_refresh=True,
        )

        original_orders = fetch_fact_rows(
            postgres_settings,
            mart_schema,
            "fct_orders",
        )
        original_items = fetch_fact_rows(
            postgres_settings,
            mart_schema,
            "fct_order_items",
        )
        original_oids = {
            table_name: table_oid(
                postgres_settings,
                mart_schema,
                table_name,
            )
            for table_name in ("fct_orders", "fct_order_items")
        }

        corrected_tables = {
            name: dataframe.copy(deep=True)
            for name, dataframe in initial_tables.items()
        }
        corrected_tables["orders"].loc[
            corrected_tables["orders"]["order_id"] == "order-1",
            "order_status",
        ] = "shipped"

        publish_raw_batch(postgres_settings, corrected_tables)
        run_dbt(
            postgres_settings,
            dbt_schema,
            full_refresh=False,
        )

        updated_orders = fetch_fact_rows(
            postgres_settings,
            mart_schema,
            "fct_orders",
        )
        updated_items = fetch_fact_rows(
            postgres_settings,
            mart_schema,
            "fct_order_items",
        )

        assert updated_orders["order-1"][1] == "shipped"
        assert updated_orders["order-1"][2] > original_orders["order-1"][2]
        assert updated_items["order-1"][2] > original_items["order-1"][2]

        assert updated_orders["order-2"] == original_orders["order-2"]
        assert updated_items["order-2"] == original_items["order-2"]

        assert {
            table_name: table_oid(
                postgres_settings,
                mart_schema,
                table_name,
            )
            for table_name in ("fct_orders", "fct_order_items")
        } == original_oids
    finally:
        drop_dbt_schemas(postgres_settings, dbt_schema)
