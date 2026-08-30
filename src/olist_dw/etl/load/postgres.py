import logging
from dataclasses import dataclass
from typing import Any, cast

import psycopg

from olist_dw.config.postgres import PostgresSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PostgresConnectionInfo:
    database: str
    user: str
    server_versione: str


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
