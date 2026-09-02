import os
import re
from dataclasses import dataclass, field
from typing import TypedDict

from dotenv import load_dotenv


class PostgresConfigurationError(ValueError):
    """Raised when PostgreSQL environment configuration is invalid."""


class PostgresConnectionKwargs(TypedDict):
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    schema: str
    audit_schema: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls) -> "PostgresSettings":
        load_dotenv()

        host = _required_environment_variable("POSTGRES_HOST")
        database = _required_environment_variable("POSTGRES_DB")
        user = _required_environment_variable("POSTGRES_USER")
        password = _required_environment_variable("POSTGRES_PASSWORD")
        schema = _required_environment_variable("POSTGRES_SCHEMA")
        audit_schema = _required_environment_variable("POSTGRES_AUDIT_SCHEMA")
        port_text = _required_environment_variable("POSTGRES_PORT")

        try:
            port = int(port_text)
        except ValueError as exc:
            raise PostgresConfigurationError(
                "POSTGRES_PORT must be an integer"
            ) from exc

        if not 1 <= port <= 65535:
            raise PostgresConfigurationError(
                "POSTGRES_PORT must be between 1 and 65535"
            )
        
        for variable_name, schema_name in {
            "POSTGRES_SCHEMA": schema,
            "POSTGRES_AUDIT_SCHEMA": audit_schema,
        }.items():
            if not re.fullmatch(
                r"[A-Za-z_][A-Za-z0-9_]*",
                schema_name,
            ):
                raise PostgresConfigurationError(
                    f"{variable_name} must be a valid unquoted "
                    "PostgreSQL identifier"
                )

        return cls(
            host=host,
            port=port,
            database=database,
            user=user,
            audit_schema=audit_schema,
            password=password,
            schema=schema,
        )

    def connection_kwargs(self) -> PostgresConnectionKwargs:
        """Return arguments suitable for psycopg.connect."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }


def _required_environment_variable(name: str) -> str:
    value = os.getenv(name)

    if value is None or not value.strip():
        raise PostgresConfigurationError(
            f"Required environment variable {name} is missing"
        )

    return value
