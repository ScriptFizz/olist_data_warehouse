import logging
from typing import Annotated

import pandas as pd
import pandera as pa
import typer
from google.cloud import bigquery

from olist_dw.etl.load.pandera_to_bq import pandera_schema_to_bq
from olist_dw.etl.transform.raw_schemas import validate

logger = logging.getLogger(__name__)

INGESTION_METADATA_SCHEMA = (
    bigquery.SchemaField("_batch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("_record_hash", "STRING", mode="REQUIRED"),
)


def load_dataset_to_bq(
    df: Annotated[pd.DataFrame, typer.Option(help="Pandas dataframe to load.")],
    project_id: Annotated[str, typer.Option(help="ID of the bigquery project.")],
    dataset_id: Annotated[str, typer.Option(help="ID of the bigquery dataset.")],
    table_id: Annotated[str, typer.Option(help="ID of the bigquery table to create.")],
    schema_model: Annotated[
        type[pa.SchemaModel] | None,
        typer.Option(help="Schema model the dataframe obeys."),
    ] = None,
) -> None:
    """
    Load a dataframe into a BigQuery cloud storage.

    Args:
                    df (pd.DataFrame): Pandas dataframe to store remotely.
                    project_id (str): ID of the bigquery project.
                    dataset_id (Str): ID of the bigquery dataset.
                    table_id (str): ID of the bigquery table to create.
                    pandera_schema (type(pa.SchemaModel)): Schema model the dataframe obeys.

    Returns:
                    None:
    """
    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    if schema_model:
        pa_schema = schema_model.to_schema()
        source_columns = list(pa_schema.columns)
        metadata_columns = [field.name for field in INGESTION_METADATA_SCHEMA]
        unexpected_columns = sorted(
            set(df.columns) - set(source_columns) - set(metadata_columns)
        )
        missing_metadata = sorted(set(metadata_columns) - set(df.columns))

        if unexpected_columns:
            raise ValueError(f"Unexpected BigQuery columns: {unexpected_columns}")
        if missing_metadata:
            raise ValueError(
                "Missing BigQuery ingestion metadata columns: "
                f"{missing_metadata}"
            )

        validated_source = validate(
            df=df.loc[:, source_columns],
            schema=schema_model,
        )
        df = pd.concat(
            [validated_source, df.loc[:, metadata_columns]],
            axis=1,
        )

        for column_name, column in pa_schema.columns.items():
            if isinstance(column.dtype, pa.dtypes.DateTime):
                df[column_name] = pd.to_datetime(df[column_name], utc=True)

        schema = [
            *pandera_schema_to_bq(schema_model=schema_model),
            *INGESTION_METADATA_SCHEMA,
        ]
    else:
        schema = None

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
        autodetect=schema is None,
    )

    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    logger.info(f"Loaded dataset into table {full_table_id}.")
