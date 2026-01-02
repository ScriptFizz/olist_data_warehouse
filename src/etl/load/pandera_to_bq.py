import pandera as pa
from google.cloud import bigquery
from pandera import SchemaModel

PANDERA_TO_BQ_TYPES = {
    pa.String: "STRING",
    pa.Int: "INT64",
    pa.Float: "FLOAT64",
    pa.Bool: "BOOL",
    pa.DateTime: "TIMESTAMP",
}


def pandera_schema_to_bq(schema_model: type[SchemaModel]) -> list[bigquery.SchemaField]:
    """
    Convert a pandera SchemaModel in a list of BigQuery SchemaField to use when loading DataFrames.

    Args:
        schema_model (SchemaModel): Pandera SchemaModel to convert.

    Returns:
        list[bigquery.SchemaField]: the list of bigquery.SchemaField correspondent to the SchemaModel pa.Field.
    """

    fields: list[bigquery.SchemaField] = []
    pa_schema = schema_model.to_schema()

    for column_name, column in pa_schema.columns.items():
        pandera_type = column.dtype

        if isinstance(pandera_type, pa.dtypes.String):
            bq_type = "STRING"
        elif isinstance(pandera_type, pa.dtypes.Int):
            bq_type = "INT64"
        elif isinstance(pandera_type, pa.dtypes.Float):
            bq_type = "FLOAT64"
        elif isinstance(pandera_type, pa.dtypes.Bool):
            bq_type = "BOOL"
        elif isinstance(pandera_type, pa.dtypes.DateTime):
            bq_type = "TIMESTAMP"
        else:
            raise ValueError(f"Unsupported Pandera type: {pandera_type}")

        mode = "NULLABLE" if column.nullable else "REQUIRED"

        fields.append(
            bigquery.SchemaField(
                name=column_name,
                field_type=bq_type,
                mode=mode,
            )
        )

    return fields
