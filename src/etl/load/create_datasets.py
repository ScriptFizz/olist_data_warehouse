import logging
from pathlib import Path
from typing import Annotated

import typer
from google.cloud import bigquery

from config.logconfig import setup_logging
from etl.utils.utils_methods import load_params

setup_logging()
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def create_datasets(
    project_id: Annotated[
        str | None, typer.Option(help="ID of the bigquery project.")
    ] = None,
    datasets: Annotated[
        dict[str, str] | None, typer.Option(help="ID of the dataset to use for views.")
    ] = None,
    sql_dir: Annotated[
        str | None, typer.Option(help="path pointing to the directory with sql files.")
    ] = None,
) -> None:
    """
    Create views in the analytic database, defining the business logic.

    Args:
    project_id (str): ID of the bigquery project.
    datasets (dict[str, str]): Name of the datasets to use for creating and storing views.
    sql_dir (str): path pointing to the directory with sql files.
    """

    params = load_params()
    project_id = project_id or params["bigquery"]["project_id"]
    datasets = datasets or params["bigquery"]["datasets"]
    sql_dir = Path(sql_dir or params["paths"]["sql_dir"])
    #sql_dir = Path(sql_dir or params["bigquery"]["sql_dir"])

    replacements = {
        "{{ PROJECT_ID }}": project_id,
        "{{ RAW_DATASET_ID }}": datasets["raw"],
        "{{ CORE_DATASET_ID }}": datasets["core"],
        "{{ ANALYTICS_DATASET_ID }}": datasets["analytics"],
        "{{ BI_DATASET_ID }}": datasets["bi"],
    }

    client = bigquery.Client(project=project_id)

    for layer in ["analytics", "bi"]:#["core", "analytics", "bi"]:
        typer.echo(f"Storing data to {layer} dataset...")
        layer_path = sql_dir / layer

        if not layer_path.exists():
            raise FileNotFoundError(f"Missing SQL layer directory: {layer_path}")
        
        has_subdirectories = any(subdir.is_dir() for subdir in layer_path.iterdir())
        
        if has_subdirectories:
            sql_files = layer_path.glob("**/*.sql")
        else:
            sql_files = layer_path.glob("*.sql")
        
        for sql_file in sorted(sql_files):
            print("SQL NAME: ", sql_file.name)
            with open(sql_file) as f:
                query = f.read()

                for key, value in replacements.items():
                    query = query.replace(key, value)

                job = client.query(query)
                job.result()

                # print(f"Created view: {sql_file.name}")
                logger.info(f"Loaded {sql_file.name} successfully.")
        logger.info(f"{layer} dataset created successfully")

if __name__ == "__main__":
    create_datasets()




    # params = load_params()
    # project_id = project_id or params["bigquery"]["project_id"]
    # datasets = datasets or params["bigquery"]["datasets"]
    # sql_dir = Path(sql_dir or params["bigquery"]["sql_dir"])

    # replacements = {
        # "{{ PROJECT_ID }}": project_id,
        # "{{ RAW_DATASET_ID }}": datasets["raw"],
        # "{{ CORE_DATASET_ID }}": datasets["core"],
        # "{{ ANALYTICS_DATASET_ID }}": datasets["analytics"],
        # "{{ BI_DATASET_ID }}": datasets["bi"],
    # }

    # client = bigquery.Client(project=project_id)

    # for layer in ["core", "analytics", "bi"]:
        # typer.echo(f"Storing data to {layer} dataset...")
        # layer_path = sql_dir / layer

        # if not layer_path.exists():
            # raise FileNotFoundError(f"Missing SQL layer directory: {layer_path}")

        # for sql_file in sorted(layer_path.glob("*.sql")):
            # print("SQL NAME: ", sql_file.name)
            # with open(sql_file) as f:
                # query = f.read()

                # for key, value in replacements.items():
                    # query = query.replace(key, value)

                # # print("QUERY: ", query)
                # job = client.query(query)
                # job.result()

                # # print(f"Created view: {sql_file.name}")
                # logger.info(f"Loaded {sql_file.name} successfully.")
        # logger.info(f"{layer} dataset created successfully")
