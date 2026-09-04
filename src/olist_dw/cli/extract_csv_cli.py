import logging
import os
from typing import Annotated

import typer
from dotenv import load_dotenv

from olist_dw.config.logconfig import setup_logging
from olist_dw.etl.extract.extract_csv import ingest_data
from olist_dw.etl.utils.utils_methods import load_params

setup_logging()
logger = logging.getLogger(__name__)

app = typer.Typer(
    help="ETL: extract dataset from Kaggle",
    pretty_exceptions_show_locals=False,
)


@app.command()
def run(
    out_dir: Annotated[
        str | None, typer.Option(help="Directory where the data will be stored")
    ] = None,
    dataset_name: Annotated[
        str | None, typer.Option(help="Dataset identifier to download from Kaggle")
    ] = None,
) -> None:
    """
    Ingest Kaggle dataset (Require Kaggle credentials setup).

    Args:
            out_dir (Path): location where the dataset is saved.
            dataset_name (str): identifier of the dataset to download (the username/dataset-name portion of https://www.kaggle.com/datasets/username/dataset-name)

    Returns:
            None:
    """

    params = load_params()

    load_dotenv()
    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_key = os.getenv("KAGGLE_KEY")

    if not kaggle_username or not kaggle_key:
        raise ValueError("KAGGLE_USERNAME or KAGGLE_KEY missing in .env file")

    out_dir = out_dir or params["paths"]["raw_data_dir"]
    dataset_name = dataset_name or params["data"]["dataset_name"]

    typer.echo(f"Extracting data files from {dataset_name} to {out_dir}")
    ingest_data(out_dir=out_dir, dataset_name=dataset_name)


if __name__ == "__main__":
    run()
