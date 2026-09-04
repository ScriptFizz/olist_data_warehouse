import logging
import subprocess
from pathlib import Path
from typing import Annotated

import typer

logger = logging.getLogger(__name__)


def ingest_data(
    out_dir: Annotated[
        str, typer.Option(help="Directory where the data will be stored")
    ],
    dataset_name: Annotated[
        str, typer.Option(help="Dataset identifier to download from Kaggle")
    ],
) -> None:
    """
    Ingest Kaggle dataset (Require Kaggle credentials setup).

    Args:
            out_dir (Path): location where the dataset is saved.
            dataset_name (str): identifier of the dataset to download (the username/dataset-name portion of https://www.kaggle.com/datasets/username/dataset-name)

    Returns:
            None:
    """

    logger.info(f"Starting data ingestion into {out_dir}.")
    # Ensure the output director exists.
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    cmd = [
        "kaggle",
        "datasets",
        "download",
        "-d",
        dataset_name,
        "-p",
        out_dir,
        "--unzip",
    ]

    subprocess.run(cmd, check=True)
    logger.info("Data ingestion completed.")
