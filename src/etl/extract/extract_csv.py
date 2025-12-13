import json
import logging
import subprocess
from pathlib import Path
from typing import Annotated

import typer

logger = logging.getLogger(__name__)


def configure_kaggle_cli(
    username: Annotated[str, typer.Option(help="Kaggle username")],
    key: Annotated[str, typer.Option(help="Kaggle API key of the user")],
) -> None:
    """
    Automatically configure Kaggle CLI

    Args:
            username (str): Kaggle username
            key (str): Kaggle API key.

    Returns:
            None:
    """

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(exist_ok=True)

    kaggle_config_file = kaggle_dir / "kaggle.json"

    if kaggle_config_file.exists():
        kaggle_config_file.unlink()

    kaggle_config_file.write_text(json.dumps({"username": username, "key": key}))

    # required permissions
    kaggle_config_file.chmod(0o600)


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
