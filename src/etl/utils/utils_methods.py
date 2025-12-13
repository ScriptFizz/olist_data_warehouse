import json
import logging
from pathlib import Path

import pandas as pd

from config.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


def load_params(env: str | None = None):
    """
    Read a configuration yaml file and return its data.

    Args:
            path (str): filepath of the configuration file.

    Returns:
            Dict (Python object that best fits the data): configuration data in a nested structure
    """
    return ConfigLoader(env=env).as_dict()


def load_dict(path: Path) -> dict:
    """
    Load a JSON file into a Dict.

    Args:
            path (str): filepath of the JSON file to load.

    Returns:
            (Dict): dictionary the JSON file is loaded into.
    """
    with open(path) as fp:
        d = json.load(fp)
    return d


def load_csv(name: str, _dir: str) -> pd.DataFrame:
    """
    Load data from csv file and return a Pandas dataframe.

    Args:
                    name (str): name of the file to load data from.
                    raw_dir (str): name of the directory the data is stored.

    Returns:
                    pd.DataFrame: pandas dataframe with the file data.
    """

    path = Path(_dir) / name
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        logger.error(f"Missing file: {path}")
        raise
    except pd.errors.ParserError:
        logger.error(f"CSV parsing error in file: {path}")
        raise
    return df
