import json
from pathlib import Path

import yaml


def load_params(path: str = "config/settings.yaml"):
    """
    Read a configuration yaml file and return its data.

    Args:
            path (str): filepath of the configuration file.

    Returns:
            Dict (Python object that best fits the data): configuration data in a nested structure
    """
    with open(path) as f:
        return yaml.safe_load(f)


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
