import json
from pathlib import Path
from typing import Annotated

import requests
import typer
from utils.utils_methods import load_params

from config.logconfig import logger

app = typer.Typer(help="ETL: extract API data.")


@app.command()
def extract_exchange_rate(
    url: Annotated[str | None, typer.Option(help="url to extract data from")] = None,
    out_dir: Annotated[
        str | None, typer.Option(help="location to store the extracted data")
    ] = None,
) -> None:
    """
    Extract exchange rates data.

    Args:
            url: url to extract data from.
            out_dir: location to store the extracted data.

    Returns:
            None:
    """
    params = load_params()

    url = url or params["api"]["exchange_rate_url"]
    out_dir = out_dir or params["path"]["raw_data_dir"]

    logger.info(f"Calling exchange rate API: {url}.")
    response = requests.get(url)
    data = response.json()

    out_filepath = Path(out_dir) / "exchange_rate.json"
    with open(out_filepath, "w") as f:
        json.dump(data, f)

    logger.info("Saved API data.")


if __name__ == "__main__":
    extract_exchange_rate()
