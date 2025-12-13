import json
import logging
from pathlib import Path
from typing import Annotated

import typer

from config.logconfig import setup_logging
from etl.extract.extract_exchange_rates import extract_exchange_json
from etl.utils.utils_methods import load_params

setup_logging()
logger = logging.getLogger(__name__)

app = typer.Typer(help="ETL: extract API data.")


@app.command()
def run(
    url: Annotated[str | None, typer.Option(help="url to extract data from")] = None,
    out_dir: Annotated[
        str | None, typer.Option(help="location to store the extracted data")
    ] = None,
) -> None:
    """
    Extract exchange rates data and store it in the specified location.

    Args:
            url: url to extract data from.
            out_dir: location to store the extracted data.

    Returns:
            None:
    """
    params = load_params()

    url = url or params["api"]["exchange_rate_url"]
    out_dir = out_dir or params["paths"]["raw_data_dir"]

    data = extract_exchange_json(url=url)

    out_filepath = Path(out_dir) / "exchange_rate.json"
    with open(out_filepath, "w") as f:
        json.dump(data, f)

    logger.info("Saved API data.")


if __name__ == "__main__":
    run()
