import logging
from typing import Annotated, TypedDict

import requests
import typer

logger = logging.getLogger(__name__)


class ExchangeRateResponse(TypedDict):
    result: str
    provider: str
    documentation: str
    terms_of_use: str
    time_last_update_unix: int
    time_last_update_utc: str
    time_next_update_unix: int
    time_next_update_utc: str
    time_eol_unix: int
    base_code: str
    rates: dict[str, float]


def extract_exchange_json(
    url: Annotated[str, typer.Option(help="url to extract data from")],
) -> ExchangeRateResponse:
    """
    Extract exchange rates data.

    Args:
            url: url to extract data from.
            out_dir: location to store the extracted data.

    Returns:
            (ExchangeRateResponse): exchange rates data in JSON format.
    """

    logger.info(f"Calling exchange rate API: {url}...")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("HTTP request failed.")
        raise RuntimeError("Failed to fetch exchange rates") from e

    try:
        data = response.json()
    except ValueError as e:
        logger.error("Invalid JSON response")
        raise RuntimeError("Response is not valid JSON") from e

    return data
