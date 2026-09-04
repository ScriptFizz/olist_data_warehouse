import logging
import logging.config
import os
import sys
from pathlib import Path


def resolve_logs_directory(log_dir: str | Path | None = None) -> Path:
    """Resolve logs independently of the package installation directory."""
    configured_directory = log_dir or os.getenv("OLIST_LOG_DIR")
    if configured_directory is not None:
        return Path(configured_directory).expanduser()

    return Path.cwd() / "logs"


def setup_logging(
    log_level: int = logging.INFO,
    log_dir: str | Path | None = None,
) -> None:
    """
    Define logging configurations for applications.
    """

    logs_dir = resolve_logs_directory(log_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "minimal": {"format": "%(message)s"},
            "detailed": {
                "format": (
                    "%(levelname)s %(asctime)s "
                    "[%(name)s:%(filename)s:%(funcName)s:%(lineno)d]\n"
                    "%(message)s\n"
                )
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "minimal",
                "level": log_level,
            },
            "info": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": logs_dir / "info.log",
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "detailed",
                "level": logging.INFO,
            },
            "error": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": logs_dir / "error.log",
                "maxBytes": 10_000_000,
                "backupCount": 5,
                "formatter": "detailed",
                "level": logging.ERROR,
            },
        },
        "root": {
            "handlers": ["console", "info", "error"],
            "level": log_level,
            "propagate": True,
        },
    }

    logging.config.dictConfig(logging_config)
