import logging
import logging.config
from pathlib import Path
import sys


PROJ_ROOT = PAth(__file__).resolve().parents[1]
LOGS_DIR = PROJ_ROOT / "logs"
LOGS_DIR.mkdir(parents = True, exist_ok = True)

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
            "level": logging.INFO,
        },
        "info": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOGS_DIR / "info.log",
            "maxBytes": 10_000_000,
            "backupCount": 5,
            "formatter": "detailed",
            "level": logging.INFO,
        },
        "error": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": LOGS_DIR / "error.log",
            "maxBytes": 10_000_000,
            "backupCount": 5,
            "formatter": "detailed",
            "level": logging.ERROR,
        },
    },
    "root": {
        "handlers": ["console", "info", "error"],
        "level": logging.INFO,
        "propagate": True,
    },
}

logging.config.dictConfig(logging_config)
logger = logging.getLogger()
