import logging
import os
from logging.handlers import RotatingFileHandler


LOG_FILE = os.getenv("LOG_FILE", "lookup.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"


logger = logging.getLogger("GTIN_Lookup")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.propagate = False


if not logger.handlers:
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
        delay=True,
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)
