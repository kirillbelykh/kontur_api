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


class SafeRotatingFileHandler(RotatingFileHandler):
    def doRollover(self):
        try:
            super().doRollover()
        except OSError:
            # На Windows старое и новое приложение могут держать lookup.log одновременно.
            # Пропускаем ротацию, чтобы логирование не ломало основной сценарий.
            pass


if not logger.handlers:
    file_handler = SafeRotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
        delay=True,
    )
    file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)
