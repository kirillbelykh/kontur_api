"""Shared application logger with Windows-safe UTF-8 output."""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler


LOG_FILE = os.getenv("LOG_FILE", "lookup.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3
LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(message)s"


logger = logging.getLogger("GTIN_Lookup")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.propagate = False


def _configure_standard_streams() -> None:
    """Prefer UTF-8 console output; fall back silently for frozen GUI builds."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if not callable(reconfigure):
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


class SafeRotatingFileHandler(RotatingFileHandler):
    """Rotating file handler that keeps logging alive if rollover is locked."""

    def doRollover(self) -> None:
        try:
            super().doRollover()
        except OSError:
            # On Windows an old and a new app instance may hold lookup.log at once.
            # Skipping rollover is safer than breaking the active workflow.
            pass


_configure_standard_streams()


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
