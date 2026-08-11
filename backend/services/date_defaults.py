from __future__ import annotations

from datetime import date, datetime

# Owner-fixed defaults (запрошено 11.08.2026): формы всегда предзаполняются этими датами.
DEFAULT_PRODUCTION_DATE = "01-03-2026"
DEFAULT_EXPIRATION_DATE = "01-03-2031"


def get_default_production_window(reference: date | datetime | None = None) -> tuple[str, str]:
    """Return the default production and expiration dates for the operational forms."""
    del reference  # kept for call-site compatibility with the old rolling-window rule
    return (DEFAULT_PRODUCTION_DATE, DEFAULT_EXPIRATION_DATE)
