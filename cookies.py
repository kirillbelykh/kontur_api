"""Backward-compatible facade for the historical ``cookies`` module.

Implementation lives in the ``auth`` package. Prefer:

    from auth import get_valid_cookies
"""

from __future__ import annotations

import threading as threading  # noqa: F401  # historical test surface

from auth import *  # noqa: F403
from auth.browser import (  # noqa: F401
    _build_browser_options,
    _click_cookie_accept_if_present,
    _hide_driver_windows,
    _remove_webdriver_marker,
    _wait_for_valid_cookies,
)
from auth.constants import (  # noqa: F401
    DEFAULT_PROLONGATION_INTERVAL_HOURS,
    PROLONGATION_BUTTON_XPATH,
    PROLONGATION_ENABLED_ENV,
    PROLONGATION_IDLE_CHECK_SECONDS,
    PROLONGATION_INTERVAL_HOURS_ENV,
    PROLONGATION_RETRY_DELAY_SECONDS,
    PROLONGATION_SIGN_BUTTON_XPATH,
    PROLONGATION_STARTUP_DELAY_SECONDS,
    PROLONGATION_URL,
    PROLONGATION_WAIT_TIMEOUT,
    SLEEP,
)
from auth.prolongation import (  # noqa: F401
    _PROLONGATION_LOCK,
    _kontur_access_prolongation_worker,
    _load_prolongation_state,
    _prolongation_enabled,
    _prolongation_interval_seconds,
    _prolongation_is_due,
    _run_kontur_access_prolongation_browser_flow,
    _save_prolongation_state,
    _seconds_until_next_prolongation,
    _timestamp_to_iso8601,
)
from auth.store import (  # noqa: F401
    cookies_age as _cookies_age,
    cookies_are_fresh as _cookies_are_fresh,
    remember_cookies as _remember_cookies,
)

# Mutable worker handle kept on this module for historical tests/callers.
import auth.prolongation as _prolongation_mod

_PROLONGATION_THREAD = _prolongation_mod._PROLONGATION_THREAD


def __getattr__(name: str):
    if name == "_PROLONGATION_THREAD":
        return _prolongation_mod._PROLONGATION_THREAD
    raise AttributeError(f"module 'cookies' has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | {"_PROLONGATION_THREAD"})
