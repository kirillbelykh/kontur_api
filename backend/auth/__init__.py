"""Production auth package for Kontur session cookies.

Public API stays compatible with the historical ``cookies`` module.
"""

from __future__ import annotations

from backend.auth.browser import (
    build_browser_options,
    get_cookies,
    hide_driver_windows,
    is_driver_version_mismatch,
    remove_webdriver_marker,
    wait_for_valid_cookies,
)
from backend.auth.constants import (
    AUTH_RUNTIME_DIR,
    COOKIE_TTL,
    COOKIES_FILE,
    HEADLESS,
    LEGACY_COOKIES_FILE,
    LEGACY_PROLONGATION_STATE_FILE,
    OPTIONAL_COOKIE_FIELDS,
    PROFILE_DIRECTORY,
    PROFILE_USER_DATA_DIR,
    PROLONGATION_STATE_FILE,
    REQUIRED_COOKIE_FIELDS,
    RUNTIME_DIR,
    SELENIUM_PROFILE_DIRECTORY,
    SELENIUM_USER_DATA_DIR,
    TARGET_URL,
    WAIT_TIMEOUT,
    YANDEX_BROWSER_PATH,
    YANDEX_DRIVER_PATH,
)
from backend.auth.kontur_check import validate_kontur_session
from backend.auth.paths import find_yandex_paths
from backend.auth.prolongation import (
    ensure_kontur_access_prolongation_worker_started,
    get_kontur_access_prolongation_state,
    prolong_kontur_access,
    run_kontur_access_prolongation_service,
)
from backend.auth.service import get_valid_cookies
from backend.auth.store import (
    load_cookies_from_file,
    save_cookies_to_file,
    validate_cookies,
)
from backend.auth.yandex_cookies import load_cookies_from_yandex_profile

__all__ = [
    "AUTH_RUNTIME_DIR",
    "COOKIE_TTL",
    "COOKIES_FILE",
    "HEADLESS",
    "LEGACY_COOKIES_FILE",
    "LEGACY_PROLONGATION_STATE_FILE",
    "OPTIONAL_COOKIE_FIELDS",
    "PROFILE_DIRECTORY",
    "PROFILE_USER_DATA_DIR",
    "PROLONGATION_STATE_FILE",
    "REQUIRED_COOKIE_FIELDS",
    "RUNTIME_DIR",
    "SELENIUM_PROFILE_DIRECTORY",
    "SELENIUM_USER_DATA_DIR",
    "TARGET_URL",
    "WAIT_TIMEOUT",
    "YANDEX_BROWSER_PATH",
    "YANDEX_DRIVER_PATH",
    "build_browser_options",
    "ensure_kontur_access_prolongation_worker_started",
    "find_yandex_paths",
    "get_cookies",
    "get_kontur_access_prolongation_state",
    "get_valid_cookies",
    "hide_driver_windows",
    "is_driver_version_mismatch",
    "load_cookies_from_file",
    "load_cookies_from_yandex_profile",
    "prolong_kontur_access",
    "remove_webdriver_marker",
    "run_kontur_access_prolongation_service",
    "save_cookies_to_file",
    "validate_cookies",
    "validate_kontur_session",
    "wait_for_valid_cookies",
]
