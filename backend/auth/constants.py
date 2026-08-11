"""Auth-layer constants and runtime paths."""

from __future__ import annotations

import os
from pathlib import Path

from backend.auth.paths import find_yandex_paths

_paths = find_yandex_paths()

YANDEX_DRIVER_PATH = Path(os.getenv("KONTUR_YANDEX_DRIVER", r"driver\yandexdriver.exe"))
YANDEX_BROWSER_PATH = _paths.get("browser")
PROFILE_USER_DATA_DIR = _paths.get("user_data")
# Known-good profile that held the Kontur session (d647455). Env overrides.
_DEFAULT_YANDEX_PROFILE = "Vinsent O`neal"
PROFILE_DIRECTORY = str(
    os.getenv("KONTUR_YANDEX_PROFILE")
    or _paths.get("profile_directory")
    or _DEFAULT_YANDEX_PROFILE
)
# True Chrome --headless=new is unsupported for this auth stack by default.
HEADLESS = False

RUNTIME_DIR = Path(os.getenv("KONTUR_RUNTIME_DIR", "runtime"))
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
AUTH_RUNTIME_DIR = RUNTIME_DIR / "auth"
AUTH_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

COOKIES_FILE = AUTH_RUNTIME_DIR / "kontur_cookies.json"
LEGACY_COOKIES_FILE = Path("kontur_cookies.json")

ORGANIZATION_ID = os.getenv(
    "ORGANIZATION_ID",
    "5cda50fa-523f-4bb5-85b6-66d7241b23cd",
).strip()
BASE_URL = os.getenv("BASE_URL", "https://mk.kontur.ru").rstrip("/")

TARGET_URL = f"{BASE_URL}/organizations/{ORGANIZATION_ID}/warehouses"
USER_INFO_URL = f"{BASE_URL}/api/v1/user"
PROLONGATION_URL = (
    f"{BASE_URL}/organizations/{ORGANIZATION_ID}/settings"
    "#organization_settings_anchor_prolongation_token"
)

WAIT_TIMEOUT = 20
SLEEP = 1.0
COOKIE_TTL = 13 * 60

PROLONGATION_BUTTON_XPATH = (
    "/html/body/div[1]/div/div/div[2]/div/div/div[1]/div[3]/div[1]/div[2]"
    "/div[6]/span/button/div[2]/span[2]"
)
PROLONGATION_SIGN_BUTTON_XPATH = (
    "/html/body/div[5]/div/div[2]/div/div/div/div/div[2]/div[3]/div/div/div"
    "/div[2]/div/div/span[1]/span/button/div[2]/span[2]"
)
PROLONGATION_STATE_FILE = AUTH_RUNTIME_DIR / "kontur_access_prolongation.json"
LEGACY_PROLONGATION_STATE_FILE = Path("kontur_access_prolongation.json")
PROLONGATION_WAIT_TIMEOUT = 30
DEFAULT_PROLONGATION_INTERVAL_HOURS = 9.0
PROLONGATION_ENABLED_ENV = "KONTUR_ACCESS_PROLONGATION_ENABLED"
PROLONGATION_INTERVAL_HOURS_ENV = "KONTUR_ACCESS_PROLONGATION_INTERVAL_HOURS"
PROLONGATION_RETRY_DELAY_SECONDS = 5 * 60
PROLONGATION_IDLE_CHECK_SECONDS = 15 * 60
PROLONGATION_STARTUP_DELAY_SECONDS = 2 * 60

REQUIRED_COOKIE_FIELDS = [
    "auth.sid",
    "token",
    "portaluserid",
    "auth.check",
    "ngtoken",
    "device",
]

OPTIONAL_COOKIE_FIELDS = [
    "gdpr-consent",
    "_kmts",
    "_mfp",
    "_kfpxv5",
]
