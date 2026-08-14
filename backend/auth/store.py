"""Cookie file storage, memoization and structural validation."""

from __future__ import annotations

import json
import threading
import time
from typing import Dict, List, Optional

from backend.services.logger import logger

from backend.auth.constants import (
    COOKIE_TTL,
    COOKIES_FILE,
    LEGACY_COOKIES_FILE,
    OPTIONAL_COOKIE_FIELDS,
    REQUIRED_COOKIE_FIELDS,
)

_COOKIE_LOCK = threading.RLock()
_COOKIE_REFRESH_EVENT = threading.Event()
_COOKIE_REFRESH_IN_PROGRESS = False
_MEMOIZED_COOKIES: Optional[Dict[str, str]] = None
_MEMOIZED_TIMESTAMP = 0.0


def cookie_lock() -> threading.RLock:
    return _COOKIE_LOCK


def cookie_refresh_event() -> threading.Event:
    return _COOKIE_REFRESH_EVENT


def is_cookie_refresh_in_progress() -> bool:
    return _COOKIE_REFRESH_IN_PROGRESS


def set_cookie_refresh_in_progress(value: bool) -> None:
    global _COOKIE_REFRESH_IN_PROGRESS
    _COOKIE_REFRESH_IN_PROGRESS = bool(value)


def cookies_age(timestamp: float) -> float:
    return max(0.0, time.time() - float(timestamp or 0))


def cookies_are_fresh(timestamp: float) -> bool:
    return bool(timestamp) and cookies_age(timestamp) <= COOKIE_TTL


def remember_cookies(cookies: Dict[str, str], timestamp: Optional[float] = None) -> None:
    global _MEMOIZED_COOKIES, _MEMOIZED_TIMESTAMP
    with _COOKIE_LOCK:
        _MEMOIZED_COOKIES = dict(cookies)
        _MEMOIZED_TIMESTAMP = float(timestamp or time.time())


def clear_memoized_cookies() -> None:
    global _MEMOIZED_COOKIES, _MEMOIZED_TIMESTAMP
    with _COOKIE_LOCK:
        _MEMOIZED_COOKIES = None
        _MEMOIZED_TIMESTAMP = 0.0


def validate_cookies(cookies: Dict[str, str]) -> tuple[bool, List[str]]:
    if not cookies:
        return False, ["all cookies missing"]

    missing_required = [field for field in REQUIRED_COOKIE_FIELDS if field not in cookies]
    if missing_required:
        return False, missing_required

    empty_required = [field for field in REQUIRED_COOKIE_FIELDS if not cookies.get(field)]
    if empty_required:
        logger.warning("Обязательные поля cookies пустые: %s", empty_required)
        return False, empty_required

    missing_optional = [field for field in OPTIONAL_COOKIE_FIELDS if field not in cookies]
    if missing_optional:
        logger.debug("Отсутствуют необязательные поля cookies: %s", missing_optional)

    return True, []


def read_fresh_cookie_bundle() -> Optional[Dict[str, object]]:
    """Local cookies + timestamp if they are still within TTL. No Kontur call."""
    cookies = load_cookies_from_file(allow_stale=False)
    if not cookies:
        return None
    with _COOKIE_LOCK:
        timestamp = float(_MEMOIZED_TIMESTAMP or 0.0)
    return {"timestamp": timestamp, "cookies": cookies}


def load_cookies_from_file(allow_stale: bool = False) -> Optional[Dict[str, str]]:
    with _COOKIE_LOCK:
        if _MEMOIZED_COOKIES and cookies_are_fresh(_MEMOIZED_TIMESTAMP):
            return dict(_MEMOIZED_COOKIES)

    cookies_file = COOKIES_FILE if COOKIES_FILE.exists() else LEGACY_COOKIES_FILE
    if not cookies_file.exists():
        logger.info("Файл cookies не существует")
        return None

    try:
        data = json.loads(cookies_file.read_text(encoding="utf-8"))
        cookies = data.get("cookies")
        timestamp = float(data.get("timestamp", 0) or 0)

        if not cookies_are_fresh(timestamp) and not allow_stale:
            logger.info("Cookies устарели (%.0f сек). Нужно обновить.", cookies_age(timestamp))
            return None

        is_valid, missing_fields = validate_cookies(cookies if isinstance(cookies, dict) else {})
        if not is_valid:
            logger.info("Cookies в файле невалидны. Отсутствуют поля: %s", missing_fields)
            return None

        remember_cookies(cookies, timestamp)
        logger.info("Cookies успешно загружены из файла и прошли проверку")
        return dict(cookies)
    except json.JSONDecodeError as exc:
        logger.error("Ошибка декодирования JSON в файле cookies: %s", exc)
        return None
    except Exception:
        logger.exception("Ошибка при чтении cookies из файла")
        return None


def save_cookies_to_file(cookies: Dict[str, str]) -> bool:
    try:
        is_valid, missing_fields = validate_cookies(cookies)
        if not is_valid:
            logger.error("Нельзя сохранить невалидные cookies. Отсутствуют поля: %s", missing_fields)
            return False

        timestamp = time.time()
        payload = {
            "timestamp": timestamp,
            "cookies": cookies,
        }
        COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
        COOKIES_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        remember_cookies(cookies, timestamp)
        logger.info("Cookies сохранены в %s", COOKIES_FILE)
        return True
    except Exception:
        logger.exception("Ошибка при сохранении cookies")
        return False
