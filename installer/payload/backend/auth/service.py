"""High-level cookie acquisition orchestration."""

from __future__ import annotations

from typing import Dict, Optional

from backend.services.logger import logger

from backend.auth.browser import get_cookies
from backend.auth.kontur_check import validate_kontur_session
from backend.auth.store import (
    cookie_lock,
    cookie_refresh_event,
    is_cookie_refresh_in_progress,
    load_cookies_from_file,
    save_cookies_to_file,
    set_cookie_refresh_in_progress,
)
from backend.auth.yandex_cookies import load_cookies_from_yandex_profile


def _accept_live_cookies(cookies: Optional[Dict[str, str]], *, source: str) -> Optional[Dict[str, str]]:
    if not cookies:
        return None
    if not validate_kontur_session(cookies):
        logger.info("Источник cookies '%s' не прошел проверку Kontur API", source)
        return None
    if not save_cookies_to_file(cookies):
        logger.warning("Не удалось сохранить cookies из источника '%s'", source)
        return dict(cookies)
    logger.info("Используем живые cookies из источника '%s'", source)
    return dict(cookies)


def get_valid_cookies(force_refresh: bool = False) -> Optional[Dict[str, str]]:
    """Return Kontur cookies that are structurally valid and live on the API.

    Local file freshness alone is not enough: cookies are always checked via
    Kontur `/api/v1/user` before being reused. Selenium is opened only when
    cheaper sources fail that live check.
    """
    if not force_refresh:
        cached = load_cookies_from_file()
        accepted = _accept_live_cookies(cached, source="file")
        if accepted:
            return accepted

    became_refresher = False
    with cookie_lock():
        if is_cookie_refresh_in_progress():
            logger.info("Ожидаем завершения параллельного обновления cookies")
        else:
            set_cookie_refresh_in_progress(True)
            cookie_refresh_event().clear()
            became_refresher = True

    if not became_refresher:
        cookie_refresh_event().wait(timeout=120)
        cached = load_cookies_from_file()
        accepted = _accept_live_cookies(cached, source="file-after-wait")
        if accepted:
            return accepted
        with cookie_lock():
            if not is_cookie_refresh_in_progress():
                set_cookie_refresh_in_progress(True)
                cookie_refresh_event().clear()
                became_refresher = True

    if not became_refresher:
        cached = load_cookies_from_file(allow_stale=True)
        return _accept_live_cookies(cached, source="file-stale-fallback")

    try:
        logger.info("Получаем новые cookies")

        profile_cookies = load_cookies_from_yandex_profile()
        accepted = _accept_live_cookies(profile_cookies, source="yandex-profile")
        if accepted:
            return accepted

        selenium_cookies = get_cookies()
        if selenium_cookies:
            # get_cookies already persists structurally valid cookies; re-check live.
            accepted = _accept_live_cookies(selenium_cookies, source="selenium")
            if accepted:
                return accepted

        stale = load_cookies_from_file(allow_stale=True)
        return _accept_live_cookies(stale, source="file-stale-after-refresh")
    finally:
        with cookie_lock():
            set_cookie_refresh_in_progress(False)
            cookie_refresh_event().set()
