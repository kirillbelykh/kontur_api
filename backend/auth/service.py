"""High-level cookie acquisition orchestration."""

from __future__ import annotations

import time
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
from backend.auth.lan_cookies import fetch_cookies_from_lan
from backend.auth.yandex_cookies import load_cookies_from_yandex_profile

# Skip a second Selenium launch when cookies were just collected successfully.
_SELENIUM_DEBOUNCE_SECONDS = 90.0
_LAST_SELENIUM_OK_AT = 0.0
_LAST_SELENIUM_TRY_AT = 0.0


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


def get_valid_cookies(
    force_refresh: bool = False,
    *,
    force_browser: bool = False,
) -> Optional[Dict[str, str]]:
    """Return Kontur cookies that are structurally valid and live on the API.

    Selenium opens only when cheaper sources fail the live check, unless
    ``force_browser`` is set. Even then a short debounce avoids opening the
    browser twice when two refresh triggers fire back-to-back.
    """
    global _LAST_SELENIUM_OK_AT, _LAST_SELENIUM_TRY_AT

    if not force_refresh and not force_browser:
        cached = load_cookies_from_file()
        accepted = _accept_live_cookies(cached, source="file")
        if accepted:
            return accepted

    # Force-refresh without forcing the browser: re-validate file first.
    if force_refresh and not force_browser:
        cached = load_cookies_from_file(allow_stale=True)
        accepted = _accept_live_cookies(cached, source="file-revalidate")
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
        cached = load_cookies_from_file(allow_stale=True)
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

        if not force_browser:
            lan_cookies = fetch_cookies_from_lan()
            accepted = _accept_live_cookies(lan_cookies, source="lan")
            if accepted:
                return accepted

            profile_cookies = load_cookies_from_yandex_profile()
            accepted = _accept_live_cookies(profile_cookies, source="yandex-profile")
            if accepted:
                return accepted

        now = time.time()
        cached = load_cookies_from_file(allow_stale=True)
        recently_tried = (
            not force_browser
            and _LAST_SELENIUM_TRY_AT
            and (now - _LAST_SELENIUM_TRY_AT) < _SELENIUM_DEBOUNCE_SECONDS
        )
        if recently_tried and cached:
            accepted = _accept_live_cookies(cached, source="selenium-debounce")
            if accepted:
                logger.info(
                    "Пропускаем повторный Selenium — cookies свежие (%.0f сек назад)",
                    now - _LAST_SELENIUM_OK_AT,
                )
                return accepted
            logger.info(
                "Пропускаем повторный Selenium — уже пробовали %.0f сек назад",
                now - _LAST_SELENIUM_TRY_AT,
            )
            return _accept_live_cookies(cached, source="file-stale-after-refresh")
        if recently_tried and not cached:
            logger.info(
                "Debounce Selenium сброшен: файла cookies нет, запускаем браузер"
            )

        _LAST_SELENIUM_TRY_AT = now
        selenium_cookies = get_cookies()
        if selenium_cookies:
            accepted = _accept_live_cookies(selenium_cookies, source="selenium")
            if accepted:
                _LAST_SELENIUM_OK_AT = time.time()
                return accepted

        stale = load_cookies_from_file(allow_stale=True)
        return _accept_live_cookies(stale, source="file-stale-after-refresh")
    finally:
        with cookie_lock():
            set_cookie_refresh_in_progress(False)
            cookie_refresh_event().set()
