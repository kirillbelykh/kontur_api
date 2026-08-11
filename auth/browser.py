"""Selenium / YandexDriver cookie collection.

Default mode matches the historical background auth flow:
off-screen window + Win32 hide, using the real Yandex profile.
True Chrome ``--headless=new`` is available via ``HEADLESS`` / env.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

from logger import logger

from auth.constants import (
    HEADLESS,
    PROFILE_DIRECTORY,
    PROFILE_USER_DATA_DIR,
    SLEEP,
    TARGET_URL,
    WAIT_TIMEOUT,
    YANDEX_BROWSER_PATH,
    YANDEX_DRIVER_PATH,
)
from auth.store import save_cookies_to_file, validate_cookies


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _effective_headless(explicit: Optional[bool] = None) -> bool:
    if explicit is not None:
        return bool(explicit)
    if os.getenv("KONTUR_BROWSER_HEADLESS") is not None:
        return _env_flag("KONTUR_BROWSER_HEADLESS", False)
    return bool(HEADLESS)


def _click_cookie_accept_if_present(driver, by) -> None:
    try:
        cookie_btn = driver.find_elements(
            by.XPATH,
            '//*[@id="root"]/div/div/div[1]/div[1]/span/button/div[2]/span',
        )
        if cookie_btn:
            cookie_btn[0].click()
            time.sleep(SLEEP)
    except Exception as exc:
        logger.debug("Cookie accept button not available: %s", exc)


def build_browser_options(
    *,
    browser_path: Path,
    profile_user_data_dir: Optional[Path],
    profile_directory: Optional[str],
    headless: bool,
) -> Any:
    """Build Chrome/Yandex options for background cookie collection."""
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.binary_location = str(browser_path)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    normalized_user_data_dir = Path(profile_user_data_dir) if profile_user_data_dir else None
    normalized_profile_directory = str(profile_directory or "").strip()
    if normalized_user_data_dir:
        options.add_argument(f"--user-data-dir={normalized_user_data_dir}")
    if normalized_profile_directory:
        options.add_argument(f"--profile-directory={normalized_profile_directory}")

    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
    else:
        # Historical "works in background" mode: keep a real profile session,
        # but keep the window off-screen and hidden via Win32.
        options.add_argument("--disable-gpu")
        options.add_argument("--window-position=-32000,-32000")
        options.add_argument("--window-size=1920,1080")

    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--remote-debugging-port=0")
    return options


def wait_for_valid_cookies(driver, *, timeout_seconds: float) -> Optional[Dict[str, str]]:
    deadline = time.time() + max(1.0, float(timeout_seconds))
    while time.time() < deadline:
        try:
            raw_cookies = driver.get_cookies()
        except Exception as exc:
            logger.debug("Не удалось прочитать cookies из браузера: %s", exc)
            raw_cookies = []
        cookies = {
            str(item.get("name") or ""): str(item.get("value") or "")
            for item in raw_cookies
            if item.get("name")
        }
        is_valid, missing_fields = validate_cookies(cookies)
        if is_valid:
            return cookies
        logger.debug("Ожидаем появления валидных cookies, пока отсутствуют: %s", missing_fields)
        time.sleep(1.0)
    return None


def remove_webdriver_marker(driver) -> None:
    """Avoid exposing Selenium's navigator.webdriver flag to the login page."""
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});",
            },
        )
    except Exception as exc:
        logger.debug("Could not configure browser anti-detection: %s", exc)


def hide_driver_windows(driver) -> None:
    try:
        import win32con
        import win32gui
        import win32process
    except ImportError as exc:
        logger.debug("pywin32 недоступен для скрытия окна браузера: %s", exc)
        return

    try:
        pid = driver.service.process.pid
        time.sleep(1.0)

        def enum_window_callback(hwnd, results):
            _, window_pid = win32process.GetWindowThreadProcessId(hwnd)
            if window_pid == pid:
                results.append(hwnd)

        handles: list[int] = []
        win32gui.EnumWindows(enum_window_callback, handles)
        for hwnd in handles:
            win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
    except Exception as exc:
        logger.debug("Не удалось скрыть окно браузера Selenium: %s", exc)


def _try_select_profile_and_warehouse(driver, wait, by, expected_conditions) -> None:
    try:
        profile_xpath = (
            '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div'
        )
        wait.until(expected_conditions.element_to_be_clickable((by.XPATH, profile_xpath))).click()
        time.sleep(SLEEP)
    except Exception as exc:
        logger.debug("Profile select error: %s", exc)

    try:
        wait.until(
            expected_conditions.element_to_be_clickable(
                (by.XPATH, '//*[@id="root"]/div/div/div[2]/div/div/div[1]/div[3]/ul/li/div[2]')
            )
        ).click()
        time.sleep(SLEEP)
    except Exception as exc:
        logger.debug("Warehouse select error: %s", exc)


def get_cookies(
    driver_path: Path = YANDEX_DRIVER_PATH,
    browser_path: Optional[Path] = YANDEX_BROWSER_PATH,
    profile_user_data_dir: Optional[Path] = PROFILE_USER_DATA_DIR,
    profile_directory: str = PROFILE_DIRECTORY,
    headless: Optional[bool] = None,
    target_url: str = TARGET_URL,
    max_retries: int = 3,
) -> Optional[Dict[str, str]]:
    """Collect Kontur cookies via YandexDriver in background mode.

    Uses the real Yandex profile. Window stays hidden/off-screen (historical
    background auth). Optional true Chrome headless via ``HEADLESS`` /
    ``KONTUR_BROWSER_HEADLESS=1``.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception as exc:
        logger.error("Selenium не установлен или недоступен: %s", exc)
        return None

    win32_available = False
    try:
        import win32con  # noqa: F401
        import win32gui  # noqa: F401
        import win32process  # noqa: F401

        win32_available = True
    except ImportError as exc:
        logger.warning(
            "pywin32 не установлен. Окно браузера не будет скрыто. Установите: pip install pywin32"
        )
        logger.debug("Ошибка импорта pywin32: %s", exc)

    if not driver_path or not Path(driver_path).exists():
        logger.error("Driver not found: %s", driver_path)
        return None
    if not browser_path or not Path(browser_path).exists():
        logger.error("Browser binary not found: %s", browser_path)
        return None

    use_headless = _effective_headless(headless)
    logger.info(
        "Сбор cookies через Selenium (headless=%s, profile=%s)",
        use_headless,
        profile_directory,
    )

    for attempt in range(1, max_retries + 1):
        logger.info("Попытка получения cookies #%s", attempt)
        driver = None
        try:
            options = build_browser_options(
                browser_path=Path(browser_path),
                profile_user_data_dir=profile_user_data_dir,
                profile_directory=profile_directory,
                headless=use_headless,
            )
            service = Service(str(driver_path))
            driver = webdriver.Chrome(service=service, options=options)
            remove_webdriver_marker(driver)
            wait = WebDriverWait(driver, WAIT_TIMEOUT)

            if not use_headless and win32_available:
                hide_driver_windows(driver)

            driver.get(target_url)
            time.sleep(2.0)
            _click_cookie_accept_if_present(driver, By)

            cookies = wait_for_valid_cookies(driver, timeout_seconds=WAIT_TIMEOUT)
            if cookies and save_cookies_to_file(cookies):
                logger.info("Successfully refreshed Kontur cookies")
                return dict(cookies)

            _try_select_profile_and_warehouse(driver, wait, By, EC)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            cookies = wait_for_valid_cookies(driver, timeout_seconds=WAIT_TIMEOUT)
            if cookies and save_cookies_to_file(cookies):
                logger.info("Successfully refreshed Kontur cookies")
                return dict(cookies)

            raw_cookies = driver.get_cookies()
            if not raw_cookies:
                logger.warning("После загрузки страницы cookies не найдены")
                continue

            cookies = {item["name"]: item["value"] for item in raw_cookies}
            is_valid, missing_fields = validate_cookies(cookies)
            if not is_valid:
                logger.warning("Полученные cookies невалидны. Отсутствуют поля: %s", missing_fields)
                if attempt < max_retries:
                    time.sleep(2.0)
                continue

            if save_cookies_to_file(cookies):
                logger.info("Успешно получили и сохранили валидные cookies")
                return dict(cookies)
        except Exception:
            logger.exception("get_cookies failed on attempt %s", attempt)
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    logger.error("Не удалось получить валидные cookies после %s попыток", max_retries)
    return None


# Backward-compatible private aliases used by prolongation and older callers.
_build_browser_options = build_browser_options
_wait_for_valid_cookies = wait_for_valid_cookies
_remove_webdriver_marker = remove_webdriver_marker
_hide_driver_windows = hide_driver_windows
