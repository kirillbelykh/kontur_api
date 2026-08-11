"""Selenium / YandexDriver cookie collection.

Restored verbatim from the last known-good background flow (``60bd74f``
"best cookies update, works in bg"): real Yandex profile,
``HEADLESS = False``, window parked off-screen and hidden via Win32,
one attempt, no profile-busy guard.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional

from backend.services.logger import logger

from backend.auth.constants import (
    HEADLESS,
    PROFILE_DIRECTORY,
    PROFILE_USER_DATA_DIR,
    SLEEP,
    TARGET_URL,
    WAIT_TIMEOUT,
    YANDEX_BROWSER_PATH,
    YANDEX_DRIVER_PATH,
)
from backend.auth.store import save_cookies_to_file, validate_cookies


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
    """Build Chrome/Yandex options exactly as the working background flow did."""
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.binary_location = str(browser_path)

    if profile_user_data_dir:
        options.add_argument(f"--user-data-dir={Path(profile_user_data_dir)}")
    profile = str(profile_directory or "").strip()
    if profile:
        options.add_argument(f"--profile-directory={profile}")

    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

    # Off-screen parking is the fallback that keeps the window invisible.
    options.add_argument("--window-position=-32000,-32000")
    options.add_argument("--window-size=1920,1080")
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


def is_driver_version_mismatch(exc: BaseException) -> bool:
    """True when chromedriver refuses the installed Yandex Browser build."""
    message = str(exc).lower()
    return (
        "supports chrome version" in message
        or "this version of chromedriver only supports" in message
    )


def is_profile_in_use(exc: BaseException) -> bool:
    """True when the profile is already owned by a running Yandex Browser.

    Windows opens the profile exclusively, so the browser Selenium starts is
    handed over to the running instance and exits immediately.
    """
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "devtoolsactiveport",
            "chrome failed to start",
            "user data directory is already in use",
            "profile is already in use",
        )
    )


def hide_driver_windows(driver) -> None:
    """Hide windows owned by the driver process (Win32 ``SW_HIDE``)."""
    try:
        import win32con
        import win32gui
        import win32process
    except ImportError as exc:
        logger.debug("pywin32 недоступен для скрытия окна браузера: %s", exc)
        return

    try:
        pid = int(driver.service.process.pid)
    except Exception as exc:
        logger.debug("Не удалось получить PID YandexDriver: %s", exc)
        return

    try:
        time.sleep(1.0)
        handles: list[int] = []

        def enum_window_callback(hwnd, results):
            try:
                _, window_pid = win32process.GetWindowThreadProcessId(hwnd)
            except Exception:
                return
            if window_pid == pid:
                results.append(hwnd)

        win32gui.EnumWindows(enum_window_callback, handles)
        for hwnd in handles:
            try:
                win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
            except Exception:
                pass
        if handles:
            logger.info("Скрыто %s окон браузера по PID %s", len(handles), pid)
        else:
            logger.debug("Окна браузера по PID %s не найдены — окно уже за экраном", pid)
    except Exception as exc:
        logger.debug("Не удалось скрыть окно браузера Selenium: %s", exc)


def ensure_yandex_driver_updated(*, force: bool = True) -> bool:
    """Run scripts/ensure_yandex_driver.ps1. Returns True when the script exits 0."""
    ensure_script = Path(__file__).resolve().parents[2] / "scripts" / "ensure_yandex_driver.ps1"
    if not ensure_script.exists():
        logger.error("Не найден scripts/ensure_yandex_driver.ps1")
        return False
    cmd = [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ensure_script),
    ]
    if force:
        cmd.append("-Force")
    try:
        logger.info("Обновляем YandexDriver через ensure_yandex_driver.ps1...")
        completed = subprocess.run(cmd, check=False, timeout=180)
        ok = completed.returncode == 0
        if not ok:
            logger.error("ensure_yandex_driver.ps1 завершился с кодом %s", completed.returncode)
        return ok
    except Exception as exc:
        logger.warning("Автообновление YandexDriver не удалось: %s", exc)
        return False


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


def _collect_once(
    *,
    driver_path: Path,
    browser_path: Path,
    profile_user_data_dir: Optional[Path],
    profile_directory: str,
    headless: bool,
    target_url: str,
    hide_windows: bool,
) -> Optional[Dict[str, str]]:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    options = build_browser_options(
        browser_path=browser_path,
        profile_user_data_dir=profile_user_data_dir,
        profile_directory=profile_directory,
        headless=headless,
    )
    service = Service(str(driver_path))
    driver = webdriver.Chrome(service=service, options=options)
    try:
        remove_webdriver_marker(driver)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)
        if hide_windows:
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
            return None

        cookies = {item["name"]: item["value"] for item in raw_cookies}
        is_valid, missing_fields = validate_cookies(cookies)
        if not is_valid:
            logger.warning("Полученные cookies невалидны. Отсутствуют поля: %s", missing_fields)
            return None
        if save_cookies_to_file(cookies):
            logger.info("Успешно получили и сохранили валидные cookies")
            return dict(cookies)
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def get_cookies(
    driver_path: Path = YANDEX_DRIVER_PATH,
    browser_path: Optional[Path] = YANDEX_BROWSER_PATH,
    profile_user_data_dir: Optional[Path] = PROFILE_USER_DATA_DIR,
    profile_directory: str = PROFILE_DIRECTORY,
    headless: Optional[bool] = None,
    target_url: str = TARGET_URL,
) -> Optional[Dict[str, str]]:
    """Collect Kontur cookies via YandexDriver using the real profile.

    Single attempt, exactly like the working background flow. The only retry
    is one driver repair when chromedriver and Yandex Browser versions drift
    apart after a browser auto-update.
    """
    try:
        import selenium  # noqa: F401
    except Exception as exc:
        logger.error("Selenium не установлен или недоступен: %s", exc)
        return None

    try:
        import win32con  # noqa: F401
        import win32gui  # noqa: F401
        import win32process  # noqa: F401

        win32_available = True
    except ImportError as exc:
        win32_available = False
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

    for attempt in (1, 2):
        try:
            return _collect_once(
                driver_path=Path(driver_path),
                browser_path=Path(browser_path),
                profile_user_data_dir=profile_user_data_dir,
                profile_directory=profile_directory,
                headless=use_headless,
                target_url=target_url,
                hide_windows=win32_available,
            )
        except Exception as exc:
            logger.exception("get_cookies failed on attempt %s", attempt)
            if attempt == 1 and is_driver_version_mismatch(exc):
                logger.error(
                    "YandexDriver несовместим с Яндекс.Браузером: %s. Обновляем драйвер.",
                    exc,
                )
                if ensure_yandex_driver_updated(force=True):
                    continue
                return None
            if is_profile_in_use(exc):
                logger.error(
                    "Профиль '%s' занят запущенным Яндекс.Браузером — Windows держит его "
                    "монопольно, поэтому окно закрывается сразу. Закройте Яндекс.Браузер "
                    "полностью и повторите обновление cookies.",
                    profile_directory,
                )
            return None
    return None


# Backward-compatible private aliases used by prolongation and older callers.
_build_browser_options = build_browser_options
_wait_for_valid_cookies = wait_for_valid_cookies
_remove_webdriver_marker = remove_webdriver_marker
_hide_driver_windows = hide_driver_windows
