"""Selenium / YandexDriver cookie collection.

Working background mode (restored from d647455 / 60bd74f):
real Yandex profile, ``HEADLESS=False``, always park the window
off-screen and hide it via Win32. True ``--headless=new`` is opt-in
only and still keeps the off-screen size/position flags.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set

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
        # Opt-in only. Historical working path used HEADLESS=False + hide.
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

    # Always park off-screen (d647455). Do not gate on headless.
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


def classify_session_error(exc: BaseException) -> str:
    """Classify Selenium startup failures for retry / fail-fast policy.

    Returns one of: ``version``, ``profile_lock``, ``other``.
    """
    message = str(exc).lower()
    if "supports chrome version" in message or "this version of chromedriver only supports" in message:
        return "version"
    lock_markers = (
        "devtoolsactiveport",
        "chrome instance exited",
        "chrome failed to start",
        "user data directory is already in use",
        "profile is already in use",
        "no longer running",
    )
    if any(marker in message for marker in lock_markers):
        return "profile_lock"
    if "session not created" in message and (
        "crashed" in message or "exited" in message or "chrome" in message
    ):
        # Ambiguous SessionNotCreated — do not open three empty windows.
        return "profile_lock"
    return "other"


def _process_descendant_pids(root_pid: int) -> Set[int]:
    """Return ``root_pid`` plus all descendants via Toolhelp snapshot."""
    pids: Set[int] = {int(root_pid)}
    parent_map: Dict[int, int] = {}
    try:
        import ctypes
        from ctypes import wintypes

        class TAGPROCESSENTRY32(ctypes.Structure):
            _fields_ = [
                ("dwSize", wintypes.DWORD),
                ("cntUsage", wintypes.DWORD),
                ("th32ProcessID", wintypes.DWORD),
                ("th32DefaultHeapID", ctypes.POINTER(ctypes.c_ulong)),
                ("th32ModuleID", wintypes.DWORD),
                ("cntThreads", wintypes.DWORD),
                ("th32ParentProcessID", wintypes.DWORD),
                ("pcPriClassBase", ctypes.c_long),
                ("dwFlags", wintypes.DWORD),
                ("szExeFile", ctypes.c_wchar * 260),
            ]

        kernel32 = ctypes.windll.kernel32
        snap = kernel32.CreateToolhelp32Snapshot(0x00000002, 0)
        if snap in (-1, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF):
            return pids
        pe = TAGPROCESSENTRY32()
        pe.dwSize = ctypes.sizeof(TAGPROCESSENTRY32)
        try:
            if not kernel32.Process32FirstW(snap, ctypes.byref(pe)):
                return pids
            while True:
                parent_map[int(pe.th32ProcessID)] = int(pe.th32ParentProcessID)
                if not kernel32.Process32NextW(snap, ctypes.byref(pe)):
                    break
        finally:
            kernel32.CloseHandle(snap)
    except Exception:
        return pids

    changed = True
    while changed:
        changed = False
        for pid, parent in parent_map.items():
            if parent in pids and pid not in pids:
                pids.add(pid)
                changed = True
    return pids


def hide_driver_windows(driver) -> None:
    """Hide chromedriver *and* browser windows spawned for the session."""
    try:
        import win32con
        import win32gui
        import win32process
    except ImportError as exc:
        logger.debug("pywin32 недоступен для скрытия окна браузера: %s", exc)
        return

    try:
        service_pid = int(driver.service.process.pid)
    except Exception as exc:
        logger.debug("Не удалось получить PID YandexDriver: %s", exc)
        return

    try:
        time.sleep(0.5)
        target_pids = _process_descendant_pids(service_pid)
        # Browser may briefly outlive parent linkage; also match by exe path later.
        handles: list[int] = []

        def enum_window_callback(hwnd, _results):
            if not win32gui.IsWindowVisible(hwnd) and not win32gui.IsWindow(hwnd):
                return
            try:
                _, window_pid = win32process.GetWindowThreadProcessId(hwnd)
            except Exception:
                return
            if window_pid in target_pids:
                handles.append(hwnd)

        win32gui.EnumWindows(enum_window_callback, None)

        if not handles:
            # Fallback: hide recent Yandex Browser windows (same binary) when
            # process-tree linkage is not visible yet.
            browser_name = "browser.exe"
            try:
                import ctypes
                from ctypes import wintypes

                class TAGPROCESSENTRY32(ctypes.Structure):
                    _fields_ = [
                        ("dwSize", wintypes.DWORD),
                        ("cntUsage", wintypes.DWORD),
                        ("th32ProcessID", wintypes.DWORD),
                        ("th32DefaultHeapID", ctypes.POINTER(ctypes.c_ulong)),
                        ("th32ModuleID", wintypes.DWORD),
                        ("cntThreads", wintypes.DWORD),
                        ("th32ParentProcessID", wintypes.DWORD),
                        ("pcPriClassBase", ctypes.c_long),
                        ("dwFlags", wintypes.DWORD),
                        ("szExeFile", ctypes.c_wchar * 260),
                    ]

                kernel32 = ctypes.windll.kernel32
                snap = kernel32.CreateToolhelp32Snapshot(0x00000002, 0)
                pe = TAGPROCESSENTRY32()
                pe.dwSize = ctypes.sizeof(TAGPROCESSENTRY32)
                browser_pids: Set[int] = set()
                if kernel32.Process32FirstW(snap, ctypes.byref(pe)):
                    while True:
                        if pe.szExeFile.lower() == browser_name and int(pe.th32ParentProcessID) == service_pid:
                            browser_pids.add(int(pe.th32ProcessID))
                        if not kernel32.Process32NextW(snap, ctypes.byref(pe)):
                            break
                kernel32.CloseHandle(snap)
                target_pids |= browser_pids

                def enum_again(hwnd, _results):
                    try:
                        _, window_pid = win32process.GetWindowThreadProcessId(hwnd)
                    except Exception:
                        return
                    if window_pid in target_pids:
                        handles.append(hwnd)

                win32gui.EnumWindows(enum_again, None)
            except Exception as exc:
                logger.debug("Fallback browser window enum failed: %s", exc)

        for hwnd in handles:
            try:
                win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
            except Exception:
                pass
    except Exception as exc:
        logger.debug("Не удалось скрыть окно браузера Selenium: %s", exc)


def is_yandex_profile_busy(user_data_dir: Optional[Path] = None) -> bool:
    """True when Yandex Browser already holds the profile (Selenium would crash)."""
    root = Path(user_data_dir) if user_data_dir else None
    if root is not None:
        for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
            if (root / name).exists():
                return True

    browser_path = YANDEX_BROWSER_PATH
    needle = "yandexbrowser"
    try:
        import ctypes
        from ctypes import wintypes

        class TAGPROCESSENTRY32(ctypes.Structure):
            _fields_ = [
                ("dwSize", wintypes.DWORD),
                ("cntUsage", wintypes.DWORD),
                ("th32ProcessID", wintypes.DWORD),
                ("th32DefaultHeapID", ctypes.POINTER(ctypes.c_ulong)),
                ("th32ModuleID", wintypes.DWORD),
                ("cntThreads", wintypes.DWORD),
                ("th32ParentProcessID", wintypes.DWORD),
                ("pcPriClassBase", ctypes.c_long),
                ("dwFlags", wintypes.DWORD),
                ("szExeFile", ctypes.c_wchar * 260),
            ]

        kernel32 = ctypes.windll.kernel32
        snap = kernel32.CreateToolhelp32Snapshot(0x00000002, 0)
        if snap in (-1, 0xFFFFFFFF, 0xFFFFFFFFFFFFFFFF):
            return False
        pe = TAGPROCESSENTRY32()
        pe.dwSize = ctypes.sizeof(TAGPROCESSENTRY32)
        found = False
        try:
            if kernel32.Process32FirstW(snap, ctypes.byref(pe)):
                while True:
                    if pe.szExeFile.lower() == "browser.exe":
                        # Confirm it is Yandex, not an unrelated browser.exe.
                        try:
                            import win32api
                            import win32con
                            import win32process

                            handle = win32api.OpenProcess(
                                win32con.PROCESS_QUERY_LIMITED_INFORMATION,
                                False,
                                int(pe.th32ProcessID),
                            )
                            try:
                                path = win32process.GetModuleFileNameEx(handle, 0)
                            finally:
                                win32api.CloseHandle(handle)
                            if needle in str(path).lower():
                                found = True
                                break
                        except Exception:
                            # If path cannot be resolved, treat browser.exe as busy
                            # only when our known binary path exists under Program Files.
                            if browser_path and "yandex" in str(browser_path).lower():
                                found = True
                                break
                    if not kernel32.Process32NextW(snap, ctypes.byref(pe)):
                        break
        finally:
            kernel32.CloseHandle(snap)
        return found
    except Exception as exc:
        logger.debug("Could not detect running Yandex Browser: %s", exc)
        return False


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

    if profile_user_data_dir and is_yandex_profile_busy(Path(profile_user_data_dir)):
        logger.error(
            "Профиль Yandex Browser уже занят (браузер запущен). "
            "Selenium с профилем '%s' не запускаем — иначе открываются пустые окна. "
            "Закройте Яндекс.Браузер или используйте сохранённые/профильные cookies.",
            profile_directory,
        )
        return None

    use_headless = _effective_headless(headless)
    logger.info(
        "Сбор cookies через Selenium (headless=%s, profile=%s)",
        use_headless,
        profile_directory,
    )

    driver_repaired = False
    attempt = 0
    while attempt < max_retries:
        attempt += 1
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

            # Always hide when possible (d647455) — not gated on headless.
            if win32_available:
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
                if attempt < max_retries:
                    time.sleep(2.0)
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
        except Exception as exc:
            logger.exception("get_cookies failed on attempt %s", attempt)
            kind = classify_session_error(exc)

            if kind == "version":
                logger.error(
                    "Несовместимость YandexDriver и Яндекс.Браузера: %s. "
                    "Обновляем драйвер перед повтором.",
                    exc,
                )
                if driver_repaired:
                    logger.error(
                        "YandexDriver всё ещё не совпадает с браузером после обновления. "
                        "Выполните вручную: powershell -ExecutionPolicy Bypass "
                        "-File scripts\\ensure_yandex_driver.ps1 -Force"
                    )
                    return None
                driver_repaired = True
                if ensure_yandex_driver_updated(force=True):
                    # One repair + one retry only — never burn max_retries=3 on mismatch.
                    max_retries = min(max_retries, attempt + 1)
                    continue
                return None

            if kind == "profile_lock":
                logger.error(
                    "Не удалось создать сессию Selenium (профиль занят / DevToolsActivePort / "
                    "Chrome instance exited). Повторные запуски только откроют пустые окна. "
                    "Закройте Яндекс.Браузер с профилем '%s' и повторите.",
                    profile_directory,
                )
                # One optional driver ensure (non-force) in case a stale driver
                # crashed the browser; never open three empty windows.
                if not driver_repaired:
                    driver_repaired = True
                    ensure_yandex_driver_updated(force=False)
                    if is_yandex_profile_busy(
                        Path(profile_user_data_dir) if profile_user_data_dir else None
                    ):
                        return None
                    logger.info("Профиль свободен после проверки драйвера — одна повторная попытка")
                    max_retries = min(max_retries, attempt + 1)
                    continue
                return None
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
