"""Selenium / YandexDriver cookie collection.

One Chrome window: auto-detected Yandex profile, click name → Лахта, then quit.
LAN sharing lives in ``backend.auth.service``.
"""

from __future__ import annotations

import ctypes
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set, Tuple

from backend.services.logger import logger

from backend.auth.constants import (
    HEADLESS,
    SELENIUM_PROFILE_DIRECTORY,
    SELENIUM_USER_DATA_DIR,
    SLEEP,
    TARGET_URL,
    WAIT_TIMEOUT,
    YANDEX_BROWSER_PATH,
    YANDEX_DRIVER_PATH,
)
from backend.auth.store import save_cookies_to_file, validate_cookies

# XPaths from 60bd74f (working background collection).
STEP1_NAME_XPATH = (
    '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]'
    "/div/div/div/div/div/div/div[1]/div/div/div/div[1]/div/div"
)
STEP2_WAREHOUSE_XPATH = '//*[@id="root"]/div/div/div[2]/div/div/div[1]/div[3]/ul/li/div[2]'

# Aliases kept for tests / callers.
PROFILE_CARD_XPATH = STEP1_NAME_XPATH
PROFILE_CARD_XPATHS = [STEP1_NAME_XPATH]
WAREHOUSE_CARD_XPATH = STEP2_WAREHOUSE_XPATH
WAREHOUSE_XPATHS = [STEP2_WAREHOUSE_XPATH]
WAREHOUSE_NAME = (os.getenv("KONTUR_WAREHOUSE_NAME") or "Лахта").strip()


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


def _click_xpath(driver, wait, by, expected_conditions, xpath: str, label: str) -> bool:
    """Desktop-style click: wait clickable → scroll → click (JS fallback)."""
    try:
        element = wait.until(expected_conditions.element_to_be_clickable((by.XPATH, xpath)))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.1)
        try:
            element.click()
        except Exception:
            driver.execute_script("arguments[0].click();", element)
        logger.info("%s: clicked", label)
        time.sleep(0.3)
        return True
    except Exception as exc:
        logger.info("%s: miss — %s", label, exc)
        return False


def click_first_available(driver, xpaths, step_name: str, *, timeout_per_xpath: float = 8.0) -> bool:
    """Compatibility helper used by tests."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    wait = WebDriverWait(driver, timeout_per_xpath)
    for xpath in xpaths:
        if _click_xpath(driver, wait, By, EC, xpath, step_name):
            return True
    return False


def _click_profile_card(driver, *, timeout_per_xpath: float = 10.0) -> bool:
    return click_first_available(driver, PROFILE_CARD_XPATHS, "Шаг 1 — имя", timeout_per_xpath=timeout_per_xpath)


def _click_warehouse_card(driver, *, timeout_per_xpath: float = 10.0) -> bool:
    return click_first_available(
        driver,
        WAREHOUSE_XPATHS,
        f"Шаг 2 — склад «{WAREHOUSE_NAME}»",
        timeout_per_xpath=timeout_per_xpath,
    )


def complete_kontur_certificate_login(driver) -> bool:
    """Best-effort: on login page the step-1 XPath is the name/cert control."""
    return _click_profile_card(driver)


def build_browser_options(
    *,
    browser_path: Path,
    profile_user_data_dir: Optional[Path],
    profile_directory: Optional[str],
    headless: bool,
) -> Any:
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.binary_location = str(browser_path)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if profile_user_data_dir:
        options.add_argument(f"--user-data-dir={Path(profile_user_data_dir).resolve()}")
    profile = str(profile_directory or "").strip()
    if profile:
        options.add_argument(f"--profile-directory={profile}")

    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--remote-debugging-port=0")
    options.add_argument("--window-position=-32000,-32000")
    options.add_argument("--window-size=1920,1080")
    return options


def build_chrome_service(driver_path: Path):
    """YandexDriver without a console window (this PC runs Selenium; others often don't)."""
    from selenium.webdriver.chrome.service import Service

    from backend.services.win_subprocess import hidden_console_kwargs

    service = Service(str(driver_path))
    flags = hidden_console_kwargs().get("creationflags")
    if flags:
        service.creation_flags = int(flags)
    return service


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
        logger.debug("Ожидаем cookies, отсутствуют: %s", missing_fields)
        time.sleep(0.25)
    return None


def remove_webdriver_marker(driver) -> None:
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
    message = str(exc).lower()
    return (
        "supports chrome version" in message
        or "this version of chromedriver only supports" in message
    )


def is_profile_in_use(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "devtoolsactiveport",
            "chrome failed to start",
            "user data directory is already in use",
            "profile is already in use",
            "no longer running",
        )
    )


def _iter_process_entries() -> Iterator[Tuple[int, int, str]]:
    """Yield (pid, parent_pid, exe_name_lower) for every running process."""
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
        return

    pe = TAGPROCESSENTRY32()
    pe.dwSize = ctypes.sizeof(TAGPROCESSENTRY32)
    try:
        if not kernel32.Process32FirstW(snap, ctypes.byref(pe)):
            return
        while True:
            yield int(pe.th32ProcessID), int(pe.th32ParentProcessID), pe.szExeFile.lower()
            if not kernel32.Process32NextW(snap, ctypes.byref(pe)):
                break
    finally:
        kernel32.CloseHandle(snap)


def _iter_yandex_browser_pids(browser_path: Optional[Path] = None) -> Set[int]:
    from ctypes import wintypes

    kernel32 = ctypes.windll.kernel32
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    found: Set[int] = set()
    for pid, _parent, exe in _iter_process_entries():
        if exe != "browser.exe":
            continue
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        image = ""
        if handle:
            try:
                buf = ctypes.create_unicode_buffer(1024)
                size = wintypes.DWORD(1024)
                if kernel32.QueryFullProcessImageNameW(handle, 0, buf, ctypes.byref(size)):
                    image = buf.value.lower()
            finally:
                kernel32.CloseHandle(handle)
        if image and "yandex" in image and "browser" in image:
            found.add(pid)
    return found


def yandex_browser_is_running() -> bool:
    """True when the operator already has Yandex Browser open."""
    try:
        return bool(_iter_yandex_browser_pids())
    except Exception:
        return False


def _iter_descendant_pids(root_pid: int) -> Set[int]:
    children: Dict[int, Set[int]] = {}
    for pid, parent, _exe in _iter_process_entries():
        children.setdefault(parent, set()).add(pid)

    result: Set[int] = set()
    queue = [int(root_pid)]
    while queue:
        current = queue.pop()
        for child in children.get(current, ()):  # PIDs могут переиспользоваться — глубина тут мала
            if child not in result:
                result.add(child)
                queue.append(child)
    return result


def _terminate_pids(pids: Set[int]) -> int:
    if not pids:
        return 0
    PROCESS_TERMINATE = 0x0001
    kernel32 = ctypes.windll.kernel32
    killed = 0
    for pid in sorted(pids):
        handle = kernel32.OpenProcess(PROCESS_TERMINATE, False, int(pid))
        if not handle:
            continue
        try:
            if kernel32.TerminateProcess(handle, 1):
                killed += 1
        finally:
            kernel32.CloseHandle(handle)
    return killed


def terminate_yandex_browser_processes(browser_path: Optional[Path] = None) -> int:
    killed = _terminate_pids(_iter_yandex_browser_pids(browser_path or YANDEX_BROWSER_PATH))
    if killed:
        logger.info("Закрыто процессов Яндекс.Браузера: %s", killed)
        time.sleep(2.0)
    return killed


def _hide_windows_for_pids(pids: Set[int]) -> int:
    try:
        import win32con
        import win32gui
        import win32process
    except ImportError:
        return 0

    handles: list[int] = []

    def enum_window_callback(hwnd, results):
        _, window_pid = win32process.GetWindowThreadProcessId(hwnd)
        if window_pid in pids:
            results.append(hwnd)

    win32gui.EnumWindows(enum_window_callback, handles)
    for hwnd in handles:
        win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
    return len(handles)


def _start_new_window_hider(before_pids: Set[int]) -> threading.Event:
    """Hide windows of browser processes that appear after ``before_pids``.

    The browser window pops up while ``webdriver.Chrome()`` is still blocking,
    so hiding must run in parallel with the launch, not after it.
    """
    stop = threading.Event()

    def loop() -> None:
        hidden_total = 0
        while not stop.is_set():
            try:
                fresh = _iter_yandex_browser_pids() - before_pids
                if fresh:
                    hidden = _hide_windows_for_pids(fresh)
                    if hidden:
                        hidden_total += hidden
                        logger.info("Скрыто %s новых окон браузера (всего %s)", hidden, hidden_total)
            except Exception as exc:
                logger.debug("Сторож окон браузера: %s", exc)
            stop.wait(0.05)

    threading.Thread(target=loop, daemon=True, name="browser-window-hider").start()
    return stop


def hide_driver_windows(driver) -> None:
    """Hide by driver PID, same as 60bd74f."""
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

        results: list[int] = []
        win32gui.EnumWindows(enum_window_callback, results)
        if results:
            for hwnd in results:
                win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
            logger.info("Скрыто %s окон браузера по PID %s", len(results), pid)
        else:
            logger.warning("Не удалось найти окна браузера по PID для скрытия")
    except Exception as exc:
        logger.debug("Не удалось скрыть окно браузера: %s", exc)


def ensure_yandex_driver_updated(*, force: bool = True) -> bool:
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
        from backend.services.win_subprocess import hidden_console_kwargs

        completed = subprocess.run(cmd, check=False, timeout=180, **hidden_console_kwargs())
        return completed.returncode == 0
    except Exception as exc:
        logger.warning("Автообновление YandexDriver не удалось: %s", exc)
        return False


def get_cookies(
    driver_path: Path = YANDEX_DRIVER_PATH,
    browser_path: Optional[Path] = YANDEX_BROWSER_PATH,
    profile_user_data_dir: Optional[Path] = SELENIUM_USER_DATA_DIR,
    profile_directory: str = SELENIUM_PROFILE_DIRECTORY,
    headless: bool = HEADLESS,
    target_url: str = TARGET_URL,
    max_retries: int = 1,
) -> Optional[Dict[str, str]]:
    """One Chrome on the app's persistent profile, then quit."""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception as exc:
        logger.exception("Selenium import failed")
        logger.error("Selenium не установлен или недоступен: %s", exc)
        return None

    win32gui = None
    win32con = None
    win32process = None
    try:
        import win32con  # type: ignore
        import win32gui  # type: ignore
        import win32process  # type: ignore
    except ImportError as exc:
        logger.warning(
            "pywin32 не установлен. Окно браузера не будет скрыто. Установите: pip install pywin32"
        )
        logger.warning("Ошибка импорта pywin32: %s", exc)

    if not driver_path or not Path(driver_path).exists():
        logger.error("Driver not found: %s", driver_path)
        return None
    if not browser_path or not Path(browser_path).exists():
        logger.error("Browser binary not found: %s", browser_path)
        return None

    profile_dir = Path(profile_user_data_dir).resolve() if profile_user_data_dir else None
    if profile_dir:
        profile_dir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Профиль программы: user-data=%s profile=%s",
        profile_dir,
        profile_directory,
    )

    opts = build_browser_options(
        browser_path=Path(browser_path),
        profile_user_data_dir=profile_dir,
        profile_directory=profile_directory,
        headless=bool(headless),
    )

    driver = None
    try:
        service = build_chrome_service(Path(driver_path))
        driver = webdriver.Chrome(service=service, options=opts)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        if win32gui and win32con and win32process:
            pid = driver.service.process.pid
            time.sleep(1.0)

            def enum_window_callback(hwnd, results):
                _, window_pid = win32process.GetWindowThreadProcessId(hwnd)
                if window_pid == pid:
                    results.append(hwnd)

            results: list[int] = []
            win32gui.EnumWindows(enum_window_callback, results)
            if results:
                for hwnd in results:
                    win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
                logger.info("Скрыто %s окон браузера по PID %s", len(results), pid)
            else:
                logger.warning("Не удалось найти окна браузера по PID для скрытия")

        driver.get(target_url)
        time.sleep(2.0)

        try:
            cookie_btn = driver.find_elements(
                By.XPATH,
                '//*[@id="root"]/div/div/div[1]/div[1]/span/button/div[2]/span',
            )
            if cookie_btn:
                cookie_btn[0].click()
                logger.info("Clicked cookie accept")
                time.sleep(SLEEP)
            else:
                logger.info("Cookie accept button not found on page - skipping")
        except Exception as exc:
            logger.info("Error with cookie accept button: %s - skipping", exc)

        try:
            profile_xpath = STEP1_NAME_XPATH
            profile_el = wait.until(EC.element_to_be_clickable((By.XPATH, profile_xpath)))
            profile_el.click()
            logger.info("Clicked profile (best-effort)")
            time.sleep(SLEEP)
        except Exception as exc:
            logger.info("Profile select error: %s - ignored", exc)

        try:
            warehouse_el = wait.until(
                EC.element_to_be_clickable((By.XPATH, STEP2_WAREHOUSE_XPATH))
            )
            warehouse_el.click()
            logger.info("Clicked warehouse (fallback selector)")
            time.sleep(SLEEP)
        except Exception as exc:
            logger.info("Warehouse select error: %s - ignored", exc)

        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        logger.info("Страница загружена (body найден)")

        raw = driver.get_cookies()
        if not raw:
            logger.warning(
                "Нет cookies после загрузки - возможно, требуется авторизация или ошибка"
            )
            return None

        cookies = {item["name"]: item["value"] for item in raw}
        save_cookies_to_file(cookies)
        logger.info("Collected cookies keys: %s", list(cookies.keys()))
        return cookies
    except Exception as exc:
        logger.exception("get_cookies failed")
        logger.error("get_cookies failed: %s", exc)
        return None
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


_build_browser_options = build_browser_options
_wait_for_valid_cookies = wait_for_valid_cookies
_remove_webdriver_marker = remove_webdriver_marker
_hide_driver_windows = hide_driver_windows
