import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from logger import logger
from utils import find_yandex_paths


paths = find_yandex_paths()
YANDEX_DRIVER_PATH = Path(r"driver\yandexdriver.exe")
YANDEX_BROWSER_PATH = paths["browser"]
PROFILE_USER_DATA_DIR = paths["user_data"]
PROFILE_DIRECTORY = "Vinsent O`neal"
HEADLESS = False

COOKIES_FILE = Path("kontur_cookies.json")
TARGET_URL = "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses"
WAIT_TIMEOUT = 20
SLEEP = 1.0
COOKIE_TTL = 13 * 60
PROLONGATION_URL = "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/settings#organization_settings_anchor_prolongation_token"
PROLONGATION_BUTTON_XPATH = "/html/body/div[1]/div/div/div[2]/div/div/div[1]/div[3]/div[1]/div[2]/div[6]/span/button/div[2]/span[2]"
PROLONGATION_SIGN_BUTTON_XPATH = "/html/body/div[5]/div/div[2]/div/div/div/div/div[2]/div[3]/div/div/div/div[2]/div/div/span[1]/span/button/div[2]/span[2]"
PROLONGATION_STATE_FILE = Path("kontur_access_prolongation.json")
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

_COOKIE_LOCK = threading.RLock()
_COOKIE_REFRESH_EVENT = threading.Event()
_COOKIE_REFRESH_IN_PROGRESS = False
_MEMOIZED_COOKIES: Optional[Dict[str, str]] = None
_MEMOIZED_TIMESTAMP = 0.0
_PROLONGATION_LOCK = threading.RLock()
_PROLONGATION_THREAD: Optional[threading.Thread] = None


def _cookies_age(timestamp: float) -> float:
    return max(0.0, time.time() - float(timestamp or 0))


def _cookies_are_fresh(timestamp: float) -> bool:
    return bool(timestamp) and _cookies_age(timestamp) <= COOKIE_TTL


def _remember_cookies(cookies: Dict[str, str], timestamp: Optional[float] = None) -> None:
    global _MEMOIZED_COOKIES, _MEMOIZED_TIMESTAMP
    with _COOKIE_LOCK:
        _MEMOIZED_COOKIES = dict(cookies)
        _MEMOIZED_TIMESTAMP = float(timestamp or time.time())


def _prolongation_enabled() -> bool:
    value = os.getenv(PROLONGATION_ENABLED_ENV, "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def _prolongation_interval_seconds() -> float:
    raw_value = os.getenv(
        PROLONGATION_INTERVAL_HOURS_ENV,
        str(DEFAULT_PROLONGATION_INTERVAL_HOURS),
    ).strip()
    try:
        hours = float(raw_value)
    except (TypeError, ValueError):
        hours = DEFAULT_PROLONGATION_INTERVAL_HOURS
    if hours <= 0:
        hours = DEFAULT_PROLONGATION_INTERVAL_HOURS
    return hours * 60.0 * 60.0


def _load_prolongation_state() -> Dict[str, Any]:
    if not PROLONGATION_STATE_FILE.exists():
        return {}
    try:
        payload = json.loads(PROLONGATION_STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Автопродление доступа: не удалось разобрать %s", PROLONGATION_STATE_FILE)
        return {}
    except Exception:
        logger.exception("Автопродление доступа: ошибка чтения %s", PROLONGATION_STATE_FILE)
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_prolongation_state(payload: Dict[str, Any]) -> None:
    PROLONGATION_STATE_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _timestamp_to_iso8601(timestamp: float) -> str:
    if not timestamp:
        return ""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(timestamp))


def _seconds_until_next_prolongation(
    last_success_ts: float,
    *,
    interval_seconds: Optional[float] = None,
    now: Optional[float] = None,
) -> float:
    current_time = float(now or time.time())
    interval = float(interval_seconds or _prolongation_interval_seconds())
    if last_success_ts <= 0:
        return 0.0
    return max(0.0, (float(last_success_ts) + interval) - current_time)


def _prolongation_is_due(
    last_success_ts: float,
    *,
    interval_seconds: Optional[float] = None,
    now: Optional[float] = None,
) -> bool:
    return _seconds_until_next_prolongation(
        last_success_ts,
        interval_seconds=interval_seconds,
        now=now,
    ) <= 0


def get_kontur_access_prolongation_state() -> Dict[str, Any]:
    with _PROLONGATION_LOCK:
        payload = _load_prolongation_state()
    last_success_ts = float(payload.get("last_success_ts", 0) or 0.0)
    last_attempt_ts = float(payload.get("last_attempt_ts", 0) or 0.0)
    interval_seconds = _prolongation_interval_seconds()
    seconds_until_due = _seconds_until_next_prolongation(
        last_success_ts,
        interval_seconds=interval_seconds,
    )
    return {
        "enabled": _prolongation_enabled(),
        "interval_hours": round(interval_seconds / 3600.0, 2),
        "last_success_ts": last_success_ts,
        "last_success_at": _timestamp_to_iso8601(last_success_ts),
        "last_attempt_ts": last_attempt_ts,
        "last_attempt_at": _timestamp_to_iso8601(last_attempt_ts),
        "last_error": str(payload.get("last_error") or "").strip(),
        "due": _prolongation_is_due(last_success_ts, interval_seconds=interval_seconds),
        "seconds_until_due": round(seconds_until_due, 2),
    }


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


def _hide_driver_windows(driver) -> None:
    try:
        import win32con  # type: ignore
        import win32gui  # type: ignore
        import win32process  # type: ignore
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

        handles = []
        win32gui.EnumWindows(enum_window_callback, handles)
        for hwnd in handles:
            win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
    except Exception as exc:
        logger.debug("Не удалось скрыть окно браузера Selenium: %s", exc)


def _click_first_matching_xpath(driver, wait, by, expected_conditions, xpaths: List[str], label: str):
    last_error: Optional[Exception] = None
    for xpath in xpaths:
        try:
            element = wait.until(expected_conditions.element_to_be_clickable((by.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.3)
            try:
                element.click()
            except Exception:
                driver.execute_script("arguments[0].click();", element)
            logger.info("Автопродление доступа: нажата кнопка '%s'", label)
            return element
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Не удалось нажать кнопку '{label}': {last_error}")


def _run_kontur_access_prolongation_browser_flow(
    driver_path: Path = YANDEX_DRIVER_PATH,
    browser_path: Path = YANDEX_BROWSER_PATH,
    profile_user_data_dir: Path = PROFILE_USER_DATA_DIR,
    profile_directory: str = PROFILE_DIRECTORY,
    headless: bool = HEADLESS,
    target_url: str = PROLONGATION_URL,
) -> None:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception as exc:
        raise RuntimeError(f"Selenium недоступен для автопродления доступа: {exc}") from exc

    if not driver_path or not Path(driver_path).exists():
        raise RuntimeError(f"Не найден yandexdriver: {driver_path}")
    if not browser_path or not Path(browser_path).exists():
        raise RuntimeError(f"Не найден Yandex Browser: {browser_path}")

    driver = None
    try:
        options = Options()
        options.binary_location = str(browser_path)
        options.add_argument(f"--user-data-dir={profile_user_data_dir}")
        options.add_argument(f"--profile-directory={profile_directory}")
        if headless:
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
        options.add_argument("--window-position=-32000,-32000")
        options.add_argument("--window-size=1920,1080")

        service = Service(str(driver_path))
        driver = webdriver.Chrome(service=service, options=options)
        _hide_driver_windows(driver)

        wait = WebDriverWait(driver, PROLONGATION_WAIT_TIMEOUT)
        driver.get(target_url)
        wait.until(lambda current_driver: current_driver.execute_script("return document.readyState") == "complete")
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2.0)

        _click_cookie_accept_if_present(driver, By)
        _click_first_matching_xpath(
            driver,
            wait,
            By,
            EC,
            [
                PROLONGATION_BUTTON_XPATH,
                "//button[.//span[normalize-space()='Продлить доступ']]",
                "//span/button[.//span[normalize-space()='Продлить доступ']]",
            ],
            "Продлить доступ",
        )
        time.sleep(1.0)
        sign_element = _click_first_matching_xpath(
            driver,
            wait,
            By,
            EC,
            [
                PROLONGATION_SIGN_BUTTON_XPATH,
                "//button[.//span[normalize-space()='Подписать и продлить']]",
                "//span/button[.//span[normalize-space()='Подписать и продлить']]",
            ],
            "Подписать и продлить",
        )
        try:
            wait.until(EC.staleness_of(sign_element))
        except Exception:
            time.sleep(5.0)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def prolong_kontur_access(force: bool = False) -> Dict[str, Any]:
    if not _prolongation_enabled():
        return {
            "success": False,
            "skipped": True,
            "reason": "disabled",
            "state": get_kontur_access_prolongation_state(),
        }

    with _PROLONGATION_LOCK:
        current_state = _load_prolongation_state()
        last_success_ts = float(current_state.get("last_success_ts", 0) or 0.0)
        interval_seconds = _prolongation_interval_seconds()
        if not force and not _prolongation_is_due(last_success_ts, interval_seconds=interval_seconds):
            state = get_kontur_access_prolongation_state()
            return {
                "success": True,
                "skipped": True,
                "reason": "not_due",
                "state": state,
            }

        attempt_ts = time.time()
        payload = dict(current_state)
        payload["last_attempt_ts"] = attempt_ts
        payload["last_attempt_at"] = _timestamp_to_iso8601(attempt_ts)
        _save_prolongation_state(payload)

        try:
            logger.info("Автопродление доступа: запускаем браузерный сценарий")
            _run_kontur_access_prolongation_browser_flow()
            success_ts = time.time()
            payload["last_success_ts"] = success_ts
            payload["last_success_at"] = _timestamp_to_iso8601(success_ts)
            payload["last_error"] = ""
            _save_prolongation_state(payload)
            logger.info("Автопродление доступа: успешно завершено")
            return {
                "success": True,
                "performed": True,
                "state": get_kontur_access_prolongation_state(),
            }
        except Exception as exc:
            payload["last_error"] = str(exc)
            _save_prolongation_state(payload)
            logger.exception("Автопродление доступа: ошибка выполнения")
            return {
                "success": False,
                "error": str(exc),
                "state": get_kontur_access_prolongation_state(),
            }


def _kontur_access_prolongation_worker() -> None:
    logger.info(
        "Автопродление доступа: фоновый цикл запущен (интервал %.2f ч)",
        _prolongation_interval_seconds() / 3600.0,
    )
    first_cycle = True
    while True:
        try:
            if first_cycle:
                first_cycle = False
                time.sleep(PROLONGATION_STARTUP_DELAY_SECONDS)
            result = prolong_kontur_access(force=False)
            if result.get("success") and result.get("skipped"):
                sleep_seconds = min(
                    PROLONGATION_IDLE_CHECK_SECONDS,
                    max(60.0, float(result.get("state", {}).get("seconds_until_due", PROLONGATION_IDLE_CHECK_SECONDS))),
                )
            elif result.get("success"):
                sleep_seconds = max(60.0, _prolongation_interval_seconds())
            else:
                sleep_seconds = PROLONGATION_RETRY_DELAY_SECONDS
                logger.warning(
                    "Автопродление доступа: повторим попытку через %s сек. Причина: %s",
                    int(sleep_seconds),
                    result.get("error") or result.get("reason") or "unknown",
                )
        except Exception as exc:
            logger.exception("Автопродление доступа: ошибка фонового цикла: %s", exc)
            sleep_seconds = PROLONGATION_RETRY_DELAY_SECONDS
        time.sleep(max(60.0, float(sleep_seconds)))


def ensure_kontur_access_prolongation_worker_started() -> bool:
    global _PROLONGATION_THREAD

    if not _prolongation_enabled():
        logger.info("Автопродление доступа: отключено через %s", PROLONGATION_ENABLED_ENV)
        return False

    with _PROLONGATION_LOCK:
        if _PROLONGATION_THREAD is not None and _PROLONGATION_THREAD.is_alive():
            return True
        _PROLONGATION_THREAD = threading.Thread(
            target=_kontur_access_prolongation_worker,
            daemon=True,
            name="KonturAccessProlongation",
        )
        _PROLONGATION_THREAD.start()
        return True


def run_kontur_access_prolongation_service() -> None:
    if not ensure_kontur_access_prolongation_worker_started():
        logger.info("Автопродление доступа: сервис не запущен, так как функция отключена.")
        return

    logger.info("Автопродление доступа: отдельный сервис запущен.")
    try:
        while True:
            time.sleep(60.0)
    except KeyboardInterrupt:
        logger.info("Автопродление доступа: сервис остановлен пользователем.")


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


def load_cookies_from_file() -> Optional[Dict[str, str]]:
    with _COOKIE_LOCK:
        if _MEMOIZED_COOKIES and _cookies_are_fresh(_MEMOIZED_TIMESTAMP):
            return dict(_MEMOIZED_COOKIES)

    if not COOKIES_FILE.exists():
        logger.info("Файл cookies не существует")
        return None

    try:
        data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        cookies = data.get("cookies")
        timestamp = float(data.get("timestamp", 0) or 0)

        if not _cookies_are_fresh(timestamp):
            logger.info("Cookies устарели (%.0f сек). Нужно обновить.", _cookies_age(timestamp))
            return None

        is_valid, missing_fields = validate_cookies(cookies)
        if not is_valid:
            logger.info("Cookies в файле невалидны. Отсутствуют поля: %s", missing_fields)
            return None

        _remember_cookies(cookies, timestamp)
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
        COOKIES_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _remember_cookies(cookies, timestamp)
        logger.info("Cookies сохранены в %s", COOKIES_FILE)
        return True
    except Exception:
        logger.exception("Ошибка при сохранении cookies")
        return False


def get_cookies(
    driver_path: Path = YANDEX_DRIVER_PATH,
    browser_path: Path = YANDEX_BROWSER_PATH,
    profile_user_data_dir: Path = PROFILE_USER_DATA_DIR,
    profile_directory: str = PROFILE_DIRECTORY,
    headless: bool = HEADLESS,
    target_url: str = TARGET_URL,
    max_retries: int = 3,
) -> Optional[Dict[str, str]]:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except Exception as exc:
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
        logger.debug("Ошибка импорта pywin32: %s", exc)

    if not driver_path or not Path(driver_path).exists():
        logger.error("Driver not found: %s", driver_path)
        return None
    if not browser_path or not Path(browser_path).exists():
        logger.error("Browser binary not found: %s", browser_path)
        return None

    for attempt in range(1, max_retries + 1):
        logger.info("Попытка получения cookies #%s", attempt)
        driver = None
        try:
            options = Options()
            options.binary_location = str(browser_path)
            options.add_argument(f"--user-data-dir={profile_user_data_dir}")
            options.add_argument(f"--profile-directory={profile_directory}")
            if headless:
                options.add_argument("--headless=new")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
            options.add_argument("--window-position=-32000,-32000")
            options.add_argument("--window-size=1920,1080")

            service = Service(str(driver_path))
            driver = webdriver.Chrome(service=service, options=options)
            wait = WebDriverWait(driver, WAIT_TIMEOUT)

            if win32gui and win32con and win32process:
                _hide_driver_windows(driver)

            driver.get(target_url)
            time.sleep(2.0)
            _click_cookie_accept_if_present(driver, By)

            try:
                profile_xpath = (
                    '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div'
                )
                wait.until(EC.element_to_be_clickable((By.XPATH, profile_xpath))).click()
                time.sleep(SLEEP)
            except Exception as exc:
                logger.debug("Profile select error: %s", exc)

            try:
                wait.until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '//*[@id="root"]/div/div/div[2]/div/div/div[1]/div[3]/ul/li/div[2]')
                    )
                ).click()
                time.sleep(SLEEP)
            except Exception as exc:
                logger.debug("Warehouse select error: %s", exc)

            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
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


def get_valid_cookies(force_refresh: bool = False) -> Optional[Dict[str, str]]:
    global _COOKIE_REFRESH_IN_PROGRESS

    if not force_refresh:
        cookies = load_cookies_from_file()
        if cookies:
            logger.info("Используем cookies из файла")
            return cookies

    became_refresher = False
    with _COOKIE_LOCK:
        if _COOKIE_REFRESH_IN_PROGRESS:
            logger.info("Ожидаем завершения параллельного обновления cookies")
        else:
            _COOKIE_REFRESH_IN_PROGRESS = True
            _COOKIE_REFRESH_EVENT.clear()
            became_refresher = True

    if not became_refresher:
        _COOKIE_REFRESH_EVENT.wait(timeout=120)
        cookies = load_cookies_from_file()
        if cookies:
            logger.info("Используем cookies после завершения параллельного обновления")
            return cookies
        with _COOKIE_LOCK:
            if not _COOKIE_REFRESH_IN_PROGRESS:
                _COOKIE_REFRESH_IN_PROGRESS = True
                _COOKIE_REFRESH_EVENT.clear()
                became_refresher = True

    if not became_refresher:
        return load_cookies_from_file()

    try:
        logger.info("Получаем новые cookies")
        cookies = get_cookies()
        if cookies:
            return cookies
        return load_cookies_from_file()
    finally:
        with _COOKIE_LOCK:
            _COOKIE_REFRESH_IN_PROGRESS = False
            _COOKIE_REFRESH_EVENT.set()


if __name__ == "__main__":
    cookies = get_valid_cookies()
    if cookies:
        logger.info("Cookies готовы и прошли проверку")
        is_valid, missing_fields = validate_cookies(cookies)
        logger.debug("Проверка cookies: valid=%s, missing_fields=%s", is_valid, missing_fields)
    else:
        logger.error("Не удалось получить валидные cookies")
