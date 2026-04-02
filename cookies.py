import json
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

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


def _cookies_age(timestamp: float) -> float:
    return max(0.0, time.time() - float(timestamp or 0))


def _cookies_are_fresh(timestamp: float) -> bool:
    return bool(timestamp) and _cookies_age(timestamp) <= COOKIE_TTL


def _remember_cookies(cookies: Dict[str, str], timestamp: Optional[float] = None) -> None:
    global _MEMOIZED_COOKIES, _MEMOIZED_TIMESTAMP
    with _COOKIE_LOCK:
        _MEMOIZED_COOKIES = dict(cookies)
        _MEMOIZED_TIMESTAMP = float(timestamp or time.time())


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

            driver.get(target_url)
            time.sleep(2.0)

            try:
                cookie_btn = driver.find_elements(
                    By.XPATH,
                    '//*[@id="root"]/div/div/div[1]/div[1]/span/button/div[2]/span',
                )
                if cookie_btn:
                    cookie_btn[0].click()
                    time.sleep(SLEEP)
            except Exception as exc:
                logger.debug("Cookie accept button not available: %s", exc)

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
