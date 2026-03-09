import time
import json
import os
import importlib
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Optional, List

from logger import logger
from utils import find_yandex_paths

paths = find_yandex_paths()
# Настройки — поправь пути под систему
YANDEX_DRIVER_PATH = Path("driver") / "yandexdriver.exe"
YANDEX_BROWSER_PATH = paths["browser"]
PROFILE_USER_DATA_DIR = (
    Path(os.environ["KONTUR_YANDEX_USER_DATA_DIR"])
    if "KONTUR_YANDEX_USER_DATA_DIR" in os.environ
    else paths["user_data"]
)
PROFILE_DIRECTORY = os.environ.get("KONTUR_YANDEX_PROFILE", paths["profile"] or "Default")
ALLOW_TEMP_PROFILE_FALLBACK = os.environ.get("KONTUR_ALLOW_TEMP_PROFILE_FALLBACK", "0") == "1"
HEADLESS = False

COOKIES_FILE = Path("kontur_cookies.json")
TARGET_URL = "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses"
WAIT_TIMEOUT = 20
SLEEP = 1.0
COOKIE_TTL = 10 * 60  # 15 минут в секундах

# Обязательные поля cookies, которые должны присутствовать
REQUIRED_COOKIE_FIELDS = [
    "auth.sid",
    "token", 
    "portaluserid",
    "auth.check",
    "ngtoken",
    "device"
]

# Дополнительные поля, которые желательны но не обязательны
OPTIONAL_COOKIE_FIELDS = [
    "gdpr-consent",
    "_kmts", 
    "_mfp",
    "_kfpxv5"
]

PROFILE_CARD_XPATHS = [
    '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div[1]/div/div[1]/div/div/span/span',
    '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div',
    '//div[contains(@class,"profile") and .//span]',
]

WAREHOUSE_XPATHS = [
    '//*[@id="root"]/div/div/div[2]/div/div/div[1]/div[3]/ul/li/div[2]',
    '//div[@role="button" and contains(., "Склад")]',
]


def click_first_available(driver, xpaths: List[str], step_name: str, timeout_per_xpath: int = 4) -> bool:
    """Пытается кликнуть первый доступный элемент по списку XPath."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    for xpath in xpaths:
        try:
            local_wait = WebDriverWait(driver, timeout_per_xpath)
            element = local_wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            try:
                element.click()
            except Exception:
                driver.execute_script("arguments[0].click();", element)
            logger.info(f"{step_name}: clicked by xpath {xpath}")
            return True
        except Exception:
            continue
    logger.info(f"{step_name}: no matching clickable elements")
    return False


def validate_cookies(cookies: Dict[str, str]) -> tuple[bool, List[str]]:
    """
    Проверяет cookies на наличие обязательных полей.
    Возвращает (is_valid, missing_fields)
    """
    if not cookies:
        return False, ["all cookies missing"]
    
    missing_required = []
    for field in REQUIRED_COOKIE_FIELDS:
        if field not in cookies:
            missing_required.append(field)
    
    # Проверяем, что значения не пустые
    empty_fields = []
    for field in REQUIRED_COOKIE_FIELDS:
        if field in cookies and not cookies[field]:
            empty_fields.append(field)
    
    missing_optional = []
    for field in OPTIONAL_COOKIE_FIELDS:
        if field not in cookies:
            missing_optional.append(field)
    
    if missing_required:
        return False, missing_required
    
    if empty_fields:
        logger.warning(f"Обязательные поля cookies пустые: {empty_fields}")
        return False, empty_fields
    
    if missing_optional:
        logger.info(f"Отсутствуют необязательные поля cookies: {missing_optional}")
    
    return True, []


def load_cookies_from_file() -> Optional[Dict[str, str]]:
    """Загружает cookies из файла, проверяя возраст и валидность."""
    if not COOKIES_FILE.exists():
        logger.info("Файл cookies не существует")
        return None
    
    try:
        data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        cookies = data.get("cookies")
        ts = data.get("timestamp", 0)
        
        # Проверяем возраст cookies
        age = time.time() - ts
        if age > COOKIE_TTL:
            logger.info(f"Cookies устарели ({age:.0f} сек). Нужно обновить.")
            return None
        
        # Проверяем наличие обязательных полей
        is_valid, missing_fields = validate_cookies(cookies)
        if not is_valid:
            logger.info(f"Cookies в файле невалидны. Отсутствуют поля: {missing_fields}")
            return None
        
        logger.info("Cookies успешно загружены из файла и прошли проверку")
        return cookies
        
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON в файле cookies: {e}")
        return None
    except Exception as e:
        logger.exception("Ошибка при чтении cookies из файла")
        logger.error(f"Ошибка при чтении cookies из файла: {e}")
        return None


def save_cookies_to_file(cookies: Dict[str, str]) -> bool:
    """Сохраняет cookies + метку времени. Возвращает успешность операции."""
    try:
        # Проверяем cookies перед сохранением
        is_valid, missing_fields = validate_cookies(cookies)
        if not is_valid:
            logger.error(f"Нельзя сохранить невалидные cookies. Отсутствуют поля: {missing_fields}")
            return False
        
        data = {
            "timestamp": time.time(),
            "cookies": cookies
        }
        COOKIES_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"Cookies сохранены в {COOKIES_FILE}")
        return True
        
    except Exception as e:
        logger.exception("Ошибка при сохранении cookies")
        logger.error(f"Ошибка при сохранении cookies: {e}")
        return False


def get_cookies(driver_path: Path = YANDEX_DRIVER_PATH,
                browser_path: Optional[Path] = YANDEX_BROWSER_PATH,
                profile_user_data_dir: Optional[Path] = PROFILE_USER_DATA_DIR,
                profile_directory: str = PROFILE_DIRECTORY,
                headless: bool = HEADLESS,
                target_url: str = TARGET_URL,
                max_retries: int = 3) -> Optional[Dict[str, str]]:
    """
    Получает cookies через Selenium и сохраняет их в файл с timestamp.
    Повторяет попытки при неудаче.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
    except Exception as e:
        logger.exception("Selenium import failed")
        logger.error(f"Selenium не установлен или недоступен: {e}")
        return None

    # Импорт pywin32 для скрытия окна
    win32gui_mod = None
    win32con_mod = None
    win32process_mod = None
    try:
        win32gui_mod = importlib.import_module("win32gui")
        win32con_mod = importlib.import_module("win32con")
        win32process_mod = importlib.import_module("win32process")
    except ImportError as e:
        logger.warning("pywin32 не установлен. Окно браузера не будет скрыто. Установите: pip install pywin32")
        logger.warning(f"Ошибка импорта pywin32: {e}")

    # Повторно проверяем пути на момент запуска (вдруг браузер установили после старта приложения)
    try:
        runtime_paths = find_yandex_paths()
    except Exception as e:
        runtime_paths = {"browser": None, "user_data": None, "profile": "Default"}
        logger.warning(f"Не удалось автоматически определить пути Яндекс Браузера: {e}")

    if browser_path is None and runtime_paths["browser"] is not None:
        browser_path = runtime_paths["browser"]
    if profile_user_data_dir is None and runtime_paths["user_data"] is not None:
        profile_user_data_dir = runtime_paths["user_data"]
    if not profile_directory:
        profile_directory = runtime_paths["profile"] or "Default"

    if not driver_path.exists():
        logger.error(f"Driver not found: {driver_path}")
        return None
    if browser_path is None or not browser_path.exists():
        logger.error(f"Browser binary not found: {browser_path}")
        return None

    user_data_dir_for_option = profile_user_data_dir
    if profile_user_data_dir and profile_user_data_dir.name.lower() == "default":
        # Selenium ожидает путь к "User Data", а профиль задаётся отдельным параметром.
        user_data_dir_for_option = profile_user_data_dir.parent
        if not profile_directory:
            profile_directory = "Default"

    if not profile_directory:
        profile_directory = "Default"

    logger.info(f"Профиль браузера для Selenium: {profile_directory}")
    logger.info(f"Путь user-data-dir: {user_data_dir_for_option}")

    for attempt in range(max_retries):
        logger.info(f"Попытка получения cookies #{attempt + 1}")

        launch_modes: list[tuple[str, Optional[Path], Optional[str], bool]] = []
        if user_data_dir_for_option:
            launch_modes.append(("profile", user_data_dir_for_option, profile_directory, True))
        else:
            logger.warning("Папка профиля Яндекс Браузера не определена")
        if ALLOW_TEMP_PROFILE_FALLBACK:
            launch_modes.append(("temporary", None, None, False))
        elif not user_data_dir_for_option:
            logger.error(
                "Временный профиль отключен. Укажите профиль через KONTUR_YANDEX_USER_DATA_DIR/KONTUR_YANDEX_PROFILE."
            )

        for mode_name, launch_user_data_dir, launch_profile_dir, hide_window in launch_modes:
            driver = None
            temp_profile_dir: Optional[Path] = None
            try:
                if mode_name == "temporary":
                    temp_profile_dir = Path(tempfile.mkdtemp(prefix="konturapi-yandex-profile-"))
                    launch_user_data_dir = temp_profile_dir
                    launch_profile_dir = None
                    logger.warning("Резервный запуск Яндекс.Браузера с временным профилем")

                opts = Options()
                opts.binary_location = str(browser_path)
                if launch_user_data_dir:
                    opts.add_argument(f"--user-data-dir={launch_user_data_dir}")
                if launch_profile_dir:
                    opts.add_argument(f"--profile-directory={launch_profile_dir}")
                if headless:
                    opts.add_argument("--headless=new")
                    opts.add_argument("--no-sandbox")
                    opts.add_argument("--disable-dev-shm-usage")
                    opts.add_argument("--disable-gpu")
                if hide_window and not headless:
                    opts.add_argument("--window-position=-32000,-32000")
                opts.add_argument("--window-size=1920,1080")
                opts.add_argument("--remote-debugging-port=0")
                opts.add_argument("--no-first-run")
                opts.add_argument("--no-default-browser-check")
                opts.add_argument("--disable-background-networking")
                opts.add_argument("--disable-component-update")
                opts.add_argument("--disable-sync")
                opts.add_argument("--disable-features=Translate,OptimizationHints")

                service = Service(str(driver_path))
                driver = webdriver.Chrome(service=service, options=opts)
                wait = WebDriverWait(driver, WAIT_TIMEOUT)

                if mode_name == "temporary" and not headless:
                    logger.warning(
                        "Открылся браузер с временным профилем. Войдите в Контур вручную и дождитесь завершения."
                    )

                # Скрытие окна браузера с помощью Windows API по PID (если pywin32 доступен)
                if hide_window and win32gui_mod and win32con_mod and win32process_mod:
                    if driver.service is None or driver.service.process is None:
                        logger.warning("Сервис Selenium недоступен для скрытия окна браузера")
                        pid = None
                    else:
                        pid = driver.service.process.pid
                    time.sleep(1.0)  # Ждём запуска окна

                    def enum_window_callback(hwnd, results):
                        _, window_pid = win32process_mod.GetWindowThreadProcessId(hwnd)
                        if pid is not None and window_pid == pid:
                            results.append(hwnd)

                    results: list[int] = []
                    win32gui_mod.EnumWindows(enum_window_callback, results)
                    if results:
                        for hwnd in results:
                            win32gui_mod.ShowWindow(hwnd, win32con_mod.SW_HIDE)
                        logger.info(f"Скрыто {len(results)} окон браузера по PID {pid}")
                    else:
                        logger.warning("Не удалось найти окна браузера по PID для скрытия")

                driver.get(target_url)
                time.sleep(2.0)  # Увеличили задержку для полной загрузки

                # best-effort шаги с дополнительными проверками
                try:
                    # Проверяем наличие кнопки на странице
                    cookie_btn = driver.find_elements(By.XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/span/button/div[2]/span')
                    if cookie_btn:
                        cookie_btn[0].click()
                        logger.info("Clicked cookie accept")
                        time.sleep(SLEEP)
                    else:
                        logger.info("Cookie accept button not found on page - skipping")
                except Exception as e:
                    logger.info(f"Error with cookie accept button: {e} - skipping")

                click_first_available(driver, PROFILE_CARD_XPATHS, "Profile select")
                time.sleep(SLEEP)
                click_first_available(driver, WAREHOUSE_XPATHS, "Warehouse select")
                time.sleep(SLEEP)

                # Дополнительная проверка загрузки страницы
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                logger.info("Страница загружена (body найден)")

                validation_wait_seconds = 120 if mode_name == "profile" else 180
                deadline = time.time() + validation_wait_seconds
                missing_fields: List[str] = []
                next_click_ts = time.time() + 3

                while True:
                    raw = driver.get_cookies()
                    if raw:
                        cookies = {c["name"]: c["value"] for c in raw}
                        is_valid, missing_fields = validate_cookies(cookies)
                        if is_valid:
                            if save_cookies_to_file(cookies):
                                logger.info("Успешно получили и сохранили валидные cookies")
                                return cookies
                            logger.error("Не удалось сохранить cookies")
                            break
                    else:
                        missing_fields = ["all cookies missing"]

                    if time.time() >= next_click_ts:
                        # Периодически повторяем клики: интерфейс может догружаться асинхронно.
                        click_first_available(driver, PROFILE_CARD_XPATHS, "Profile retry")
                        click_first_available(driver, WAREHOUSE_XPATHS, "Warehouse retry")
                        next_click_ts = time.time() + 5

                    if time.time() >= deadline:
                        break
                    time.sleep(2)

                logger.warning(
                    f"Cookies не получены в режиме {mode_name}. Отсутствуют поля: {missing_fields}"
                )

            except Exception as e:
                logger.exception(
                    f"get_cookies failed on attempt {attempt + 1} in mode '{mode_name}'"
                )
                logger.error(f"get_cookies failed: {e}")
                error_text = str(e).lower()
                if "devtoolsactiveport" in error_text:
                    logger.warning(
                        "Ошибка DevToolsActivePort: закройте все окна Яндекс.Браузера и попробуйте снова."
                    )
                if mode_name == "profile" and ALLOW_TEMP_PROFILE_FALLBACK:
                    logger.info("Пробуем резервный запуск без пользовательского профиля")
            finally:
                if driver is not None:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                if temp_profile_dir is not None:
                    shutil.rmtree(temp_profile_dir, ignore_errors=True)

    logger.error(f"Не удалось получить валидные cookies после {max_retries} попыток")
    return None


def get_valid_cookies() -> Optional[Dict[str, str]]:
    """Основная точка входа: возвращает валидные cookies (из файла или новые)."""
    cookies = load_cookies_from_file()
    if cookies:
        logger.info("Используем cookies из файла")
        return cookies
    
    logger.info("Получаем новые cookies")
    return get_cookies()


if __name__ == "__main__":
    c = get_valid_cookies()
    if c:
        logger.info("Cookies готовы и прошли проверку")
        is_valid, missing = validate_cookies(c)
        logger.info(f"Проверка cookies: valid={is_valid}, missing_fields={missing}")
    else:
        logger.error("Не удалось получить валидные cookies")
