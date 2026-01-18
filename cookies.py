import json
from logger import logger
import time
from pathlib import Path
from typing import Dict, Optional, Any, List
from utils import find_yandex_paths

paths = find_yandex_paths()
# Настройки — поправь пути под систему
YANDEX_DRIVER_PATH = Path(r"driver\yandexdriver.exe")
YANDEX_BROWSER_PATH = paths['browser']
PROFILE_USER_DATA_DIR = paths['user_data']
PROFILE_DIRECTORY = "Vinsent O`neal"
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
                browser_path: Path = YANDEX_BROWSER_PATH,
                profile_user_data_dir: Path = PROFILE_USER_DATA_DIR,
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
    win32gui = None
    win32con = None
    win32process = None
    try:
        import win32gui
        import win32con
        import win32process
    except ImportError as e:
        logger.warning("pywin32 не установлен. Окно браузера не будет скрыто. Установите: pip install pywin32")
        logger.warning(f"Ошибка импорта pywin32: {e}")

    if not driver_path.exists():
        logger.error(f"Driver not found: {driver_path}")
        return None
    if not browser_path.exists():
        logger.error(f"Browser binary not found: {browser_path}")
        return None

    for attempt in range(max_retries):
        logger.info(f"Попытка получения cookies #{attempt + 1}")
        
        opts = Options()
        opts.binary_location = str(browser_path)
        opts.add_argument(f"--user-data-dir={profile_user_data_dir}")
        opts.add_argument(f"--profile-directory={profile_directory}")
        if headless:
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
        # Добавляем сдвиг окна за экран как fallback
        opts.add_argument("--window-position=-32000,-32000")
        opts.add_argument("--window-size=1920,1080")

        service = Service(str(driver_path))
        driver = webdriver.Chrome(service=service, options=opts)
        wait = WebDriverWait(driver, WAIT_TIMEOUT)

        try:
            # Скрытие окна браузера с помощью Windows API по PID (если pywin32 доступен)
            if win32gui and win32con and win32process:
                pid = driver.service.process.pid
                time.sleep(1.0)  # Ждём запуска окна

                def enum_window_callback(hwnd, results):
                    _, window_pid = win32process.GetWindowThreadProcessId(hwnd)
                    if window_pid == pid:
                        results.append(hwnd)

                results = []
                win32gui.EnumWindows(enum_window_callback, results)
                if results:
                    for hwnd in results:
                        win32gui.ShowWindow(hwnd, win32con.SW_HIDE)
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

            try:
                profile_xpath = '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div' 
                profile_el = wait.until(EC.element_to_be_clickable((By.XPATH, profile_xpath)))
                profile_el.click()
                logger.info("Clicked profile (best-effort)")
                time.sleep(SLEEP)
            except Exception as e:
                logger.info(f"Profile select error: {e} - ignored")

            try:
                warehouse_el = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="root"]/div/div/div[2]/div/div/div[1]/div[3]/ul/li/div[2]')
                ))
                warehouse_el.click()
                logger.info("Clicked warehouse (fallback selector)")
                time.sleep(SLEEP)
            except Exception as e:
                logger.info(f"Warehouse select error: {e} - ignored")

            # Дополнительная проверка загрузки страницы
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            logger.info("Страница загружена (body найден)")

            raw = driver.get_cookies()
            if not raw:
                logger.warning("Нет cookies после загрузки - возможно, требуется авторизация или ошибка")
                continue

            cookies = {c["name"]: c["value"] for c in raw}
            
            # Проверяем полученные cookies
            is_valid, missing_fields = validate_cookies(cookies)
            if not is_valid:
                logger.warning(f"Полученные cookies невалидны. Отсутствуют поля: {missing_fields}")
                if attempt < max_retries - 1:
                    logger.info("Повторяем попытку...")
                    time.sleep(2)  # Задержка перед повторной попыткой
                continue
            
            # Сохраняем только валидные cookies
            if save_cookies_to_file(cookies):
                logger.info("Успешно получили и сохранили валидные cookies")
                return cookies
            else:
                logger.error("Не удалось сохранить cookies")

        except Exception as e:
            logger.exception(f"get_cookies failed on attempt {attempt + 1}")
            logger.error(f"get_cookies failed: {e}")
        finally:
            try:
                driver.quit()
            except Exception:
                pass

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