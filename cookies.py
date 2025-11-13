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
MAX_ATTEMPTS = 3  # Максимальное количество попыток получения cookies


def validate_cookies(cookies: Dict[str, str]) -> bool:
    """
    Проверяет, что cookies содержат все обязательные поля.
    
    Args:
        cookies: Словарь с cookies
        
    Returns:
        bool: True если cookies валидны, False если нет
    """
    required_fields = ["auth.sid", "token", "auth.check"]
    
    if not cookies:
        logger.error("Cookies пусты или None")
        return False
        
    missing_fields = []
    for field in required_fields:
        if field not in cookies:
            missing_fields.append(field)
            
    if missing_fields:
        logger.error(f"В cookies отсутствуют обязательные поля: {missing_fields}")
        return False
        
    # Дополнительная проверка, что значения не пустые
    empty_fields = []
    for field in required_fields:
        if not cookies[field]:
            empty_fields.append(field)
            
    if empty_fields:
        logger.error(f"Обязательные поля cookies пусты: {empty_fields}")
        return False
        
    logger.info("Cookies прошли валидацию: все обязательные поля присутствуют")
    return True


def load_cookies_from_file() -> Optional[Dict[str, str]]:
    """Загружает cookies из файла, проверяя возраст и валидность."""
    if not COOKIES_FILE.exists():
        logger.info("Файл cookies не существует")
        return None
        
    try:
        data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        cookies = data.get("cookies")
        ts = data.get("timestamp", 0)
        age = time.time() - ts
        
        if age > COOKIE_TTL:
            logger.info(f"Cookies устарели ({age:.0f} сек). Нужно обновить.")
            return None
            
        # Проверяем валидность загруженных cookies
        if not validate_cookies(cookies):
            logger.warning("Cookies в файле невалидны. Требуется обновление.")
            return None
            
        logger.info("Успешно загружены валидные cookies из файла")
        return cookies
        
    except Exception as e:
        logger.exception("Ошибка при чтении cookies из файла")
        logger.error(f"Ошибка при чтении cookies из файла: {e}")
        return None


def save_cookies_to_file(cookies: Dict[str, str]) -> None:
    """Сохраняет cookies + метку времени."""
    try:
        data = {
            "timestamp": time.time(),
            "cookies": cookies
        }
        COOKIES_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"Cookies сохранены в {COOKIES_FILE}")
    except Exception:
        logger.exception("Ошибка при сохранении cookies")


def get_cookies(driver_path: Path = YANDEX_DRIVER_PATH,
                browser_path: Path = YANDEX_BROWSER_PATH,
                profile_user_data_dir: Path = PROFILE_USER_DATA_DIR,
                profile_directory: str = PROFILE_DIRECTORY,
                headless: bool = HEADLESS,
                target_url: str = TARGET_URL) -> Optional[Dict[str, str]]:
    """
    Получает cookies через Selenium и сохраняет их в файл с timestamp.
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
    opts.add_argument("--window-size=1920,1080")  # Устанавливаем нормальный размер, чтобы избежать маленького окна

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
            profile_xpath = '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div/div/div[1]/div/div'
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
            return None

        cookies = {c["name"]: c["value"] for c in raw}
        
        # Проверяем валидность cookies перед сохранением
        if validate_cookies(cookies):
            save_cookies_to_file(cookies)
            return cookies
        else:
            logger.warning("Полученные cookies невалидны, не сохраняем в файл")
            return None

    except Exception as e:
        logger.exception("get_cookies failed")
        logger.error(f"get_cookies failed: {str(e)}")
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def get_valid_cookies(max_attempts: int = MAX_ATTEMPTS) -> Optional[Dict[str, str]]:
    """
    Основная точка входа: возвращает валидные cookies (из файла или новые).
    
    Args:
        max_attempts: Максимальное количество попыток получения cookies
        
    Returns:
        Dict[str, str] or None: Валидные cookies или None если не удалось получить
    """
    # Пытаемся загрузить из файла
    cookies = load_cookies_from_file()
    if cookies:
        return cookies
        
    # Если в файле нет валидных cookies, пытаемся получить новые
    logger.info("Получение новых cookies...")
    
    for attempt in range(1, max_attempts + 1):
        logger.info(f"Попытка {attempt} из {max_attempts}")
        
        cookies = get_cookies()
        
        if cookies and validate_cookies(cookies):
            logger.info(f"Успешно получены валидные cookies с попытки {attempt}")
            return cookies
        else:
            logger.warning(f"Попытка {attempt} не удалась: не удалось получить валидные cookies")
            
            if attempt < max_attempts:
                retry_delay = attempt * 5  # Увеличиваем задержку с каждой попыткой
                logger.info(f"Повторная попытка через {retry_delay} секунд...")
                time.sleep(retry_delay)
    
    logger.error(f"Не удалось получить валидные cookies после {max_attempts} попыток")
    return None


if __name__ == "__main__":
    c = get_valid_cookies()
    if c:
        logger.info("Cookies готовы")
    else:
        logger.error("Не удалось получить cookies")