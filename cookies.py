import json
import logging
import time
from pathlib import Path
from typing import Dict, Optional
from logger import logger

LOG_FILE = "kontur_collect.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Настройки — поправь пути под систему
YANDEX_DRIVER_PATH = Path(r"driver\yandexdriver.exe")
YANDEX_BROWSER_PATH = Path(r"C:\Users\sklad\AppData\Local\Yandex\YandexBrowser\Application\browser.exe")
PROFILE_USER_DATA_DIR = Path(r"C:\Users\sklad\AppData\Local\Yandex\YandexBrowser\User Data\Default")
PROFILE_DIRECTORY = "Vinsent O`neal"
HEADLESS = False

COOKIES_FILE = Path("kontur_cookies.json")
TARGET_URL = "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses"
WAIT_TIMEOUT = 20
SLEEP = 1.0
COOKIE_TTL = 15 * 60  # 15 минут в секундах


def load_cookies_from_file() -> Optional[Dict[str, str]]:
    """Загружает cookies из файла, проверяя возраст."""
    if not COOKIES_FILE.exists():
        return None
    try:
        data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
        cookies = data.get("cookies")
        ts = data.get("timestamp", 0)
        age = time.time() - ts
        if age > COOKIE_TTL:
            logging.info(f"Cookies устарели ({age:.0f} сек). Нужно обновить.")
            return None
        return cookies
    except Exception as e:
        logging.exception("Ошибка при чтении cookies из файла")
        logger.error("Ошибка при чтении cookies из файла:", e)
        return None


def save_cookies_to_file(cookies: Dict[str, str]) -> None:
    """Сохраняет cookies + метку времени."""
    try:
        data = {
            "timestamp": time.time(),
            "cookies": cookies
        }
        COOKIES_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logging.info(f"Cookies сохранены в {COOKIES_FILE}")
    except Exception:
        logging.exception("Ошибка при сохранении cookies")


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
        logging.exception("Selenium import failed")
        logger.error("Selenium не установлен или недоступен:", e)
        return None

    if not driver_path.exists():
        logging.error(f"Driver not found: {driver_path}")
        logger.error("Driver not found:", driver_path)
        return None
    if not browser_path.exists():
        logging.error(f"Browser binary not found: {browser_path}")
        logger.error("Browser binary not found:", browser_path)
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

    service = Service(str(driver_path))
    driver = webdriver.Chrome(service=service, options=opts)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)

    try:
        driver.get(target_url)
        time.sleep(0.8)

        # best-effort шаги
        try:
            cookie_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/span/button/div[2]/span')
            ))
            cookie_btn.click()
            logging.info("Clicked cookie accept")
            time.sleep(SLEEP)
        except Exception:
            logging.info("Cookie accept not found/ignored")

        try:
            profile_xpath = '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div/div/div[1]/div/div'
            profile_el = wait.until(EC.element_to_be_clickable((By.XPATH, profile_xpath)))
            profile_el.click()
            logging.info("Clicked profile (best-effort)")
            time.sleep(SLEEP)
        except Exception:
            logging.info("Profile select not found/ignored")

        try:
            warehouse_el = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="root"]/div/div/div[2]/div/div/div[1]/div[3]/ul/li/div[2]')
            ))
            warehouse_el.click()
            logging.info("Clicked warehouse (fallback selector)")
            time.sleep(SLEEP)
        except Exception:
            logging.info("Warehouse select not found/ignored")

        raw = driver.get_cookies()
        cookies = {c["name"]: c["value"] for c in raw}
        save_cookies_to_file(cookies)

        logger.info("Collected cookies keys:", list(cookies.keys()))
        return cookies

    except Exception as e:
        logging.exception("get_cookies failed")
        logger.error("get_cookies failed:", e)
        return None
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def get_valid_cookies() -> Optional[Dict[str, str]]:
    """Основная точка входа: возвращает валидные cookies (из файла или новые)."""
    cookies = load_cookies_from_file()
    if cookies:
        return cookies
    return get_cookies()


if __name__ == "__main__":
    c = get_valid_cookies()
    if c:
        logger.info("Cookies готовы")
    else:
        logger.error("Не удалось получить cookies")
