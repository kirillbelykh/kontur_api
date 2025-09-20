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
# user-data-dir — лучше создать отдельный профиль для selenium, но можно использовать и основной
PROFILE_USER_DATA_DIR = Path(r"C:\Users\sklad\AppData\Local\Yandex\YandexBrowser\User Data\Default")
PROFILE_DIRECTORY = "Vinsent O`neal"  # имя профиля, который ты используешь
HEADLESS = False

COOKIES_FILE = Path("kontur_cookies.json")
TARGET_URL = "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses"
WAIT_TIMEOUT = 20
SLEEP = 1.0

def get_cookies(driver_path: Path = YANDEX_DRIVER_PATH,
                    browser_path: Path = YANDEX_BROWSER_PATH,
                    profile_user_data_dir: Path = PROFILE_USER_DATA_DIR,
                    profile_directory: str = PROFILE_DIRECTORY,
                    headless: bool = HEADLESS,
                    target_url: str = TARGET_URL) -> Optional[Dict[str, str]]:
    """
    Открывает браузер (Yandex/Chrome), проходит лучшие-усилия шаги:
    - нажать Accept cookies (если видимо)
    - выбрать профиль (best-effort)
    - выбрать склад (best-effort)
    Возвращает dict {name: value} cookies и сохраняет в kontur_cookies.json.
    """
    try:
        # импорт локально, чтобы файл можно было использовать без selenium, если не нужен
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
    # профиль (осторожно: не запускать одновременно с открытым браузером этим же профилем)
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

        # Кнопка Accept cookies — best-effort
        try:
            cookie_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/span/button/div[2]/span')
            ))
            cookie_btn.click()
            logging.info("Clicked cookie accept")
            time.sleep(SLEEP)
        except Exception:
            logging.info("Cookie accept not found/ignored")

        # Выбор профиля — best-effort (в некоторых случаях не требуется)
        try:
            profile_xpath = '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]/div/div/div/div/div/div/div[1]/div/div/div/div[1]/div/div'
            profile_el = wait.until(EC.element_to_be_clickable((By.XPATH, profile_xpath)))
            profile_el.click()
            logging.info("Clicked profile (best-effort)")
            time.sleep(SLEEP)
        except Exception:
            logging.info("Profile select not found/ignored")

        # Выбор склада — best-effort (fallback селектор из devtools)
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

        # Сохраняем в файл
        try:
            COOKIES_FILE.write_text(json.dumps(cookies, ensure_ascii=False, indent=2), encoding="utf-8")
            logging.info(f"Saved cookies to {COOKIES_FILE}")
        except Exception:
            logging.exception("Failed to save cookies")

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

if __name__ == "__main__":
    c = get_cookies()
    if c:
        logger.info("Cookies saved to kontur_cookies.json")
    else:
        logger.error("Failed to collect cookies.")
