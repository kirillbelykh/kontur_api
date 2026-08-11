"""Automatic Kontur access prolongation via browser UI."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from backend.services.logger import logger

from backend.auth.browser import hide_driver_windows
from backend.auth.constants import (
    DEFAULT_PROLONGATION_INTERVAL_HOURS,
    HEADLESS,
    LEGACY_PROLONGATION_STATE_FILE,
    PROFILE_DIRECTORY,
    PROFILE_USER_DATA_DIR,
    PROLONGATION_BUTTON_XPATH,
    PROLONGATION_ENABLED_ENV,
    PROLONGATION_IDLE_CHECK_SECONDS,
    PROLONGATION_INTERVAL_HOURS_ENV,
    PROLONGATION_RETRY_DELAY_SECONDS,
    PROLONGATION_SIGN_BUTTON_XPATH,
    PROLONGATION_STARTUP_DELAY_SECONDS,
    PROLONGATION_STATE_FILE,
    PROLONGATION_URL,
    PROLONGATION_WAIT_TIMEOUT,
    YANDEX_BROWSER_PATH,
    YANDEX_DRIVER_PATH,
)
from backend.auth.browser import _click_cookie_accept_if_present

_PROLONGATION_LOCK = threading.RLock()
_PROLONGATION_THREAD: Optional[threading.Thread] = None


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
    state_file = PROLONGATION_STATE_FILE if PROLONGATION_STATE_FILE.exists() else LEGACY_PROLONGATION_STATE_FILE
    if not state_file.exists():
        return {}
    try:
        payload = json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Автопродление доступа: не удалось разобрать %s", state_file)
        return {}
    except Exception:
        logger.exception("Автопродление доступа: ошибка чтения %s", state_file)
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_prolongation_state(payload: Dict[str, Any]) -> None:
    PROLONGATION_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
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
    browser_path: Optional[Path] = YANDEX_BROWSER_PATH,
    profile_user_data_dir: Optional[Path] = PROFILE_USER_DATA_DIR,
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
        hide_driver_windows(driver)

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
