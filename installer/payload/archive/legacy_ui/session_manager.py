"""Shared Kontur HTTP session manager.

Used by the legacy CustomTkinter UI. UI v2 keeps its own runtime session
inside ``ui_v2.api_bridge``, but both paths go through ``auth.get_valid_cookies``.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

import requests

from backend.auth import get_valid_cookies
from backend.services.logger import logger
from backend.services.utils import make_session_with_cookies


class SessionManager:
    _lock = threading.Lock()
    _session: Optional[requests.Session] = None
    _last_update = 0.0
    _lifetime = 60 * 13
    _update_event = threading.Event()
    _update_thread: Optional[threading.Thread] = None
    _initialized = False

    @classmethod
    def initialize(cls) -> None:
        if not cls._initialized:
            cls._initialized = True
            cls.start_background_update()
            cls._update_event.set()

    @classmethod
    def start_background_update(cls) -> None:
        if cls._update_thread is None or not cls._update_thread.is_alive():
            cls._update_thread = threading.Thread(
                target=cls._background_update_worker,
                daemon=True,
                name="SessionUpdater",
            )
            cls._update_thread.start()

    @classmethod
    def _background_update_worker(cls) -> None:
        while True:
            try:
                update_triggered = cls._update_event.wait(timeout=cls._lifetime)
                logger.info(
                    "Сессия: фоновое обновление cookies (%s)",
                    "принудительное" if update_triggered else "плановое",
                )

                cookies = get_valid_cookies(force_refresh=True)
                if not cookies:
                    logger.warning("Сессия: новые cookies не получены, сохраняем текущую сессию")
                    cls._update_event.clear()
                    time.sleep(5)
                    continue

                new_session = make_session_with_cookies(cookies)
                with cls._lock:
                    cls._session = new_session
                    cls._last_update = time.time()

                logger.info("Сессия: cookies успешно обновлены")
                cls._update_event.clear()
            except Exception as exc:
                logger.exception("Сессия: ошибка фонового обновления cookies: %s", exc)
                time.sleep(60)

    @classmethod
    def get_session(cls) -> requests.Session:
        cls.initialize()

        with cls._lock:
            now = time.time()
            needs_refresh = cls._session is None or now - cls._last_update > cls._lifetime
            if needs_refresh:
                logger.info("Сессия: синхронно запрашиваем cookies")
                cookies = get_valid_cookies()
                if not cookies:
                    if cls._session is not None:
                        logger.warning("Сессия: cookies не обновились, используем предыдущую сессию")
                        return cls._session
                    raise RuntimeError("Не удалось получить валидные cookies для создания сессии")
                cls._session = make_session_with_cookies(cookies)
                cls._last_update = now
                cls._update_event.set()
            elif now - cls._last_update > cls._lifetime * 0.8:
                cls._update_event.set()

            assert cls._session is not None
            return cls._session

    @classmethod
    def trigger_immediate_update(cls) -> None:
        cls._update_event.set()
        logger.info("Сессия: принудительное обновление cookies запущено")

    @classmethod
    def get_session_info(cls) -> Dict[str, Any]:
        with cls._lock:
            now = time.time()
            age = now - cls._last_update if cls._last_update else 0.0
            return {
                "has_session": cls._session is not None,
                "age_seconds": age,
                "minutes_until_update": max(0.0, cls._lifetime - age) / 60.0,
            }
