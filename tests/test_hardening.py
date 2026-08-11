"""Тесты hardening-прохода: singleton runtime, ошибки записи истории, хост CHZ-бриджа."""

import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.app import api_bridge, chz_bridge_server
from backend.services.history_db import OrderHistoryDB


class GetRuntimeSingletonTests(unittest.TestCase):
    def test_concurrent_first_calls_create_single_instance(self):
        created = []

        class SlowFakeRuntime:
            def __init__(self):
                time.sleep(0.05)  # расширяем окно гонки: без лока будет несколько экземпляров
                created.append(self)

        thread_count = 8
        barrier = threading.Barrier(thread_count)
        results = []
        results_lock = threading.Lock()

        def worker():
            barrier.wait(timeout=5)
            runtime = api_bridge._get_runtime()
            with results_lock:
                results.append(runtime)

        with patch.object(api_bridge, "_BridgeRuntime", SlowFakeRuntime), \
                patch.object(api_bridge, "_RUNTIME", None):
            threads = [threading.Thread(target=worker) for _ in range(thread_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)

        self.assertEqual(len(created), 1)
        self.assertEqual(len(results), thread_count)
        self.assertEqual(len({id(runtime) for runtime in results}), 1)


class HistoryDbAddOrderResultTests(unittest.TestCase):
    def _build_db(self, temp_dir: str) -> OrderHistoryDB:
        return OrderHistoryDB(
            db_file=str(Path(temp_dir) / "full_orders_history.json"),
            legacy_db_files=[],
            sync_enabled=False,
            startup_sync="none",
        )

    def test_add_order_returns_false_and_logs_when_write_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = self._build_db(temp_dir)
            with patch.object(db, "_save_data", side_effect=OSError("disk full")), \
                    self.assertLogs("kontur", level="ERROR") as captured:
                result = db.add_order({
                    "document_id": "DOC-FAIL-1",
                    "order_name": "broken write",
                    "status": "Ожидает",
                })

            self.assertFalse(result)
            self.assertTrue(any("DOC-FAIL-1" in message for message in captured.output))

    def test_add_order_returns_true_when_write_succeeds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = self._build_db(temp_dir)
            result = db.add_order({
                "document_id": "DOC-OK-1",
                "order_name": "ok",
                "status": "Ожидает",
            })

            self.assertTrue(result)
            self.assertIsNotNone(db.get_order_by_document_id("DOC-OK-1"))

    def test_mark_tsd_created_returns_false_for_unknown_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = self._build_db(temp_dir)
            self.assertFalse(db.mark_tsd_created("NO-SUCH-DOC", "INTRO-1"))


class ChzBridgeHostTests(unittest.TestCase):
    def test_default_host_accepts_wms_callbacks(self):
        """Дефолт — все интерфейсы: WMS шлёт заявки с другого хоста, и обновление
        не должно молча ломать существующие установки без правки .env."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("KONTUR_CHZ_BRIDGE_HOST", None)
            os.environ.pop("CHZ_BRIDGE_HOST", None)
            self.assertEqual(chz_bridge_server._bridge_host(), "0.0.0.0")

    def test_kontur_env_override_wins(self):
        with patch.dict(os.environ, {"KONTUR_CHZ_BRIDGE_HOST": "127.0.0.1"}, clear=False):
            self.assertEqual(chz_bridge_server._bridge_host(), "127.0.0.1")

    def test_legacy_env_override_still_respected(self):
        with patch.dict(os.environ, {"CHZ_BRIDGE_HOST": "192.168.1.10"}, clear=False):
            os.environ.pop("KONTUR_CHZ_BRIDGE_HOST", None)
            self.assertEqual(chz_bridge_server._bridge_host(), "192.168.1.10")


if __name__ == "__main__":
    unittest.main()
