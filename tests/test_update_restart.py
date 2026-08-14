"""schedule_process_restart must arm a timer and not invoke os._exit in tests."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch


class ScheduleProcessRestartTests(unittest.TestCase):
    def test_starts_daemon_timer_without_running_callback(self) -> None:
        with patch("backend.services.update.threading.Timer") as timer_cls:
            timer = MagicMock()
            timer_cls.return_value = timer
            from backend.services.update import schedule_process_restart

            schedule_process_restart(0.4)

            timer_cls.assert_called_once()
            self.assertEqual(timer_cls.call_args[0][0], 0.4)
            self.assertTrue(timer.daemon)
            timer.start.assert_called_once()
            timer_cls.call_args[0][1]  # callback exists; do not invoke (os._exit)


if __name__ == "__main__":
    unittest.main()
