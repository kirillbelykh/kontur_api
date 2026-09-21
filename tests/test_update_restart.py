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


class OperatorLocalFilesTests(unittest.TestCase):
    def test_dirty_order_history_does_not_need_stash(self) -> None:
        from backend.services.update import local_changes_need_stash

        self.assertFalse(local_changes_need_stash(" M full_orders_history.json\n"))
        self.assertFalse(local_changes_need_stash("?? runtime/backups/history/full_orders_history-1.json\n"))

    def test_other_dirty_files_still_need_stash(self) -> None:
        from backend.services.update import local_changes_need_stash

        self.assertTrue(
            local_changes_need_stash(" M full_orders_history.json\n M backend/app/desktop.py\n")
        )

    def test_porcelain_rename_path(self) -> None:
        from backend.services.update import porcelain_path

        self.assertEqual(porcelain_path("R  old.py -> new.py"), "new.py")


class ApplyUpdateDriverSyncTests(unittest.TestCase):
    def test_apply_update_syncs_yandex_driver_before_restart(self) -> None:
        probe = {
            "update_available": True,
            "local_commit": "aaa",
            "remote_commit": "bbb",
        }
        with (
            patch("backend.services.update.probe_updates", return_value=probe),
            patch("backend.services.update._ensure_index_lock_removed"),
            patch("backend.services.update._capture_git", side_effect=["", "ccc"]),
            patch("backend.services.update._run_git"),
            patch("backend.services.update.local_changes_need_stash", return_value=False),
            patch("backend.services.update._backup_order_history", return_value=None),
            patch("backend.services.update._restore_order_history"),
            patch("backend.services.update.schedule_process_restart") as restart_mock,
            patch("backend.auth.browser.ensure_yandex_driver_updated", return_value=True) as ensure_mock,
        ):
            from backend.services.update import apply_update

            result = apply_update(
                repo_dir="/tmp/repo",
                auto_restart=True,
                pre_update_cleanup=lambda: None,
            )

        self.assertTrue(result["success"])
        self.assertTrue(result["updated"])
        ensure_mock.assert_called_once_with(force=False)
        restart_mock.assert_called_once()

    def test_apply_update_still_restarts_if_driver_sync_fails(self) -> None:
        probe = {
            "update_available": True,
            "local_commit": "aaa",
            "remote_commit": "bbb",
        }
        with (
            patch("backend.services.update.probe_updates", return_value=probe),
            patch("backend.services.update._ensure_index_lock_removed"),
            patch("backend.services.update._capture_git", side_effect=["", "ccc"]),
            patch("backend.services.update._run_git"),
            patch("backend.services.update.local_changes_need_stash", return_value=False),
            patch("backend.services.update._backup_order_history", return_value=None),
            patch("backend.services.update._restore_order_history"),
            patch("backend.services.update.schedule_process_restart") as restart_mock,
            patch("backend.auth.browser.ensure_yandex_driver_updated", return_value=False),
        ):
            from backend.services.update import apply_update

            result = apply_update(
                repo_dir="/tmp/repo",
                auto_restart=True,
                pre_update_cleanup=lambda: None,
            )

        self.assertTrue(result["success"])
        restart_mock.assert_called_once()
