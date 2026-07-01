from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("LOG_FILE", str(Path(__file__).resolve().parent.parent / "runtime" / "logs" / "lookup.log"))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from api_bridge import ApiBridge
from chz_bridge_server import start_chz_bridge_server
from ui_mobile.server_mobile import start_mobile_servers, stop_mobile_servers

IS_WINDOWS = sys.platform.startswith("win")

if IS_WINDOWS:
    try:
        import winerror
        import win32api
        import win32con
        import win32event
        import win32gui
    except ImportError:
        IS_WINDOWS = False


class WindowsTrayController:
    WINDOW_CLASS_NAME = "KonturApiCrptServerTrayWindow"
    MUTEX_NAME = "Global\\KonturApiCrptServerSingleInstance"
    MENU_EXIT = 1001
    TRAY_UID = 1
    WM_TRAY_ICON = 0x0400 + 1

    def __init__(self, on_exit: Callable[[], None]) -> None:
        self._on_exit = on_exit
        self._mutex = None
        self._hwnd = None
        self._icon_handle = None
        self._instance_handle = None
        self._installed = False

    def acquire_single_instance(self) -> bool:
        if not IS_WINDOWS:
            return True

        self._mutex = win32event.CreateMutex(None, False, self.MUTEX_NAME)
        return win32api.GetLastError() != winerror.ERROR_ALREADY_EXISTS

    def install(self) -> None:
        if not IS_WINDOWS:
            return

        self._instance_handle = win32api.GetModuleHandle(None)
        self._register_window_class()
        self._hwnd = win32gui.CreateWindow(
            self.WINDOW_CLASS_NAME,
            "CRPT server",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            self._instance_handle,
            None,
        )
        self._icon_handle = self._load_icon()
        notify_id = (
            self._hwnd,
            self.TRAY_UID,
            win32gui.NIF_ICON | win32gui.NIF_MESSAGE | win32gui.NIF_TIP,
            self.WM_TRAY_ICON,
            self._icon_handle,
            "CRPT server",
        )
        win32gui.Shell_NotifyIcon(win32gui.NIM_ADD, notify_id)
        self._installed = True

    def pump(self) -> None:
        if IS_WINDOWS and self._installed:
            win32gui.PumpWaitingMessages()

    def close(self) -> None:
        if not IS_WINDOWS:
            return

        if self._installed and self._hwnd:
            try:
                win32gui.Shell_NotifyIcon(
                    win32gui.NIM_DELETE,
                    (self._hwnd, self.TRAY_UID),
                )
            except win32gui.error:
                pass
            self._installed = False

        if self._hwnd:
            try:
                win32gui.DestroyWindow(self._hwnd)
            except win32gui.error:
                pass
            self._hwnd = None

        if self._icon_handle:
            try:
                win32gui.DestroyIcon(self._icon_handle)
            except win32gui.error:
                pass
            self._icon_handle = None

        self._mutex = None

    def _register_window_class(self) -> None:
        message_map = {
            self.WM_TRAY_ICON: self._on_tray_message,
            win32con.WM_COMMAND: self._on_command,
            win32con.WM_CLOSE: self._on_close,
            win32con.WM_DESTROY: self._on_destroy,
        }

        window_class = win32gui.WNDCLASS()
        window_class.hInstance = self._instance_handle
        window_class.lpszClassName = self.WINDOW_CLASS_NAME
        window_class.lpfnWndProc = message_map

        try:
            win32gui.RegisterClass(window_class)
        except win32gui.error as exc:
            if exc.winerror != winerror.ERROR_CLASS_ALREADY_EXISTS:
                raise

    def _load_icon(self):  # type: ignore[no-untyped-def]
        icon_candidates = [
            Path(__file__).resolve().parent.parent / "assets" / "icons" / "icon.ico",
            Path(__file__).resolve().parent.parent / "icon.ico",
        ]

        for icon_path in icon_candidates:
            if icon_path.exists():
                return win32gui.LoadImage(
                    0,
                    str(icon_path),
                    win32con.IMAGE_ICON,
                    0,
                    0,
                    win32con.LR_LOADFROMFILE | win32con.LR_DEFAULTSIZE,
                )

        return win32gui.LoadIcon(0, win32con.IDI_APPLICATION)

    def _on_tray_message(self, hwnd, msg, wparam, lparam):  # type: ignore[no-untyped-def]
        if lparam in (
            win32con.WM_CONTEXTMENU,
            win32con.WM_RBUTTONUP,
            win32con.WM_LBUTTONUP,
            win32con.WM_LBUTTONDBLCLK,
        ):
            self._show_context_menu()
        return 0

    def _on_command(self, hwnd, msg, wparam, lparam):  # type: ignore[no-untyped-def]
        command_id = win32api.LOWORD(wparam)
        if command_id == self.MENU_EXIT:
            self._on_exit()
        return 0

    def _on_close(self, hwnd, msg, wparam, lparam):  # type: ignore[no-untyped-def]
        self._on_exit()
        return 0

    def _on_destroy(self, hwnd, msg, wparam, lparam):  # type: ignore[no-untyped-def]
        return 0

    def _show_context_menu(self) -> None:
        if not self._hwnd:
            return

        menu = win32gui.CreatePopupMenu()
        try:
            win32gui.AppendMenu(menu, win32con.MF_STRING, self.MENU_EXIT, "Закрыть CRPT server")
            x, y = win32gui.GetCursorPos()
            win32gui.SetForegroundWindow(self._hwnd)
            win32gui.TrackPopupMenu(
                menu,
                win32con.TPM_LEFTALIGN | win32con.TPM_RIGHTBUTTON,
                x,
                y,
                0,
                self._hwnd,
                None,
            )
            win32gui.PostMessage(self._hwnd, win32con.WM_NULL, 0, 0)
        finally:
            win32gui.DestroyMenu(menu)


def _env_flag(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw_value = str(os.getenv(name) or "").strip()
    if not raw_value:
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        return default
    return parsed if 1 <= parsed <= 65535 else default


def main() -> None:
    api = ApiBridge()
    api.start_session_auto_refresh()

    bridge_server = None
    try:
        bridge_server = start_chz_bridge_server(api)
    except OSError:
        bridge_server = None

    mobile_bundle = None
    if _env_flag("WMS_EMBED_SERVER_ENABLED", True):
        try:
            mobile_bundle = start_mobile_servers(
                api,
                host=str(os.getenv("WMS_EMBED_SERVER_HOST") or "127.0.0.1").strip() or "127.0.0.1",
                port=_env_int("WMS_EMBED_SERVER_PORT", 8787),
                https_port=_env_int("WMS_EMBED_SERVER_HTTPS_PORT", 8788),
                enable_https=_env_flag("WMS_EMBED_SERVER_HTTPS_ENABLED", False),
            )
        except OSError:
            mobile_bundle = None

    shutdown_requested = False

    def request_shutdown() -> None:
        nonlocal shutdown_requested
        shutdown_requested = True

    tray = WindowsTrayController(on_exit=request_shutdown) if IS_WINDOWS else None
    if tray is not None:
        if not tray.acquire_single_instance():
            return
        tray.install()

    def _handle_signal(_signum, _frame) -> None:
        request_shutdown()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(signum, _handle_signal)
        except ValueError:
            continue

    try:
        while not shutdown_requested:
            if tray is not None:
                tray.pump()
            time.sleep(0.2)
    finally:
        if tray is not None:
            tray.close()
        if mobile_bundle is not None:
            stop_mobile_servers(mobile_bundle)
        if bridge_server is not None:
            bridge_server.shutdown()
            bridge_server.server_close()


if __name__ == "__main__":
    main()
