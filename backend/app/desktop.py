"""Desktop PyWebView entry — loads the React frontend."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("LOG_FILE", str(REPO_ROOT / "runtime" / "logs" / "lookup.log"))
load_dotenv(REPO_ROOT / ".env")

try:
    import webview
except ImportError as exc:
    raise SystemExit("PyWebView not installed. Run: uv sync") from exc

from backend.app.api_bridge import ApiBridge
from backend.app.chz_bridge_server import start_chz_bridge_server
from backend.services.logger import logger


def _resolve_frontend_url() -> str:
    """Prefer Vite dev server when set; otherwise load production build."""
    dev_url = str(os.getenv("VITE_DEV_URL") or "").strip()
    if dev_url:
        return dev_url
    index_path = REPO_ROOT / "frontend" / "dist" / "index.html"
    if index_path.exists():
        return index_path.resolve().as_uri()
    # Fallback: archived static UI while React dist is building
    legacy = REPO_ROOT / "archive" / "ui_v2_static" / "ui" / "index.html"
    if legacy.exists():
        return legacy.resolve().as_uri()
    message = (
        f"Frontend not found: {index_path} is missing. "
        "Run `cd frontend && npm install && npm run build` (or `git pull` to fetch the committed dist), "
        "or set VITE_DEV_URL=http://127.0.0.1:5173 in .env while developing."
    )
    # Под pythonw консоли нет — продублируем причину в lookup.log, иначе оператор не увидит её.
    logger.error(message)
    raise SystemExit(message)


def _resolve_pythonw() -> str:
    executable = Path(sys.executable)
    if executable.name.lower() == "pythonw.exe":
        return str(executable)
    pythonw = executable.with_name("pythonw.exe")
    if pythonw.exists():
        return str(pythonw)
    return str(executable)


def _ensure_desktop_shortcut() -> None:
    shortcut_path = Path.home() / "Desktop" / "Контур Маркировка.lnk"
    icon_path = REPO_ROOT / "assets" / "icons" / "kontur.ico"
    try:
        import win32com.client  # type: ignore

        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortCut(str(shortcut_path))
        shortcut.TargetPath = _resolve_pythonw()
        shortcut.Arguments = f'"{REPO_ROOT / "main.py"}"'
        shortcut.WorkingDirectory = str(REPO_ROOT)
        shortcut.Description = "Контур Маркировка"
        if icon_path.exists():
            shortcut.IconLocation = str(icon_path)
        shortcut.Save()
        logger.debug("Ярлык на рабочем столе обновлён: %s", shortcut_path)
    except Exception:
        logger.debug("Не удалось обновить ярлык %s", shortcut_path, exc_info=True)


def _apply_window_icon() -> None:
    """The window is owned by pythonw.exe, so Windows shows the Python icon.
    Load kontur.ico and set it via WM_SETICON once the window exists."""
    import ctypes
    import threading
    import time

    icon_path = REPO_ROOT / "assets" / "icons" / "kontur.ico"
    if not icon_path.exists():
        return

    def worker() -> None:
        try:
            user32 = ctypes.windll.user32
            hwnd = 0
            for _ in range(100):
                hwnd = user32.FindWindowW(None, "Контур Маркировка")
                if hwnd:
                    break
                time.sleep(0.1)
            if not hwnd:
                logger.debug("Окно приложения не найдено за 10с — иконка окна не установлена")
                return
            IMAGE_ICON, LR_LOADFROMFILE, WM_SETICON = 1, 0x10, 0x80
            big = user32.LoadImageW(None, str(icon_path), IMAGE_ICON, 0, 0, LR_LOADFROMFILE)
            small = user32.LoadImageW(None, str(icon_path), IMAGE_ICON, 16, 16, LR_LOADFROMFILE)
            if big:
                user32.SendMessageW(hwnd, WM_SETICON, 1, big)
            if small:
                user32.SendMessageW(hwnd, WM_SETICON, 0, small)
            logger.debug("Иконка окна установлена (big=%s, small=%s)", bool(big), bool(small))
        except Exception:
            logger.debug("Не удалось установить иконку окна", exc_info=True)

    threading.Thread(target=worker, daemon=True).start()


def main() -> None:
    try:
        import ctypes

        # Own taskbar identity: without this the app groups under pythonw with its icon
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("Grundlage.KonturMarkirovka")
        logger.debug("AppUserModelID установлен")
    except Exception:
        logger.debug("Не удалось установить AppUserModelID", exc_info=True)
    _ensure_desktop_shortcut()
    api = ApiBridge()
    api.start_session_auto_refresh()
    try:
        start_chz_bridge_server(api)
        logger.debug("CHZ bridge для WMS запущен")
    except OSError:
        logger.exception("CHZ bridge не смог занять порт — колбэки WMS приниматься не будут")

    window = webview.create_window(
        title="Контур Маркировка",
        url=_resolve_frontend_url(),
        js_api=api,
        width=1440,
        height=900,
        min_size=(1100, 700),
    )
    # Ensure the worker thread exists after the window loads, but do not
    # fire a second forced Selenium pass (startup already triggered one).
    window.events.loaded += lambda *_: api.start_session_auto_refresh(trigger_now=False)
    _apply_window_icon()
    debug_mode = os.getenv("KONTUR_UI_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    webview.start(debug=debug_mode)


if __name__ == "__main__":
    main()
