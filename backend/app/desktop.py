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

from backend.app.api_bridge import ApiBridge, stop_background_workers
from backend.app.chz_bridge_server import start_chz_bridge_server
from backend.services.logger import logger

WINDOW_TITLE = "Контур Маркировка"

DWMWA_USE_IMMERSIVE_DARK_MODE_OLD = 19
DWMWA_USE_IMMERSIVE_DARK_MODE = 20
DWMWA_BORDER_COLOR = 34
DWMWA_CAPTION_COLOR = 35
DWMWA_TEXT_COLOR = 36
DWMWA_COLOR_DEFAULT = 0xFFFFFFFF


def webview_persistence_kwargs() -> dict[str, object]:
    """Keep localStorage (table columns, theme, zoom) across process restarts.

    pywebview defaults to private_mode=True, which discards the WebView2 profile
    when the window closes — so column settings looked saved until the next launch.
    """
    storage = REPO_ROOT / "runtime" / "webview"
    storage.mkdir(parents=True, exist_ok=True)
    return {"private_mode": False, "storage_path": str(storage)}


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


def _resolve_pythonw() -> str | None:
    """Path to pythonw.exe only — never python.exe (that keeps a killable console)."""
    venv_pythonw = REPO_ROOT / ".venv" / "Scripts" / "pythonw.exe"
    if venv_pythonw.exists():
        return str(venv_pythonw)
    executable = Path(sys.executable)
    if executable.name.lower() == "pythonw.exe":
        return str(executable)
    sibling = executable.with_name("pythonw.exe")
    if sibling.exists():
        return str(sibling)
    return None


def _desktop_dirs() -> list[Path]:
    dirs: list[Path] = []
    try:
        import win32com.client  # type: ignore

        special = str(win32com.client.Dispatch("WScript.Shell").SpecialFolders("Desktop") or "")
        if special:
            dirs.append(Path(special))
    except Exception:
        pass
    dirs.append(Path.home() / "Desktop")
    unique: list[Path] = []
    seen: set[str] = set()
    for path in dirs:
        key = str(path)
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _write_desktop_shortcut(shortcut_path: Path) -> None:
    icon_path = REPO_ROOT / "assets" / "icons" / "kontur.ico"
    pythonw = _resolve_pythonw()
    bat = REPO_ROOT / "scripts" / "launchers" / "KonturMarkirovka.bat"
    import win32com.client  # type: ignore

    shell = win32com.client.Dispatch("WScript.Shell")
    shortcut = shell.CreateShortCut(str(shortcut_path))
    if pythonw:
        shortcut.TargetPath = pythonw
        shortcut.Arguments = f'"{REPO_ROOT / "main.py"}"'
    elif bat.exists():
        shortcut.TargetPath = str(bat)
        shortcut.Arguments = ""
    else:
        return
    shortcut.WorkingDirectory = str(REPO_ROOT)
    shortcut.Description = "Контур Маркировка"
    if icon_path.exists():
        shortcut.IconLocation = str(icon_path)
    shortcut.Save()
    logger.debug("Ярлык на рабочем столе обновлён: %s", shortcut_path)


def _ensure_desktop_shortcut() -> None:
    try:
        for desktop in _desktop_dirs():
            _write_desktop_shortcut(desktop / "Контур Маркировка.lnk")
    except Exception:
        logger.debug("Не удалось обновить ярлык на рабочем столе", exc_info=True)


def _detach_console_if_needed() -> None:
    """Survive closing a parent cmd.exe when launched as python.exe by mistake."""
    debug_mode = os.getenv("KONTUR_UI_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    if debug_mode:
        return
    if Path(sys.executable).name.lower() == "pythonw.exe":
        return
    try:
        import ctypes

        ctypes.windll.kernel32.FreeConsole()
    except Exception:
        logger.debug("FreeConsole не удался", exc_info=True)


def colorref_from_hex(value: str | None) -> int | None:
    """CSS #RRGGBB → COLORREF 0x00BBGGRR. None, если строка не цвет."""
    raw = str(value or "").strip().lstrip("#")
    if len(raw) == 3 and all(char in "0123456789abcdefABCDEF" for char in raw):
        raw = "".join(char * 2 for char in raw)
    if len(raw) != 6 or any(char not in "0123456789abcdefABCDEF" for char in raw):
        return None
    red = int(raw[0:2], 16)
    green = int(raw[2:4], 16)
    blue = int(raw[4:6], 16)
    return red | (green << 8) | (blue << 16)


def _find_app_hwnd() -> int:
    try:
        import ctypes

        return int(ctypes.windll.user32.FindWindowW(None, WINDOW_TITLE) or 0)
    except Exception:
        return 0


def _set_dwm_attribute(hwnd: int, attribute: int, value: int) -> None:
    import ctypes

    data = ctypes.c_uint32(value)
    ctypes.windll.dwmapi.DwmSetWindowAttribute(
        ctypes.c_void_p(hwnd),
        ctypes.c_uint(attribute),
        ctypes.byref(data),
        ctypes.sizeof(data),
    )


def apply_window_chrome(*, dark: bool, caption: str | None = None, text: str | None = None) -> bool:
    """Окрасить заголовок и рамку окна в цвет темы (Windows 11 DWM)."""
    hwnd = _find_app_hwnd()
    if not hwnd:
        return False
    try:
        _set_dwm_attribute(hwnd, DWMWA_USE_IMMERSIVE_DARK_MODE, 1 if dark else 0)
        _set_dwm_attribute(hwnd, DWMWA_USE_IMMERSIVE_DARK_MODE_OLD, 1 if dark else 0)
        caption_color = colorref_from_hex(caption) if dark else None
        text_color = colorref_from_hex(text) if dark else None
        _set_dwm_attribute(hwnd, DWMWA_CAPTION_COLOR, caption_color if caption_color is not None else DWMWA_COLOR_DEFAULT)
        _set_dwm_attribute(hwnd, DWMWA_BORDER_COLOR, caption_color if caption_color is not None else DWMWA_COLOR_DEFAULT)
        _set_dwm_attribute(hwnd, DWMWA_TEXT_COLOR, text_color if text_color is not None else DWMWA_COLOR_DEFAULT)
        return True
    except Exception:
        logger.debug("Не удалось окрасить рамку окна", exc_info=True)
        return False


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
                hwnd = _find_app_hwnd()
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
    _detach_console_if_needed()
    _ensure_desktop_shortcut()
    api = ApiBridge()
    api.start_session_auto_refresh()
    try:
        start_chz_bridge_server(api)
        logger.debug("CHZ bridge для WMS запущен")
    except OSError:
        logger.exception("CHZ bridge не смог занять порт — колбэки WMS приниматься не будут")

    window = webview.create_window(
        title=WINDOW_TITLE,
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
    webview.start(debug=debug_mode, **webview_persistence_kwargs())
    stop_background_workers()


if __name__ == "__main__":
    main()
