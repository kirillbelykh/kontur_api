import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("LOG_FILE", str(Path(__file__).resolve().parent.parent / "lookup.log"))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

try:
    import webview
except ImportError as exc:
    raise SystemExit(
        "PyWebView not installed. Run: pip install -r ui_v2/requirements_v2.txt"
    ) from exc

from api_bridge import ApiBridge


def _resolve_pythonw() -> str:
    executable = Path(sys.executable)
    if executable.name.lower() == "pythonw.exe":
        return str(executable)
    pythonw = executable.with_name("pythonw.exe")
    if pythonw.exists():
        return str(pythonw)
    return str(executable)


def _ensure_desktop_shortcut() -> None:
    shortcut_path = Path.home() / "Desktop" / "KonturTestAPI.lnk"
    script_path = Path(__file__).resolve()
    repo_root = script_path.parent.parent
    icon_path = repo_root / "icon.ico"

    try:
        import win32com.client  # type: ignore

        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortCut(str(shortcut_path))
        shortcut.TargetPath = _resolve_pythonw()
        shortcut.Arguments = f'"{script_path}"'
        shortcut.WorkingDirectory = str(repo_root)
        shortcut.Description = "KonturTestAPI"
        if icon_path.exists():
            shortcut.IconLocation = str(icon_path)
        shortcut.Save()
    except Exception:
        pass


def _install_desktop_scroll_overrides(window: webview.Window) -> None:
    window.load_css(
        """
        body {
          overflow-y: auto !important;
          overflow-x: hidden !important;
        }

        .app-shell {
          min-height: max(100vh, var(--app-height)) !important;
          height: auto !important;
          align-items: start;
        }

        .sidebar {
          position: sticky;
          top: 0;
          align-self: start;
          min-height: var(--app-height);
        }

        .main-shell {
          grid-template-rows: auto auto 28px !important;
          overflow: visible !important;
        }

        .content-area {
          overflow: visible !important;
        }
        """
    )


def main():
    _ensure_desktop_shortcut()
    api = ApiBridge()
    index_path = Path(__file__).resolve().parent / "ui" / "index.html"
    window = webview.create_window(
        title="KonturTestAPI [TEST]",
        url=index_path.resolve().as_uri(),
        js_api=api,
        width=1440,
        height=900,
        min_size=(1100, 700),
    )
    window.events.loaded += _install_desktop_scroll_overrides
    debug_mode = os.getenv("KONTUR_UI_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    webview.start(debug=debug_mode)


if __name__ == "__main__":
    main()
