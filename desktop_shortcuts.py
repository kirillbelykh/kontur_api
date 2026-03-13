from __future__ import annotations

import os
from pathlib import Path

from logger import logger


def _create_windows_shortcut(shortcut_path: Path, target_path: Path, script_path: Path, icon_path: Path):
    try:
        import win32com.client  # type: ignore
    except Exception as exc:
        logger.warning(f"Не удалось импортировать win32com для создания ярлыка {shortcut_path.name}: {exc}")
        return

    shell = win32com.client.Dispatch("WScript.Shell")
    shortcut = shell.CreateShortcut(str(shortcut_path))
    shortcut.TargetPath = str(target_path)
    shortcut.Arguments = f'"{script_path}"'
    shortcut.WorkingDirectory = str(script_path.parent)

    if icon_path.exists():
        shortcut.IconLocation = str(icon_path)

    shortcut.Save()


def ensure_shortcut(shortcut_name: str, script_name: str, icon_name: str):
    if os.name != "nt":
        return

    project_dir = Path(__file__).resolve().parent
    pythonw_path = project_dir / ".venv" / "Scripts" / "pythonw.exe"
    script_path = project_dir / script_name
    desktop_dir = Path(os.path.expanduser("~/Desktop"))
    shortcut_path = desktop_dir / f"{shortcut_name}.lnk"
    icon_path = project_dir / icon_name

    if shortcut_path.exists():
        return

    if not pythonw_path.exists():
        logger.warning(f"Не найден pythonw.exe для создания ярлыка {shortcut_name}")
        return

    if not script_path.exists():
        logger.warning(f"Не найден скрипт {script_name} для создания ярлыка {shortcut_name}")
        return

    try:
        _create_windows_shortcut(shortcut_path, pythonw_path, script_path, icon_path)
        logger.info(f"Создан ярлык: {shortcut_path}")
    except Exception as exc:
        logger.warning(f"Не удалось создать ярлык {shortcut_name}: {exc}")


def ensure_kontur_test_shortcut():
    ensure_shortcut("KonturTEST", "kontur_test.pyw", "kontur.ico")
