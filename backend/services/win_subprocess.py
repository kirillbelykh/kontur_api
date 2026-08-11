"""Общие kwargs для subprocess: скрыть консольное окно дочернего процесса на Windows."""

from __future__ import annotations

import os
import subprocess
from typing import Any, Dict


def hidden_console_kwargs() -> Dict[str, Any]:
    """kwargs для subprocess.run/Popen, чтобы дочерний процесс не мигал консолью.

    На не-Windows возвращает пустой словарь (startupinfo/creationflags там запрещены).
    """
    kwargs: Dict[str, Any] = {}
    if os.name != "nt":
        return kwargs

    startupinfo_cls = getattr(subprocess, "STARTUPINFO", None)
    if startupinfo_cls is not None:
        startupinfo = startupinfo_cls()
        startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
        startupinfo.wShowWindow = 0  # SW_HIDE
        kwargs["startupinfo"] = startupinfo

    create_no_window = getattr(subprocess, "CREATE_NO_WINDOW", None)
    if create_no_window is not None:
        kwargs["creationflags"] = int(create_no_window)
    return kwargs
