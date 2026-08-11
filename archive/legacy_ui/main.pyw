"""Windows GUI entry point without a console window.

Delegates to ``main.py`` so the legacy UI has a single source of truth.
"""

from __future__ import annotations

import runpy
from pathlib import Path

runpy.run_path(str(Path(__file__).resolve().with_name("main.py")), run_name="__main__")
