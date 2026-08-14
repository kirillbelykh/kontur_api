"""Adaptive concurrency for desktop workload without extra dependencies."""

from __future__ import annotations

import os


def adaptive_worker_count(*, cap: int = 8, floor: int = 2) -> int:
    """Use spare CPU cores, keep at least one for the UI process."""
    cpus = os.cpu_count() or 4
    return max(floor, min(int(cap), max(int(floor), cpus)))
