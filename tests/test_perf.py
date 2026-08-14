"""Adaptive worker count stays within CPU bounds."""

from __future__ import annotations

import unittest
from unittest import mock

from backend.services.perf import adaptive_worker_count


class AdaptiveWorkerCountTests(unittest.TestCase):
    def test_uses_cpu_count_within_cap(self):
        with mock.patch("backend.services.perf.os.cpu_count", return_value=12):
            self.assertEqual(adaptive_worker_count(cap=8, floor=2), 8)

    def test_does_not_go_below_floor(self):
        with mock.patch("backend.services.perf.os.cpu_count", return_value=1):
            self.assertEqual(adaptive_worker_count(cap=8, floor=2), 2)


if __name__ == "__main__":
    unittest.main()
