import unittest
from datetime import date, datetime

from date_defaults import get_default_production_window


class DefaultProductionWindowTests(unittest.TestCase):
    def test_uses_first_day_of_inclusive_three_month_window(self):
        self.assertEqual(
            get_default_production_window(date(2026, 3, 13)),
            ("01-01-2026", "01-01-2031"),
        )

    def test_handles_year_boundary(self):
        self.assertEqual(
            get_default_production_window(datetime(2026, 1, 5, 10, 30)),
            ("01-11-2025", "01-11-2030"),
        )


if __name__ == "__main__":
    unittest.main()
