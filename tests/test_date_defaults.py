import unittest
from datetime import date

from backend.services.date_defaults import get_default_production_window


class DefaultProductionWindowTests(unittest.TestCase):
    def test_returns_owner_fixed_defaults(self):
        self.assertEqual(
            get_default_production_window(),
            ("03-03-2026", "03-03-2031"),
        )

    def test_reference_date_is_ignored(self):
        self.assertEqual(
            get_default_production_window(date(2027, 12, 31)),
            ("03-03-2026", "03-03-2031"),
        )


if __name__ == "__main__":
    unittest.main()
