"""Цвет обязателен для латекс HR и должен совпадать с номенклатурой."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from backend.services import options
from backend.services.get_gtin import lookup_gtin


class OptionsColorRequiredTests(unittest.TestCase):
    def test_simplified_options_match_nomenclature_casing_for_hr(self) -> None:
        self.assertIn("латекс HR", options.simplified_options)
        self.assertIn("нитрил диаг HR короткий", options.simplified_options)
        self.assertIn("нитрил диаг HR удлиненный", options.simplified_options)
        self.assertNotIn("латекс hr", options.simplified_options)

    def test_color_required_includes_latex_hr_case_insensitively(self) -> None:
        self.assertTrue(options.product_requires_color("латекс HR"))
        self.assertTrue(options.product_requires_color("латекс hr"))
        self.assertTrue(options.product_requires_color(" Латекс HR "))
        self.assertFalse(options.product_requires_color("стер латекс"))

    def test_color_required_entries_exist_in_simplified_options(self) -> None:
        simplified = {item.lower() for item in options.simplified_options}
        for name in options.color_required:
            self.assertIn(
                name.lower(),
                simplified,
                f"{name!r} есть в color_required, но нет в simplified_options",
            )

    def test_lookup_without_color_picks_natural_first_for_latex_hr(self) -> None:
        """Регрессия: без фильтра по цвету lookup возвращает натуральный — UI обязан требовать цвет."""
        path = Path("data/nomenclature.xlsx")
        if not path.exists():
            self.skipTest("nomenclature.xlsx отсутствует")
        df = pd.read_excel(path)
        gtin, full_name = lookup_gtin(df, "латекс HR", "M", "25", color=None)
        self.assertIsNotNone(gtin)
        self.assertIn("натуральный", (full_name or "").lower())

    def test_lookup_with_blue_returns_blue_latex_hr(self) -> None:
        path = Path("data/nomenclature.xlsx")
        if not path.exists():
            self.skipTest("nomenclature.xlsx отсутствует")
        df = pd.read_excel(path)
        gtin, full_name = lookup_gtin(df, "латекс hr", "M", "25", color="синий")
        self.assertIsNotNone(gtin)
        self.assertIn("синий", (full_name or "").lower())
        self.assertNotIn("натуральный", (full_name or "").lower())


class ApiBridgeRequiresColorTests(unittest.TestCase):
    def test_build_lookup_payload_rejects_missing_color_for_latex_hr(self) -> None:
        from backend.app.api_bridge import ApiBridge

        bridge = ApiBridge.__new__(ApiBridge)
        with self.assertRaisesRegex(RuntimeError, "цвет"):
            bridge._build_lookup_payload(
                name="латекс hr",
                size="M",
                units_per_pack="100",
                color="",
            )

    def test_build_lookup_payload_accepts_blue_latex_hr(self) -> None:
        from backend.app.api_bridge import ApiBridge

        bridge = ApiBridge.__new__(ApiBridge)
        with mock.patch.object(
            bridge,
            "_load_nomenclature_df",
            return_value=pd.DataFrame(
                {
                    "GTIN": ["4650118040533"],
                    "Полное наименование товара": ["Перчатки латекс HR M синий"],
                    "Упрощенно": ["латекс HR"],
                    "Размер": ["СРЕДНИЙ (M)"],
                    "Количество единиц употребления в потребительской упаковке": [25],
                    "Цвет": ["синий"],
                    "венчик": [""],
                }
            ),
        ), mock.patch("backend.app.api_bridge.get_tnved_code", return_value="4015120000"):
            result = bridge._build_lookup_payload(
                name="латекс HR",
                size="M",
                units_per_pack="25",
                color="синий",
            )
        self.assertEqual(result["gtin"], "4650118040533")


if __name__ == "__main__":
    unittest.main()
