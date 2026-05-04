import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

import bartender_label_100x180 as labels


def _make_context(**overrides):
    payload = {
        "document_id": "doc-1",
        "order_name": "646 стер лат L 260407 120к",
        "template_path": "template.btw",
        "aggregation_csv_path": "codes.csv",
        "printer_name": "Printer",
        "data_source_kind": labels.MARKING_SOURCE_KIND,
        "template_category": "стерилка",
        "label_count": 1,
        "gtin": "0465011804000000",
        "size": "L",
        "batch": "260407",
        "color": "",
        "manufacture_date": "2026-02",
        "expiration_date": "2031-02",
        "quantity_pairs": 100,
        "quantity_pairs_word": "пар",
        "units_per_pack": 100,
        "dispenser_count": 0,
        "package_text": None,
    }
    payload.update(overrides)
    return labels.LabelPrint100x180Context(**payload)


def _make_object_xml(*values: str, object_name: str, object_type: str = labels.TEXT_OBJECT_TYPE) -> ET.Element:
    object_element = ET.Element("Object", Name=object_name, Type=object_type)
    for value in values:
        substring = ET.SubElement(object_element, "SubString")
        value_node = ET.SubElement(substring, "Value")
        value_node.text = labels._encode_value(value)
    return object_element


class BarTenderLabel100x180Tests(unittest.TestCase):
    def test_resolve_order_metadata_ignores_nan_optional_values(self):
        df = pd.DataFrame(
            [
                {
                    labels.GTIN_COLUMN: "0465011804000000",
                    labels.UNITS_COLUMN: 100,
                    labels.SIZE_COLUMN: "L",
                    labels.COLOR_COLUMN: float("nan"),
                    labels.FULL_NAME_COLUMN: float("nan"),
                    labels.SIMPL_COLUMN: float("nan"),
                }
            ]
        )
        order_data = {
            "document_id": "doc-1",
            "order_name": "646 стер лат L 260407 120к",
            "gtin": "0465011804000000",
            "positions": [{"name": "Перчатки Stera", "quantity": 120}],
        }

        metadata = labels.resolve_order_metadata(order_data, df)

        self.assertEqual(metadata.color, "")
        self.assertEqual(metadata.full_name, "Перчатки Stera")
        self.assertEqual(metadata.simpl_name, "")

    def test_replace_preserving_linebreak_keeps_size_padding(self):
        original = "                M\r"
        updated = labels._replace_preserving_linebreak(original, "L")
        self.assertEqual(updated, "                L\r")

    def test_replace_quantity_value_keeps_number_when_quantity_is_in_adjacent_value(self):
        values = ["Количество               ", "100пар"]

        updated = labels._replace_quantity_value(values, 0, _make_context(quantity_pairs=104))

        self.assertTrue(updated)
        self.assertEqual(values[1], "104 пар")

    def test_replace_quantity_value_keeps_package_suffix_when_number_is_inline(self):
        values = ["Количество                500 ", "пар\r   (10 диспенсеров по 50 пар)"]

        updated = labels._replace_quantity_value(
            values,
            0,
            _make_context(
                data_source_kind=labels.AGGREGATION_SOURCE_KIND,
                quantity_pairs=200,
                package_text="(4 диспенсера по 50 пар)",
            ),
        )

        self.assertTrue(updated)
        self.assertIn("200", values[0])
        self.assertEqual(values[1], "пар\r   (4 диспенсера по 50 пар)")

    def test_update_description_object_removes_color_line_when_value_is_empty(self):
        description_object = _make_object_xml(
            "-Диагностические перчатки\r-Цвет: nan\r-Манжета: с венчиком",
            object_name="Текст 1",
        )

        labels._update_description_object(description_object, "")

        updated_value = "".join(labels._get_substring_values(description_object))
        self.assertNotIn("Цвет:", updated_value)
        self.assertIn("Манжета: с венчиком", updated_value)

    def test_find_optional_serial_text_object_resets_visible_number_with_zero_padding(self):
        details_object = _make_object_xml(
            "Размер",
            "                L\r",
            "Партия                       260407\rКоличество               100 пар",
            object_name="Текст 13",
        )
        serial_text_object = _make_object_xml("081", object_name="Text 1")
        root = ET.Element("Root")
        root.extend(
            [
                details_object,
                serial_text_object,
                _make_object_xml("1", object_name="Копии 1", object_type=labels.COPIES_OBJECT_TYPE),
            ]
        )

        found_object = labels._find_optional_serial_text_object(
            list(root.findall("./Object")),
            excluded_objects=(details_object,),
        )

        self.assertIs(found_object, serial_text_object)
        labels._reset_serial_text_object(found_object)
        self.assertEqual(labels._get_substring_values(found_object)[0], "001")

    def test_ensure_unique_label_values_raises_for_duplicate_marking_codes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "codes.csv"
            csv_path.write_text(
                "010000000000000021ABC\t0465011804000000\tТовар 1\n"
                "010000000000000021ABC\t0465011804000000\tТовар 1\n",
                encoding="utf-8-sig",
            )

            with self.assertRaises(labels.BarTenderLabel100x180Error) as error_context:
                labels._ensure_unique_label_values(csv_path, labels.MARKING_SOURCE_KIND)

        self.assertIn("дублирующиеся коды маркировки", str(error_context.exception))


if __name__ == "__main__":
    unittest.main()
