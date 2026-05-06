import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

import bartender_print


def _encode_value(value: str) -> str:
    import base64

    return base64.b64encode(str(value).encode("utf-16le")).decode("ascii")


def _make_object_xml(*values: str, object_name: str, object_type: str = bartender_print.TEXT_OBJECT_TYPE) -> ET.Element:
    object_element = ET.Element("Object", Name=object_name, Type=object_type)
    for value in values:
        substring = ET.SubElement(object_element, "SubString")
        value_node = ET.SubElement(substring, "Value")
        value_node.text = _encode_value(value)
    return object_element


class BarTenderPrintTests(unittest.TestCase):
    def test_build_print_context_keeps_selected_record_number_for_single_record(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "codes.csv"
            csv_path.write_text(
                "0104650118041257215i+AhL)l-0Ny-\t04650118041257\tTest\n",
                encoding="utf-8-sig",
            )

            context = bartender_print.build_print_context(
                order_name="787 С…РёСЂ 8,0 260319 36Рє РїРѕ 50",
                document_id="doc-1",
                csv_path=str(csv_path),
                printer_name="Printer",
                selected_record_number=69,
            )

        self.assertEqual(context.selected_record_number, 69)
        self.assertEqual(context.label_count, 1)

    def test_build_print_context_uses_none_when_record_number_not_provided(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "codes.csv"
            csv_path.write_text(
                "010000000000000021ABC\t04650118041257\tTest 1\n"
                "010000000000000021DEF\t04650118041257\tTest 2\n",
                encoding="utf-8-sig",
            )

            context = bartender_print.build_print_context(
                order_name="787 С…РёСЂ 8,0 260319 36Рє РїРѕ 50",
                document_id="doc-1",
                csv_path=str(csv_path),
                printer_name="Printer",
            )

        self.assertIsNone(context.selected_record_number)

    def test_configure_template_objects_keeps_size_and_selected_number_separate(self):
        root = ET.Element("Root")
        size_object = _make_object_xml("M", object_name="Text 2")
        serial_text_object = _make_object_xml("11", object_name="Text 1")
        serial_source_object = _make_object_xml("1", object_name="Serial Numbers 1", object_type=bartender_print.SERIAL_OBJECT_TYPE)
        copies_object = _make_object_xml("2", object_name="Copies 1", object_type=bartender_print.COPIES_OBJECT_TYPE)
        root.extend([size_object, serial_text_object, serial_source_object, copies_object])

        class FakeObjects:
            def __init__(self, source_root: ET.Element):
                self.ExportDataSourceValuesToXML = ET.tostring(source_root, encoding="unicode")
                self.imported_xml = None

            def ImportDataSourceValuesFromXML(self, xml_text: str) -> None:
                self.imported_xml = xml_text

        fake_format = type("FakeFormat", (), {"Objects": FakeObjects(root)})()

        bartender_print._configure_template_objects(
            fake_format,
            "L",
            selected_record_number=69,
        )

        imported_root = ET.fromstring(fake_format.Objects.imported_xml)
        imported_objects = list(imported_root.findall(".//Object"))
        self.assertEqual(bartender_print._read_object_value(imported_objects[0]), "L")
        self.assertEqual(bartender_print._read_object_value(imported_objects[1]), "69")
        self.assertEqual(bartender_print._read_object_value(imported_objects[2]), "1")
        self.assertEqual(bartender_print._read_object_value(imported_objects[3]), "1")


if __name__ == "__main__":
    unittest.main()
