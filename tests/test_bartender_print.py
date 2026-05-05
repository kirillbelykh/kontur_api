import tempfile
import unittest
from pathlib import Path

import bartender_print


class BarTenderPrintTests(unittest.TestCase):
    def test_build_print_context_uses_marking_fragment_for_single_record(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "codes.csv"
            csv_path.write_text(
                "0104650118041257215i+AhL)l-0Ny-\t04650118041257\tTest\n",
                encoding="utf-8-sig",
            )

            context = bartender_print.build_print_context(
                order_name="787 хир 8,0 260319 36к по 50",
                document_id="doc-1",
                csv_path=str(csv_path),
                printer_name="Printer",
            )

        self.assertEqual(context.marking_fragment, "L)l-0Ny-")

    def test_build_print_context_skips_marking_fragment_for_multi_record_print(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "codes.csv"
            csv_path.write_text(
                "010000000000000021ABC\t04650118041257\tTest 1\n"
                "010000000000000021DEF\t04650118041257\tTest 2\n",
                encoding="utf-8-sig",
            )

            context = bartender_print.build_print_context(
                order_name="787 хир 8,0 260319 36к по 50",
                document_id="doc-1",
                csv_path=str(csv_path),
                printer_name="Printer",
            )

        self.assertEqual(context.marking_fragment, "")


if __name__ == "__main__":
    unittest.main()
