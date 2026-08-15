import os
import tempfile
import unittest

os.environ.setdefault("BASE_URL", "https://mk.kontur.ru")

from backend.kontur.api import _write_stream


class FakeResponse:
    def __init__(self, chunks, content_length=None):
        self._chunks = chunks
        self.headers = {}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def iter_content(self, chunk_size=1):
        yield from self._chunks


class DownloadStreamTests(unittest.TestCase):
    def test_write_stream_reports_byte_fraction(self):
        fractions = []
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "out.bin")
            _write_stream(
                FakeResponse([b"hello", b"world"], content_length=10),
                path,
                fractions.append,
            )
            with open(path, "rb") as handle:
                self.assertEqual(handle.read(), b"helloworld")
        self.assertEqual(fractions[0], 0.5)
        self.assertEqual(fractions[-1], 1.0)

    def test_write_stream_without_length_only_reports_done(self):
        fractions = []
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "out.bin")
            _write_stream(FakeResponse([b"ab", b"cd"]), path, fractions.append)
        self.assertEqual(fractions, [1.0])


if __name__ == "__main__":
    unittest.main()
