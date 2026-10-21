#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
import unittest
from typing import Sequence, Union

from tests.testbase import BaseTestCase


class BarrageCompressionTestCase(BaseTestCase):
    """to_arrow fetches with pyarrow's Flight client, which is built on gRPC Core and lists gzip and deflate in its
    grpc-accept-encoding header. So the server may compress a table with either of those, and with no other type it
    includes. The client cannot see which encoding arrived, so these tests rely on that header to know what the server
    sent, and check that the data decodes."""

    ROWS = 50_000

    def _fetch(self, name: str, compression_types: Union[str, Sequence[str]]) -> None:
        self.session.run_script(
            "from deephaven import empty_table\n"
            f'{name} = empty_table({self.ROWS}).update(["I = i", "S = `sym` + (i % 32)"])'
            f".with_compression({compression_types!r})"
        )
        arrow = self.session.open_table(name).to_arrow()
        self.assertEqual(self.ROWS, arrow.num_rows)
        self.assertEqual(list(range(self.ROWS)), arrow.column("I").to_pylist())
        self.assertEqual("sym31", arrow.column("S")[31].as_py())

    def test_fetch_compressed_tables(self):
        # each table allows only one type the client decodes, so the server sends it compressed with that type
        for compression in ["gzip", "deflate"]:
            with self.subTest(compression=compression):
                self._fetch(f"compressed_{compression}", compression)

    def test_fallback_to_uncompressed(self):
        # each table allows only a type the client cannot decode, so the server must send its data uncompressed
        for compression in ["zstd", "snappy"]:
            with self.subTest(compression=compression):
                self._fetch(f"uncompressed_{compression}", compression)

    def test_first_type_the_client_accepts(self):
        # the table prefers zstd, which the client cannot decode, so the server must skip it and use gzip
        self._fetch("compressed_zstd_gzip", ["zstd", "gzip"])


if __name__ == "__main__":
    unittest.main()
