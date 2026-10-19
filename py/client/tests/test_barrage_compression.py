#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
import unittest

from tests.testbase import BaseTestCase


class BarrageCompressionTestCase(BaseTestCase):
    def test_fetch_compressed_tables(self):
        # to_arrow fetches with pyarrow's Flight client, which is built on gRPC Core and lists gzip and deflate in its
        # grpc-accept-encoding header. Each table allows only one type, so the server sends it compressed with that
        # type, and the client must decode it. The client cannot see which encoding arrived, so this relies on that
        # header to rule out an uncompressed response.
        for compression in ["gzip", "deflate"]:
            with self.subTest(compression=compression):
                name = f"compressed_{compression}"
                self.session.run_script(
                    "from deephaven import empty_table\n"
                    f'{name} = empty_table(50_000).update(["I = i", "S = `sym` + (i % 32)"])'
                    f'.with_compression("{compression}")'
                )
                arrow = self.session.open_table(name).to_arrow()
                self.assertEqual(50_000, arrow.num_rows)
                self.assertEqual(list(range(50_000)), arrow.column("I").to_pylist())
                self.assertEqual("sym31", arrow.column("S")[31].as_py())


if __name__ == "__main__":
    unittest.main()
