#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2 import dtypes, DHError
from deephaven2 import read_csv, write_csv
from tests.testbase import BaseTestCase


class CsvTestCase(BaseTestCase):
    def test_read_simple(self):
        t = read_csv("tests/data/small_sample.csv")

        self.assertTrue(t.columns)

    def test_read_header(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float_]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv('tests/data/test_csv.csv', header=table_header)
        t_col_names = [col.name for col in t.columns]
        self.assertEqual(col_names, t_col_names)

    def test_read_error_col_type(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.float_, dtypes.long]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(DHError) as cm:
            t = read_csv('tests/data/test_csv.csv', header=table_header)

        self.assertIsNotNone(cm.exception.compact_traceback)

    def test_read_error_quote(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float_]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(DHError) as cm:
            t = read_csv('tests/data/test_csv.csv', header=table_header, quote=",")

        self.assertIsNotNone(cm.exception.compact_traceback)

    def test_write(self):
        t = read_csv("tests/data/small_sample.csv")
        write_csv(t, "./test_write.csv")
        t_cols = [col.name for col in t.columns]
        t = read_csv("./test_write.csv")
        self.assertEqual(t_cols, [col.name for col in t.columns])

        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float_]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv('tests/data/test_csv.csv', header=table_header)
        write_csv(t, "./test_write.csv", cols=col_names)
        t = read_csv('./test_write.csv')
        self.assertEqual(col_names, [c.name for c in t.columns])

        import os
        os.remove("./test_write.csv")


if __name__ == '__main__':
    unittest.main()
