#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import dtypes, DHError
from deephaven import read_csv, write_csv, new_table
from deephaven.column import bool_col, byte_col, char_col, short_col, int_col, long_col, float_col, double_col
from tests.testbase import BaseTestCase


class CsvTestCase(BaseTestCase):
    def test_read_simple(self):
        t = read_csv("tests/data/small_sample.csv")
        self.assertTrue(t.columns)

    def test_read_header(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float64]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv('tests/data/test_csv.csv', header=table_header)
        self.assertEqual(col_names, t.column_names)

    def test_read_error_col_type(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.float64, dtypes.long]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(DHError) as cm:
            t = read_csv('tests/data/test_csv.csv', header=table_header)

        self.assertIsNotNone(cm.exception.compact_traceback)

    def test_read_error_quote(self):
        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float64]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        with self.assertRaises(DHError) as cm:
            t = read_csv('tests/data/test_csv.csv', header=table_header, quote=",")

        self.assertIsNotNone(cm.exception.compact_traceback)

    def test_write(self):
        t = read_csv("tests/data/small_sample.csv")
        write_csv(t, "./test_write.csv")
        t_cols = t.column_names
        t = read_csv("./test_write.csv")
        self.assertEqual(t_cols, t.column_names)

        col_names = ["Strings", "Longs", "Floats"]
        col_types = [dtypes.string, dtypes.long, dtypes.float64]
        table_header = {k: v for k, v in zip(col_names, col_types)}
        t = read_csv('tests/data/test_csv.csv', header=table_header)
        write_csv(t, "./test_write.csv", cols=col_names)
        t = read_csv('./test_write.csv')
        self.assertEqual(col_names, t.column_names)

        import os
        os.remove("./test_write.csv")

    def test_read_header_row(self):
        t = read_csv("tests/data/small_sample.csv")
        t1 = read_csv("tests/data/small_sample.csv", header_row=2)
        self.assertEqual(t.size, t1.size + 2)

        with self.assertRaises(DHError):
            t1 = read_csv("tests/data/small_sample.csv", headless=True, header_row=2)

    def test_primitive_types(self):
        actual = read_csv("tests/data/primitive_types.csv", {
            'Bool': dtypes.bool_,
            'Byte': dtypes.byte,
            'Char': dtypes.char,
            'Short': dtypes.short,
            'Int': dtypes.int32,
            'Long': dtypes.long,
            'Float': dtypes.float32,
            'Double': dtypes.double,
        })
        expected = new_table([
            bool_col('Bool', [True]),
            byte_col('Byte', [42]),
            char_col('Char', [ord('a')]),
            short_col('Short', [42]),
            int_col('Int', [42]),
            long_col('Long', [42]),
            float_col('Float', [42.42]),
            double_col('Double', [42.42])
        ])
        self.assert_table_equals(actual, expected)


if __name__ == '__main__':
    unittest.main()
