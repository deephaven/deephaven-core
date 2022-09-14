#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import time
import unittest
from dataclasses import dataclass

from deephaven import DHError, dtypes, new_table
from deephaven import empty_table
from deephaven.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, jobj_col, ColumnType
from deephaven.constants import MAX_BYTE, MAX_SHORT, MAX_INT, MAX_LONG
from deephaven.jcompat import j_array_list
from tests.testbase import BaseTestCase


class ColumnTestCase(BaseTestCase):

    def test_column_type(self):
        normal_type = ColumnType.NORMAL.value
        self.assertEqual(ColumnType.NORMAL, ColumnType(normal_type))

    def test_column_error(self):
        jobj = j_array_list([1, -1])
        with self.assertRaises(DHError) as cm:
            bool_input_col = bool_col(name="Boolean", data=[True, 'abc'])

        self.assertNotIn("bool_input_col", dir())

        with self.assertRaises(DHError) as cm:
            _ = byte_col(name="Byte", data=[1, 'abc'])

        with self.assertRaises(DHError) as cm:
            _ = char_col(name="Char", data=[jobj])

        with self.assertRaises(DHError) as cm:
            _ = short_col(name="Short", data=[1, 'abc'])

        with self.assertRaises(DHError) as cm:
            _ = int_col(name="Int", data=[1, [1, 2]])

        with self.assertRaises(DHError) as cm:
            _ = long_col(name="Long", data=[1, float('inf')])

        with self.assertRaises(DHError) as cm:
            _ = float_col(name="Float", data=[1.01, 'NaN'])

        with self.assertRaises(DHError) as cm:
            _ = double_col(name="Double", data=[1.01, jobj])

        with self.assertRaises(DHError) as cm:
            _ = string_col(name="String", data=[1, -1.01])

        with self.assertRaises(DHError) as cm:
            _ = datetime_col(name="Datetime", data=[dtypes.DateTime(round(time.time())), False])

        with self.assertRaises(DHError) as cm:
            _ = jobj_col(name="JObj", data=[jobj, CustomClass(-1, "-1")])

    def test_array_column(self):
        strings = ["Str1", "Str1", "Str2", "Str2"]
        doubles = [1.0, 2.0, 4.0, 8.0]
        test_table = new_table([
            string_col("StringColumn", strings),
            double_col("Decimals", doubles)
        ]
        )

        test_table = test_table.group_by(["StringColumn"])

        self.assertIsNone(test_table.columns[0].component_type)
        self.assertEqual(test_table.columns[1].component_type, dtypes.double)

    def test_numeric_columns(self):
        x = [MAX_BYTE, MAX_SHORT, MAX_INT, MAX_LONG, 0.98888, 999999.888888]
        n = len(x)

        def get_x(i):
            return x[i]

        t_list = empty_table(n).update(["X = x[i]"])
        t_func = empty_table(n).update(["X = get_x(i)"])
        # We want to test that casting on both PyObject and JObject works as expected.
        self.assertEqual(t_list.columns[0].data_type, dtypes.PyObject)
        self.assertEqual(t_func.columns[0].data_type, dtypes.JObject)

        t_list_integers = t_list.update(
            ["A = (byte)X", "B = (short)X", "C = (int)X", "D = (long)X", "E = (float)X", "F = (double)X"])
        self.assertEqual(t_list_integers.columns[1].data_type, dtypes.byte)
        self.assertEqual(t_list_integers.columns[2].data_type, dtypes.short)
        self.assertEqual(t_list_integers.columns[3].data_type, dtypes.int32)
        self.assertEqual(t_list_integers.columns[4].data_type, dtypes.long)
        self.assertEqual(t_list_integers.columns[5].data_type, dtypes.float32)
        self.assertEqual(t_list_integers.columns[6].data_type, dtypes.double)

        t_func_integers = t_func.update(
            ["A = (byte)X", "B = (short)X", "C = (int)X", "D = (long)X", "E = (float)X", "F = (double)X"])
        self.assertEqual(t_func_integers.columns[1].data_type, dtypes.byte)
        self.assertEqual(t_func_integers.columns[2].data_type, dtypes.short)
        self.assertEqual(t_func_integers.columns[3].data_type, dtypes.int32)
        self.assertEqual(t_func_integers.columns[4].data_type, dtypes.long)
        self.assertEqual(t_list_integers.columns[5].data_type, dtypes.float32)
        self.assertEqual(t_list_integers.columns[6].data_type, dtypes.float64)


@dataclass
class CustomClass:
    f1: int
    f2: str


if __name__ == '__main__':
    unittest.main()
