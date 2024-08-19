#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import datetime
import time
import unittest
from dataclasses import dataclass

import numpy as np
import pandas as pd

from deephaven import DHError, dtypes, new_table, time as dhtime
from deephaven import empty_table
from deephaven.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, jobj_col, ColumnType, col_def
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

        with self.assertRaises(TypeError) as cm:
            _ = datetime_col(name="Datetime", data=[round(time.time()), False])

        with self.assertRaises(DHError) as cm:
            _ = jobj_col(name="JObj", data=[jobj, CustomClass(-1, "-1")])

    def test_array_column(self):
        strings = ["Str1", "Str1", "Str2", "Str2"]
        doubles = [1.0, 2.0, 4.0, 8.0]
        numbers = [1, 2, 3, 4]
        characters = [65, 66, 67, 68]
        bools = [True, True, False, False]
        test_table = new_table([
            string_col("StringColumn", strings),
            double_col("Decimals", doubles),
            float_col("Floats", doubles),
            byte_col("Bytes", numbers),
            short_col("Shorts", numbers),
            char_col("Chars", characters),
            int_col("Ints", numbers),
            long_col("Longs", numbers),
            bool_col("Bools", bools)
        ]
        )

        test_table = test_table.group_by(["StringColumn"])

        self.assertIsNone(test_table.columns[0].component_type)
        self.assertEqual(test_table.columns[1].component_type, dtypes.double)
        self.assertEqual(test_table.columns[2].component_type, dtypes.float32)
        self.assertEqual(test_table.columns[3].component_type, dtypes.byte)
        self.assertEqual(test_table.columns[4].component_type, dtypes.short)
        self.assertEqual(test_table.columns[5].component_type, dtypes.char)
        self.assertEqual(test_table.columns[6].component_type, dtypes.int32)
        self.assertEqual(test_table.columns[7].component_type, dtypes.long)
        self.assertEqual(test_table.columns[8].component_type, dtypes.bool_)

    def test_vector_column(self):
        t = empty_table(0).update_view("StringColumn=`abc`").group_by()
        self.assertTrue(t.columns[0].data_type.j_name.endswith("ObjectVector"))
        self.assertEqual(t.columns[0].component_type, dtypes.string)
        self.assertIsNone(t.columns[0].data_type.qst_type)

    def test_numeric_columns(self):
        x = [MAX_BYTE, MAX_SHORT, MAX_INT, MAX_LONG, 1, 999999]
        n = len(x)

        def get_x(i) -> int:
            return x[i]

        t_list = empty_table(n).update(["X = x[i]"])
        t_func = empty_table(n).update(["X = get_x(i)"])
        # We want to test that casting on both PyObject and JObject works as expected.
        self.assertEqual(t_list.columns[0].data_type, dtypes.PyObject)
        self.assertEqual(t_func.columns[0].data_type, dtypes.int64)
        t_func_str = t_func.to_string()
        for v in x:
            self.assertIn(str(int(v)), t_func_str)

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

    def test_datetime_col(self):
        inst = dhtime.to_j_instant(round(time.time()))
        dt = datetime.datetime.now()
        _ = datetime_col(name="Datetime", data=[inst, dt, None])
        self.assertEqual(_._column_definition.name, "Datetime")
        self.assertEqual(_._column_definition.data_type, dtypes.Instant)

        ts = pd.Timestamp(dt)
        np_dt = np.datetime64(dt)
        data = [ts, np_dt, dt]
        # test if we can convert to numpy datetime64 array
        np.array([pd.Timestamp(dt).to_numpy() for dt in data], dtype=np.datetime64)
        _ = datetime_col(name="Datetime", data=data)
        self.assertEqual(_._column_definition.name, "Datetime")
        self.assertEqual(_._column_definition.data_type, dtypes.Instant)

        data = np.array(['1970-01-01T00:00:00.000-07:00', '2020-01-01T01:00:00.000+07:00'])
        np.array([pd.Timestamp(str(dt)).to_numpy() for dt in data], dtype=np.datetime64)
        _ = datetime_col(name="Datetime", data=data)
        self.assertEqual(_._column_definition.name, "Datetime")
        self.assertEqual(_._column_definition.data_type, dtypes.Instant)

        data = np.array([1, -1])
        data = data.astype(np.int64)
        _ = datetime_col(name="Datetime", data=data)
        self.assertEqual(_._column_definition.name, "Datetime")
        self.assertEqual(_._column_definition.data_type, dtypes.Instant)

    def test_col_def_simple(self):
        foo_def = col_def("Foo", dtypes.int32)
        self.assertEquals(foo_def.name, "Foo")
        self.assertEquals(foo_def.data_type, dtypes.int32)
        self.assertEquals(foo_def.component_type, None)
        self.assertEquals(foo_def.column_type, ColumnType.NORMAL)

    def test_col_def_array(self):
        foo_def = col_def("Foo", dtypes.int32_array)
        self.assertEquals(foo_def.name, "Foo")
        self.assertEquals(foo_def.data_type, dtypes.int32_array)
        self.assertEquals(foo_def.component_type, dtypes.int32)
        self.assertEquals(foo_def.column_type, ColumnType.NORMAL)

    def test_col_def_partitioning(self):
        foo_def = col_def("Foo", dtypes.string, column_type=ColumnType.PARTITIONING)
        self.assertEquals(foo_def.name, "Foo")
        self.assertEquals(foo_def.data_type, dtypes.string)
        self.assertEquals(foo_def.component_type, None)
        self.assertEquals(foo_def.column_type, ColumnType.PARTITIONING)

    def test_col_def_invalid_component_type(self):
        with self.assertRaises(DHError):
            col_def("Foo", dtypes.int32_array, component_type=dtypes.int64)


@dataclass
class CustomClass:
    f1: int
    f2: str


if __name__ == '__main__':
    unittest.main()
