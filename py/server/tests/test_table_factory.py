#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest
from dataclasses import dataclass

import jpy
import numpy as np

from deephaven import DHError, read_csv, time_table, empty_table, merge, merge_sorted, dtypes, new_table, input_table
from deephaven.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven.constants import NULL_DOUBLE, NULL_FLOAT, NULL_LONG, NULL_INT, NULL_SHORT, NULL_BYTE
from deephaven.table_factory import DynamicTableWriter
from tests.testbase import BaseTestCase

JArrayList = jpy.get_type("java.util.ArrayList")


@dataclass
class CustomClass:
    f1: int
    f2: str


class TableFactoryTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_empty_table(self):
        t = empty_table(10)
        self.assertEqual(0, len(t.columns))

    def test_empty_table_error(self):
        with self.assertRaises(DHError) as cm:
            t = empty_table("abc")

        self.assertIn("RuntimeError", cm.exception.root_cause)
        self.assertIn("no matching Java method overloads found", cm.exception.compact_traceback)

    def test_time_table(self):
        t = time_table("00:00:01")
        self.assertEqual(1, len(t.columns))
        self.assertTrue(t.is_refreshing)

        t = time_table("00:00:01", start_time="2021-11-06T13:21:00 NY")
        self.assertEqual(1, len(t.columns))
        self.assertTrue(t.is_refreshing)
        self.assertEqual("2021-11-06T13:21:00.000000000 NY", t.j_table.getColumnSource("Timestamp").get(0).toString())

        t = time_table(1000_000_000)
        self.assertEqual(1, len(t.columns))
        self.assertTrue(t.is_refreshing)

        t = time_table(1000_1000_1000, start_time="2021-11-06T13:21:00 NY")
        self.assertEqual(1, len(t.columns))
        self.assertTrue(t.is_refreshing)
        self.assertEqual("2021-11-06T13:21:00.000000000 NY", t.j_table.getColumnSource("Timestamp").get(0).toString())

    def test_time_table_error(self):
        with self.assertRaises(DHError) as cm:
            t = time_table("00:0a:01")

        self.assertIn("IllegalArgumentException", cm.exception.root_cause)

    def test_merge(self):
        t1 = self.test_table.update(formulas=["Timestamp=new io.deephaven.time.DateTime(0L)"])
        t2 = self.test_table.update(formulas=["Timestamp=io.deephaven.time.DateTime.now()"])
        mt = merge([t1, t2])
        self.assertFalse(mt.is_refreshing)

    def test_merge_sorted_error(self):
        t1 = time_table("00:00:01")
        t2 = self.test_table.update(formulas=["Timestamp=io.deephaven.time.DateTime.now()"])
        with self.assertRaises(DHError) as cm:
            mt = merge_sorted(order_by="a", tables=[t1, t2])
            self.assertFalse(mt.is_refreshing)

        self.assertIn("UnsupportedOperationException", cm.exception.root_cause)

    def test_new_table(self):
        jobj1 = JArrayList()
        jobj1.add(1)
        jobj1.add(-1)
        jobj2 = JArrayList()
        jobj2.add(2)
        jobj2.add(-2)
        cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, -1]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
            datetime_col(name="Datetime", data=[dtypes.DateTime(1), dtypes.DateTime(-1)]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj1", data=[[1, 2, 3], CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj2", data=[False, 'False']),
            jobj_col(name="JObj", data=[jobj1, jobj2]),
        ]

        t = new_table(cols=cols)
        self.assertEqual(t.size, 2)

    def test_input_column_error(self):
        j_al = JArrayList()

        self.assertIsNotNone(bool_col(name="Boolean", data=[True, -1]))

        with self.assertRaises(DHError):
            bool_col(name="Boolean", data=[True, j_al])

    def test_dynamic_table_writer(self):
        with self.subTest("Correct Input"):
            col_defs = {"Numbers": dtypes.int32, "Words": dtypes.string}
            with DynamicTableWriter(col_defs) as table_writer:
                table_writer.write_row(1, "Testing")
                table_writer.write_row(2, "Dynamic")
                table_writer.write_row(3, "Table")
                table_writer.write_row(4, "Writer")
                result = table_writer.table
                self.assertTrue(result.is_refreshing)
                self.wait_ticking_table_update(result, row_count=4, timeout=5)

        with self.subTest("One too many values in the arguments"):
            with DynamicTableWriter(col_defs) as table_writer, self.assertRaises(DHError) as cm:
                table_writer.write_row(1, "Testing", "shouldn't be here")
            self.assertIn("RuntimeError", cm.exception.root_cause)

        with self.subTest("Proper numerical value conversion"):
            col_defs = {
                "Double": dtypes.double,
                "Float": dtypes.float32,
                "Long": dtypes.long,
                "Int32": dtypes.int32,
                "Short": dtypes.short,
                "Byte": dtypes.byte
            }
            with DynamicTableWriter(col_defs) as table_writer:
                table_writer.write_row(10, 10, 11, 11, 11, 11)
                table_writer.write_row(10.1, 10.1, 11.1, 11.1, 11.1, 11.1)
                table_writer.write_row(NULL_DOUBLE, NULL_FLOAT, NULL_LONG, NULL_INT, NULL_SHORT, NULL_BYTE)
            self.wait_ticking_table_update(table_writer.table, row_count=3, timeout=4)

            expected_dtypes = list(col_defs.values())
            self.assertEqual(expected_dtypes, [col.data_type for col in table_writer.table.columns])
            table_string = table_writer.table.to_string()
            self.assertEqual(6, table_string.count("null"))
            self.assertEqual(2, table_string.count("10.0"))
            self.assertEqual(2, table_string.count("10.1"))
            self.assertEqual(8, table_string.count("11"))

        with self.subTest("Incorrect value types"):
            with DynamicTableWriter(col_defs) as table_writer, self.assertRaises(DHError) as cm:
                table_writer.write_row(10, '10', 10, 10, 10, '10')
            self.assertIn("RuntimeError", cm.exception.root_cause)

    def test_dtw_with_array_types(self):
        with self.subTest("Array type columns"):
            col_defs = {
                "ByteArray": dtypes.byte_array,
                "ShortArray": dtypes.short_array,
                "Int32Array": dtypes.int32_array,
                "LongArray": dtypes.long_array,
                "Float32Array": dtypes.float32_array,
                "DoubleArray": dtypes.double_array,
                "StringArray": dtypes.string_array,
            }
            with DynamicTableWriter(col_defs) as table_writer:
                b_array = dtypes.array(dtypes.byte, [1, 1, 1])
                s_array = dtypes.array(dtypes.short, [128, 228, 328])
                i_array = dtypes.array(dtypes.int32, [32768, 42768, 52768])
                l_array = dtypes.array(dtypes.long, [2 ** 32, 2 ** 33, 2 ** 36])
                f_array = dtypes.array(dtypes.float32, [1.0, 1.1, 1.2])
                d_array = dtypes.array(dtypes.double, [1.0 / 2 ** 32, 1.1 / 2 ** 33, 1.2 / 2 ** 36])
                str_array = dtypes.array(dtypes.string, ["some", "not so random", "text"])
                table_writer.write_row(b_array, s_array, i_array, l_array, f_array, d_array, str_array
                                       )
                t = table_writer.table
                self.wait_ticking_table_update(t, row_count=1, timeout=5)
                self.assertNotIn("null", t.to_string())

    def test_dtw_single_string_arg(self):
        col_defs = {"A_String": dtypes.string}
        table_writer = DynamicTableWriter(col_defs)
        table_writer.write_row("Hello world!")
        t = table_writer.table
        self.wait_ticking_table_update(t, row_count=1, timeout=5)
        self.assertIn("Hello", t.to_string())

        col_defs = {"A_Long": dtypes.long}
        table_writer = DynamicTableWriter(col_defs)
        table_writer.write_row(10 ** 10)
        t = table_writer.table
        self.wait_ticking_table_update(t, row_count=1, timeout=5)
        self.assertIn("10000000000", t.to_string())

    def test_input_table(self):
        cols = [
            bool_col(name="Boolean", data=[True, False]),
            byte_col(name="Byte", data=(1, -1)),
            char_col(name="Char", data='-1'),
            short_col(name="Short", data=[1, -1]),
            int_col(name="Int", data=[1, -1]),
            long_col(name="Long", data=[1, -1]),
            long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", data=[1.01, -1.01]),
            double_col(name="Double", data=[1.01, -1.01]),
            string_col(name="String", data=["foo", "bar"]),
        ]
        t = new_table(cols=cols)
        self.assertEqual(t.size, 2)
        col_defs = {c.name: c.data_type for c in t.columns}
        with self.subTest("from table definition"):
            append_only_input_table = input_table(col_defs=col_defs)
            append_only_input_table.add(t)
            self.assertEqual(append_only_input_table.size, 2)
            append_only_input_table.add(t)
            self.assertEqual(append_only_input_table.size, 4)

            keyed_input_table = input_table(col_defs=col_defs, key_cols="String")
            keyed_input_table.add(t)
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.add(t)
            self.assertEqual(keyed_input_table.size, 2)

        with self.subTest("from init table"):
            append_only_input_table = input_table(init_table=t)
            self.assertEqual(append_only_input_table.size, 2)
            append_only_input_table.add(t)
            self.assertEqual(append_only_input_table.size, 4)

            keyed_input_table = input_table(init_table=t, key_cols="String")
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.add(t)
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.add(append_only_input_table)
            self.assertEqual(keyed_input_table.size, 2)

        with self.subTest("deletion on input table"):
            append_only_input_table = input_table(init_table=t)
            with self.assertRaises(DHError) as cm:
                append_only_input_table.delete(t)
            self.assertIn("not allowed.", str(cm.exception))

            keyed_input_table = input_table(init_table=t, key_cols=["String", "Double"])
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.delete(t.select(["String", "Double"]))
            self.assertEqual(keyed_input_table.size, 0)


if __name__ == '__main__':
    unittest.main()
