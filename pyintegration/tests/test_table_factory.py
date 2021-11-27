#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest
from dataclasses import dataclass

import jpy
import numpy as np
import pandas as pd

from deephaven2 import DHError, read_csv, time_table, empty_table, merge, merge_sorted, dtypes, new_table
from deephaven2.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from tests.testbase import BaseTestCase

JArrayList = jpy.get_type("java.util.ArrayList")


class TableFactoryTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_empty_table(self):
        t = empty_table(10)
        self.assertEqual(0, len(t.columns))

    def test_empty_table_error(self):
        with self.assertRaises(DHError) as cm:
            t = empty_table("abc")

        print(cm.exception.root_cause)
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
        columns = [
            bool_col(name="Boolean", values=[True, False]),
            byte_col(name="Byte", values=[1, -1]),
            char_col(name="Char", values='-1'),
            short_col(name="Short", values=[1, -1]),
            int_col(name="Int", values=[1, -1]),
            long_col(name="Long", values=[1, -1]),
            long_col(name="NPLong", values=np.array([1, -1], dtype=np.int8)),
            float_col(name="Float", values=[1.01, -1.01]),
            double_col(name="Double", values=[1.01, -1.01]),
            string_col(name="String", values=["foo", "bar"]),
            datetime_col(name="Datetime", values=[dtypes.DateTime(1), dtypes.DateTime(-1)]),
            pyobj_col(name="PyObj", values=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
            jobj_col(name="JObj", class_name="java.util.ArrayList", values=[jobj1, jobj2])

        ]
        t = new_table(cols=columns)
        self.assertEqual(t.size, 2)

    def test_new_table_error(self):
        with self.assertRaises(DHError) as cm:
            jobj1 = JArrayList()
            jobj1.add(1)
            jobj1.add(-1)
            jobj2 = JArrayList()
            jobj2.add(2)
            jobj2.add(-2)
            columns = [
                bool_col(name="Boolean", values=[True, False]),
                byte_col(name="Byte", values=[1, -1]),
                char_col(name="Char", values='-1'),
                short_col(name="Short", values=[1, -1]),
                int_col(name="Int", values=[1, -1]),
                long_col(name="Long", values=[1, -1]),
                long_col(name="NPLong", values=np.array([1, -1], dtype=np.int32)),
                float_col(name="Float", values=[1.01, -1.01]),
                float_col(name="Float_NaN", values=[float('nan'), -1.01]),
                float_col(name="NP_float_NaN", values=np.array([float('nan'), -1], dtype=np.float_)),
                double_col(name="Double", values=[1.01, -1.01]),
                string_col(name="String", values=["foo", "bar"]),
                datetime_col(name="Datetime", values=[dtypes.DateTime(1), dtypes.DateTime(-1)]),
                pyobj_col(name="PyObj", values=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
                jobj_col(name="JObj", class_name="java.util.ArrayList", values=[jobj1, CustomClass(-1, "-1")])

            ]
            t = new_table(cols=columns)


@dataclass
class CustomClass:
    f1: int
    f2: str


if __name__ == '__main__':
    unittest.main()
