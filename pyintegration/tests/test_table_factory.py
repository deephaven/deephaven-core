#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest
from dataclasses import dataclass
from time import sleep

import jpy
import numpy as np
from deephaven2.time import to_datetime

from deephaven2 import DHError, read_csv, time_table, empty_table, merge, merge_sorted, dtypes, new_table
from deephaven2.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven2.table_factory import DynamicTableWriter, TableReplayer
from tests.testbase import BaseTestCase

JArrayList = jpy.get_type("java.util.ArrayList")


@dataclass
class CustomClass:
    f1: int
    f2: str


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
        col_defs = {"Numbers": dtypes.int32, "Words": dtypes.string}
        with DynamicTableWriter(col_defs) as table_writer:
            table_writer.write_row(1, "Testing")
            table_writer.write_row(2, "Dynamic")
            table_writer.write_row(3, "Table")
            table_writer.write_row(4, "Writer")
            result = table_writer.table
            self.assertTrue(result.is_refreshing)

        with DynamicTableWriter(col_defs) as table_writer, self.assertRaises(DHError) as cm:
            table_writer.write_row(1, "Testing", "shouldn't be here")
        self.assertIn("RuntimeError", cm.exception.root_cause)

    def test_historical_tale_replayer(self):
        dt1 = to_datetime("2000-01-01T00:00:01 NY")
        dt2 = to_datetime("2000-01-01T00:00:03 NY")
        dt3 = to_datetime("2000-01-01T00:00:06 NY")

        hist_table = new_table([
            datetime_col("DateTime", [dt1, dt2, dt3]),
            int_col("Number", [1, 3, 6])]
        )

        start_time = to_datetime("2000-01-01T00:00:00 NY")
        end_time = to_datetime("2000-01-01T00:00:07 NY")

        replayer = TableReplayer(start_time, end_time)
        replay_table = replayer.add_table(hist_table, "DateTime")
        replay_table2 = replayer.add_table(hist_table, "DateTime")
        self.assertEqual(replay_table, replay_table2)

        replayer.start()
        self.assertTrue(replay_table.is_refreshing)
        self.assertTrue(replay_table2.is_refreshing)
        sleep(1)
        replayer.shutdown()

        with self.assertRaises(DHError) as cm:
            replay_table3 = replayer.add_table(hist_table, "DateTime")
        self.assertIn("RuntimeError", cm.exception.root_cause)

        with self.assertRaises(DHError) as cm:
            replayer.start()
        self.assertIn("RuntimeError", cm.exception.root_cause)


if __name__ == '__main__':
    unittest.main()
