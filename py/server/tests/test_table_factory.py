#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest
from dataclasses import dataclass

import jpy
import numpy as np
from time import sleep
from deephaven import DHError, read_csv, time_table, empty_table, merge, merge_sorted, dtypes, new_table, \
    input_table, time, _wrapper
from deephaven.column import byte_col, char_col, short_col, bool_col, int_col, long_col, float_col, double_col, \
    string_col, datetime_col, pyobj_col, jobj_col
from deephaven.constants import NULL_DOUBLE, NULL_FLOAT, NULL_LONG, NULL_INT, NULL_SHORT, NULL_BYTE
from deephaven.table_factory import DynamicTableWriter, InputTable, ring_table
from tests.testbase import BaseTestCase
from deephaven.table import Table
from deephaven.stream import blink_to_append_only, stream_to_append_only

JArrayList = jpy.get_type("java.util.ArrayList")
_JBlinkTableTools = jpy.get_type("io.deephaven.engine.table.impl.BlinkTableTools")
_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")
_JTable = jpy.get_type("io.deephaven.engine.table.Table")


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
        self.assertEqual(0, len(t.definition))

    def test_empty_table_error(self):
        with self.assertRaises(DHError) as cm:
            t = empty_table("abc")

        self.assertIn("RuntimeError", cm.exception.root_cause)
        self.assertIn("no matching Java method overloads found", cm.exception.compact_traceback)

    def test_time_table(self):
        t = time_table("PT00:00:01")
        self.assertEqual(1, len(t.definition))
        self.assertTrue(t.is_refreshing)

        t = time_table("PT00:00:01", start_time="2021-11-06T13:21:00 ET")
        self.assertEqual(1, len(t.definition))
        self.assertTrue(t.is_refreshing)
        self.assertEqual("2021-11-06T13:21:00.000000000 ET",
                         _JDateTimeUtils.formatDateTime(t.j_table.getColumnSource("Timestamp").get(0),
                                                        time.to_j_time_zone('ET')))

        t = time_table(1000_000_000)
        self.assertEqual(1, len(t.definition))
        self.assertTrue(t.is_refreshing)

        t = time_table(1000_1000_1000, start_time="2021-11-06T13:21:00 ET")
        self.assertEqual(1, len(t.definition))
        self.assertTrue(t.is_refreshing)
        self.assertEqual("2021-11-06T13:21:00.000000000 ET",
                         _JDateTimeUtils.formatDateTime(t.j_table.getColumnSource("Timestamp").get(0),
                                                        time.to_j_time_zone('ET')))

        p = time.to_timedelta(time.to_j_duration("PT1s"))
        t = time_table(p)
        self.assertEqual(1, len(t.definition))
        self.assertTrue(t.is_refreshing)

        st = time.to_datetime(time.to_j_instant("2021-11-06T13:21:00 ET"))
        t = time_table(p, start_time=st)
        self.assertEqual(1, len(t.definition))
        self.assertTrue(t.is_refreshing)
        self.assertEqual("2021-11-06T13:21:00.000000000 ET",
                         _JDateTimeUtils.formatDateTime(t.j_table.getColumnSource("Timestamp").get(0),
                                                        time.to_j_time_zone('ET')))

    def test_time_table_blink(self):
        t = time_table("PT1s", blink_table=True)
        self.assertEqual(1, len(t.definition))
        self.assertTrue(t.is_blink)

    def test_time_table_error(self):
        with self.assertRaises(DHError) as cm:
            time_table("PT00:0a:01")

        self.assertIn("DateTimeParseException", cm.exception.root_cause)

        with self.assertRaises(DHError):
            time_table(None)

    def test_merge(self):
        t1 = self.test_table.update(formulas=["Timestamp=epochNanosToInstant(0L)"])
        t2 = self.test_table.update(formulas=["Timestamp=nowSystem()"])
        mt = merge([t1, t2])
        self.assertFalse(mt.is_refreshing)

    def test_merge_sorted_error(self):
        t1 = time_table("PT00:00:01")
        t2 = self.test_table.update(formulas=["Timestamp=nowSystem()"])
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
            datetime_col(name="Datetime", data=[1, -1]),
            pyobj_col(name="PyObj", data=[CustomClass(1, "1"), CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj1", data=[[1, 2, 3], CustomClass(-1, "-1")]),
            pyobj_col(name="PyObj2", data=[False, 'False']),
            jobj_col(name="JObj", data=[jobj1, jobj2]),
        ]

    def test_new_table_dict(self):
        jobj1 = JArrayList()
        jobj1.add(1)
        jobj1.add(-1)
        jobj2 = JArrayList()
        jobj2.add(2)
        jobj2.add(-2)
        bool_cols = {
            "Boolean": [True, False],
        }
        integer_cols = {
            "Byte": (1, -1),
            "Short": [1, -1],
            "Int": [1, -1],
            "Long": [1, -1],
        }
        float_cols = {
            "Float": [1.01, -1.01],
            "Double": [1.01, -1.01],
        }
        string_cols = {
            "String": np.array(["foo", "bar"]),
        }
        datetime_cols = {
            "Datetime": np.array([1, -1], dtype=np.dtype("datetime64[ns]"))
        }

        obj_cols = {
            "PyObj": [CustomClass(1, "1"), CustomClass(-1, "-1")],
            "PyObj1": [[1, 2, 3], CustomClass(-1, "-1")],
            "PyObj2": [False, 'False'],
            "JObj": [jobj1, jobj2],
        }

        dtype_cols_map = {
            dtypes.bool_: bool_cols,
            dtypes.int64: integer_cols,
            dtypes.float64: float_cols,
            dtypes.string: string_cols,
            dtypes.Instant: datetime_cols,
            dtypes.PyObject: obj_cols
        }

        for dtype, cols in dtype_cols_map.items():
            with self.subTest(f"Testing {dtype}"):
                t = new_table(cols=cols)
                self.assertEqual(t.size, 2)
                for c in t.columns:
                    self.assertEqual(c.data_type, dtype)

        dtype_np_cols_map = {
            dtypes.int8: np.array([1, -1], dtype=np.int8),
            dtypes.int16: np.array([1, -1], dtype=np.int16),
            dtypes.int32: np.array([1, -1], dtype=np.int32),
            dtypes.int64: np.array([1, -1], dtype=np.int64),
            dtypes.float32: np.array([1.01, -1.01], dtype=np.float32),
            dtypes.float64: np.array([1.01, -1.01], dtype=np.float64),
        }
        d_cols = {dtype.j_name.capitalize(): cols for dtype, cols in dtype_np_cols_map.items()}
        t = new_table(cols=d_cols)
        for tc, dtype in zip(t.columns, dtype_np_cols_map.keys()):
            self.assertEqual(tc.data_type, dtype)

    def test_new_table_nulls(self):
        null_cols = [
            datetime_col("datetime_col", [None]),
            string_col("string_col", [None]),
            char_col("char_col", [None]),
            bool_col("bool_col", [None]),
            int_col("int_col", [None]),
            long_col("long_col", [None]),
            float_col("float_col", [None]),
            double_col("double_col", [None]),
            byte_col("byte_col", [None]),
            short_col("short_col", [None]),
        ]
        t = new_table(cols=null_cols)
        self.assertEqual(t.to_string().count("null"), len(null_cols))


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
        with self.subTest("from table definition"):
            append_only_input_table = input_table(col_defs=t.definition)
            self.assertEqual(append_only_input_table.key_names, [])
            self.assertEqual(append_only_input_table.value_names, [col._column_definition.name for col in cols])
            append_only_input_table.add(t)
            self.assertEqual(append_only_input_table.size, 2)
            append_only_input_table.add(t)
            self.assertEqual(append_only_input_table.size, 4)

            keyed_input_table = input_table(col_defs=t.definition, key_cols="String")
            self.assertEqual(keyed_input_table.key_names, ["String"])
            self.assertEqual(keyed_input_table.value_names, [col._column_definition.name for col in cols if col._column_definition.name != "String"])
            keyed_input_table.add(t)
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.add(t)
            self.assertEqual(keyed_input_table.size, 2)

        with self.subTest("from init table"):
            append_only_input_table = input_table(init_table=t)
            self.assertEqual(append_only_input_table.key_names, [])
            self.assertEqual(append_only_input_table.value_names, [col._column_definition.name for col in cols])
            self.assertEqual(append_only_input_table.size, 2)
            append_only_input_table.add(t)
            self.assertEqual(append_only_input_table.size, 4)

            keyed_input_table = input_table(init_table=t, key_cols="String")
            self.assertEqual(keyed_input_table.key_names, ["String"])
            self.assertEqual(keyed_input_table.value_names, [col._column_definition.name for col in cols if col._column_definition.name != "String"])
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.add(t)
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.add(append_only_input_table)
            self.assertEqual(keyed_input_table.size, 2)

        with self.subTest("deletion on input table"):
            append_only_input_table = input_table(init_table=t)
            with self.assertRaises(DHError) as cm:
                append_only_input_table.delete(t)
            self.assertIn("doesn\'t support delete operation", str(cm.exception))

            keyed_input_table = input_table(init_table=t, key_cols=["String", "Double"])
            self.assertEqual(keyed_input_table.key_names, ["String", "Double"])
            self.assertEqual(keyed_input_table.value_names, [col._column_definition.name for col in cols if col._column_definition.name != "String" and col._column_definition.name != "Double"])
            self.assertEqual(keyed_input_table.size, 2)
            keyed_input_table.delete(t.select(["String", "Double"]))
            self.assertEqual(keyed_input_table.size, 0)

        with self.subTest("custom input table creation"):
            place_holder_input_table = empty_table(1).update_view(["Key=`A`", "Value=10"]).with_attributes({_JTable.INPUT_TABLE_ATTRIBUTE: "Placeholder IT"}).j_table

            with self.assertRaises(DHError) as cm:
                InputTable(place_holder_input_table)
            self.assertIn("not of InputTableUpdater type", str(cm.exception))

            self.assertTrue(isinstance(_wrapper.wrap_j_object(place_holder_input_table), Table))


    def test_ring_table(self):
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
        keyed_input_table = input_table(init_table=t, key_cols=["String"])
        ring_t = ring_table(parent=keyed_input_table, capacity=6, initialize=False)
        for i in range(5):
            keyed_input_table.delete(t.select(["String"]))
            keyed_input_table.add(t)
        self.assertTrue(keyed_input_table.is_refreshing)
        self.assertEqual(keyed_input_table.size, 2)
        self.assertTrue(ring_t.is_refreshing)
        self.wait_ticking_table_update(ring_t, 6, 5)

    def test_blink_to_append_only(self):
        _JTimeTable = jpy.get_type("io.deephaven.engine.table.impl.TimeTable")
        _JBaseTable = jpy.get_type("io.deephaven.engine.table.impl.BaseTable")
        tt = Table(_JTimeTable.newBuilder().period("PT00:00:01").blinkTable(True).build())
        self.assertTrue(tt.is_refreshing)
        self.assertTrue(jpy.cast(tt.j_table, _JBaseTable).isBlink())

        bt = blink_to_append_only(tt)
        self.assertTrue(bt.is_refreshing)
        self.assertFalse(jpy.cast(bt.j_table, _JBaseTable).isBlink())

        # TODO (https://github.com/deephaven/deephaven-core/issues/3853): Delete this part of the test
        st = stream_to_append_only(tt)
        self.assertTrue(st.is_refreshing)
        self.assertFalse(jpy.cast(st.j_table, _JBaseTable).isBlink())

    def test_instant_array(self):
        from deephaven import DynamicTableWriter
        from deephaven import dtypes as dht
        from deephaven import time as dhtu

        col_defs_5 = {"InstantArray": dht.instant_array}

        dtw5 = DynamicTableWriter(col_defs_5)
        t5 = dtw5.table
        dtw5.write_row(dht.array(dht.Instant, [dhtu.to_j_instant("2021-01-01T00:00:00 ET"),
                                               dhtu.to_j_instant("2022-01-01T00:00:00 ET")]))
        self.wait_ticking_table_update(t5, row_count=1, timeout=5)
        self.assertEqual(t5.size, 1)

    def test_input_table_empty_data(self):
        from deephaven import update_graph as ugp
        from deephaven import execution_context as ec

        ug = ec.get_exec_ctx().update_graph
        cm = ugp.exclusive_lock(ug)

        with cm:
            t = time_table("PT1s", blink_table=True)
            it = input_table(t.definition, key_cols="Timestamp")
            it.add(t)
            self.assertEqual(it.size, 0)
            it.delete(t)
            self.assertEqual(it.size, 0)

        t = empty_table(0).update("Timestamp=nowSystem()")
        it.add(t)
        self.assertEqual(it.size, 0)
        it.delete(t)
        self.assertEqual(it.size, 0)

    def test_j_input_wrapping(self):
        cols = [
            bool_col(name="Boolean", data=[True, False]),
            string_col(name="String", data=["foo", "bar"]),
        ]
        t = new_table(cols=cols)
        append_only_input_table = input_table(col_defs=t.definition)

        it = _wrapper.wrap_j_object(append_only_input_table.j_table)
        self.assertTrue(isinstance(it, InputTable))

        t = _wrapper.wrap_j_object(t.j_object)
        self.assertFalse(isinstance(t, InputTable))
        self.assertTrue(isinstance(t, Table))

    def test_input_table_async(self):
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

        with self.subTest("async add"):
            self.assertEqual(t.size, 2)
            success_count = 0
            def on_success():
                nonlocal success_count
                success_count += 1
            append_only_input_table = input_table(col_defs=t.definition)
            append_only_input_table.add_async(t, on_success=on_success)
            append_only_input_table.add_async(t, on_success=on_success)
            while success_count < 2:
                sleep(0.1)
            self.assertEqual(append_only_input_table.size, 4)

            keyed_input_table = input_table(col_defs=t.definition, key_cols="String")
            keyed_input_table.add_async(t, on_success=on_success)
            keyed_input_table.add_async(t, on_success=on_success)
            while success_count < 4:
                sleep(0.1)
            self.assertEqual(keyed_input_table.size, 2)

        with self.subTest("async delete"):
            keyed_input_table = input_table(init_table=t, key_cols=["String", "Double"])
            keyed_input_table.delete_async(t.select(["String", "Double"]), on_success=on_success)
            while success_count < 5:
                sleep(0.1)
            self.assertEqual(keyed_input_table.size, 0)
            t1 = t.drop_columns("String")

        with self.subTest("schema mismatch"):
            error_count = 0
            def on_error(e: Exception):
                nonlocal error_count
                error_count += 1

            append_only_input_table = input_table(col_defs=t1.definition)
            with self.assertRaises(DHError) as cm:
                append_only_input_table.add_async(t, on_success=on_success, on_error=on_error)
            self.assertEqual(error_count, 0)


if __name__ == '__main__':
    unittest.main()
