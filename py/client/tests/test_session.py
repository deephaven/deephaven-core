#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest
from time import sleep

import pyarrow as pa
import pandas as pd
from pyarrow import csv

from pydeephaven import DHError
from pydeephaven import Session
from tests.testbase import BaseTestCase


class SessionTestCase(BaseTestCase):

    def test_connect(self):
        session = Session()
        self.assertEqual(True, session.is_connected)
        self.assertEqual(True, session.is_alive)
        session.close()

    def test_close(self):
        session = Session()
        session.close()
        self.assertEqual(False, session.is_connected)
        self.assertEqual(False, session.is_alive)

    def test_connect_failure(self):
        with self.assertRaises(DHError):
            session = Session(port=80)

    @unittest.skip("GH ticket filed #1178.")
    def test_never_timeout(self):
        session = Session()
        for _ in range(2):
            token1 = session._auth_token
            sleep(300)
            token2 = session._auth_token
            self.assertNotEqual(token1, token2)
        session.close()
        sleep(400)

    def test_empty_table(self):
        session = Session()
        t = session.empty_table(1000)
        self.assertEqual(t.size, 1000)
        session.close()

    def test_time_table(self):
        with Session() as session:
            t = session.time_table(period=100000)
            self.assertFalse(t.is_static)
            session.bind_table("t", t)
            session.run_script("""
from deephaven import empty_table
t1 = empty_table(0) if t.is_blink else None
""")
            self.assertNotIn("t1", session.tables)

            t = session.time_table(period=100000, blink_table=True)
            session.bind_table("t", t)
            session.run_script("""
from deephaven import empty_table
t1 = empty_table(0) if t.is_blink else None
""")
            self.assertIn("t1", session.tables)

    def test_merge_tables(self):
        session = Session()
        pa_table = csv.read_csv(self.csv_file)
        table1 = session.import_table(pa_table)
        table2 = table1.group_by(by=["a", "c"]).ungroup(cols=["b", "d", "e"])
        table3 = table1.where(["a % 2 > 0 && b % 3 == 1"])
        result_table = session.merge_tables(tables=[table1, table2, table3], order_by="a")

        self.assertTrue(result_table.size > table1.size)
        self.assertTrue(result_table.size > table2.size)
        self.assertTrue(result_table.size > table3.size)

    def test_multiple_sessions(self):
        sessions = []
        for i in range(10):
            sessions.append(Session())

        tables = []
        for session in sessions:
            pa_table = csv.read_csv(self.csv_file)
            table1 = session.import_table(pa_table)
            table2 = table1.group_by()
            self.assertEqual(table2.size, 1)
            tables.append(table1)

        for i, table in enumerate(tables[:-1]):
            j_table = table.natural_join(tables[i + 1], on=["a", "b", "c", "d", "e"])
            self.assertEqual(table.size, j_table.size)

        for session in sessions:
            session.close()

    def test_import_table_long_csv(self):
        pa_table = csv.read_csv(self.csv_file)
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table2, pa_table)
        df = pa_table2.to_pandas()
        self.assertEquals(1000, len(df.index))

    @unittest.skip("GH ticket filed #941.")
    def test_import_table_time64(self):
        pa_array = pa.array([1, 2], type=pa.time64('ns'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

    def test_import_table_ints(self):
        types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([-1, 0, 127], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    @unittest.skip("GH ticket filed #941.")
    def test_import_table_unsigned_ints(self):
        types = [pa.uint16()]
        exception_list = []
        for t in types:
            pa_array = pa.array([0, 255, 65535], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            # print(pa_table, pa_table2)
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_floats(self):
        types = [pa.float32(), pa.float64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([1.111, 2.222], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_import_table_strings(self):
        types = [pa.string(), pa.utf8()]
        exception_list = []
        for t in types:
            pa_array = pa.array(['text1', "text2"], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    @unittest.skip("GH ticket filed #941.")
    def test_import_table_dates(self):
        types = [pa.date32(), pa.date64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([1245, 123456], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.to_arrow()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))

    def test_input_table(self):
        pa_types = [
            pa.bool_(),
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.timestamp('ns', tz='UTC'),
            pa.float32(),
            pa.float64(),
            pa.string(),
        ]
        pa_data = [
            pa.array([True, False]),
            pa.array([2 ** 7 - 1, -2 ** 7 + 1]),
            pa.array([2 ** 15 - 1, -2 ** 15 + 1]),
            pa.array([2 ** 31 - 1, -2 ** 31 + 1]),
            pa.array([2 ** 63 - 1, -2 ** 63 + 1]),
            pa.array([pd.Timestamp('2017-01-01T12:01:01', tz='UTC'),
                      pd.Timestamp('2017-01-01T11:01:01', tz='Europe/Paris')]),
            pa.array([1.1, 2.2], pa.float32()),
            pa.array([1.1, 2.2], pa.float64()),
            pa.array(["foo", "bar"]),
        ]
        fields = [pa.field(f"f{i}", ty) for i, ty in enumerate(pa_types)]
        schema = pa.schema(fields)
        pa_table = pa.table(pa_data, schema=schema)
        dh_table = self.session.import_table(pa_table)

        with self.subTest("Create Input Table"):
            keyed_input_t = self.session.input_table(schema=schema, key_cols="f1")
            pa_table = keyed_input_t.to_arrow()
            self.assertEqual(schema, pa_table.schema)

            append_input_t = self.session.input_table(init_table=keyed_input_t)
            pa_table = append_input_t.to_arrow()
            self.assertEqual(schema, pa_table.schema)

            with self.assertRaises(ValueError):
                self.session.input_table(schema=schema, init_table=append_input_t)
            with self.assertRaises(ValueError):
                self.session.input_table(key_cols="f0")

        with self.subTest("InputTable ops"):
            keyed_input_t.add(dh_table)
            self.assertEqual(keyed_input_t.snapshot().size, dh_table.size)
            keyed_input_t.add(dh_table)
            self.assertEqual(keyed_input_t.snapshot().size, dh_table.size)
            keyed_input_t.delete(dh_table.select(["f1"]))
            self.assertEqual(keyed_input_t.snapshot().size, 0)

            append_input_t = self.session.input_table(init_table=keyed_input_t)
            append_input_t.add(dh_table)
            self.assertEqual(append_input_t.snapshot().size, dh_table.size)
            append_input_t.add(dh_table)
            self.assertEqual(append_input_t.snapshot().size, dh_table.size * 2)
            with self.assertRaises(PermissionError):
                append_input_t.delete(dh_table)

    def test_auto_close(self):
        session = Session()
        # this should trigger __del__
        session = None
        self.assertIsNone(session)


if __name__ == '__main__':
    unittest.main()
