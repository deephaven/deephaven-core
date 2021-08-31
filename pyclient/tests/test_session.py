#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest
from time import sleep

import pyarrow as pa
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
            token1 = session.session_token
            sleep(300)
            token2 = session.session_token
            self.assertNotEqual(token1, token2)
        session.close()

    def test_empty_table(self):
        session = Session()
        t = session.empty_table(1000)
        self.assertEqual(t.size, 1000)
        session.close()

    def test_time_table(self):
        session = Session()
        t = session.time_table(period=100000)
        self.assertFalse(t.is_static)
        session.close()

    def test_merge_tables(self):
        session = Session()
        pa_table = csv.read_csv(self.csv_file)
        table1 = session.import_table(pa_table)
        table2 = table1.group_by(column_names=["a", "c"]).ungroup(column_names=["b", "d", "e"])
        table3 = table1.where(["a % 2 > 0 && b % 3 == 1"])
        result_table = session.merge_tables(tables=[table1, table2, table3], key_column="a")

        self.assertTrue(result_table.size > table1.size)
        self.assertTrue(result_table.size > table2.size)
        self.assertTrue(result_table.size > table3.size)

    def test_multiple_sessions(self):
        sessions = []
        for i in range(100):
            sessions.append(Session())

        tables = []
        for session in sessions:
            pa_table = csv.read_csv(self.csv_file)
            table1 = session.import_table(pa_table)
            table2 = table1.group_by()
            self.assertEqual(table2.size, 1)
            tables.append(table1)

        for i, table in enumerate(tables[:-1]):
            j_table = table.natural_join(tables[i + 1], keys=["a", "b", "c", "d", "e"])
            self.assertEqual(table.size, j_table.size)

        for session in sessions:
            session.close()

    def test_import_table_long_csv(self):
        pa_table = csv.read_csv(self.csv_file)
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.snapshot()
        self.assertEqual(pa_table2, pa_table)
        df = pa_table2.to_pandas()
        self.assertEquals(1000, len(df.index))

    @unittest.skip("GH ticket filed #941.")
    def test_import_table_time64(self):
        pa_array = pa.array([1, 2], type=pa.time64('ns'))
        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.snapshot()
        self.assertEqual(pa_table, pa_table2)

    @unittest.skip("GH ticket filed #941.")
    def test_import_table_ints(self):
        types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
        exception_list = []
        for t in types:
            pa_array = pa.array([-1, 0, 127], type=t)
            pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
            pa_table = pa.Table.from_batches([pa_record_batch])
            new_table = self.session.import_table(pa_table)
            pa_table2 = new_table.snapshot()
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
            pa_table2 = new_table.snapshot()
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
            pa_table2 = new_table.snapshot()
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
            pa_table2 = new_table.snapshot()
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
            pa_table2 = new_table.snapshot()
            try:
                self.assertEqual(pa_table, pa_table2)
            except Exception as e:
                exception_list.append(e)

        self.assertEqual(0, len(exception_list))
