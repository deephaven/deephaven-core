#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest
from time import sleep, time

import pyarrow as pa
import pandas as pd
from pyarrow import csv

from pydeephaven import DHError
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket
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

            console_script = ("""
from deephaven import empty_table
try:
    del t1
except NameError:
    pass
t1 = empty_table(0) if t.is_blink else None
""")
            session.run_script(console_script)
            self.assertNotIn("t1", session.tables)

            t = session.time_table(period=100000, blink_table=True)
            session.bind_table("t", t)
            session.run_script(console_script)
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
    def test_import_table_date32(self):
        types = [pa.date32()]
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

    def test_import_table_date64(self):
        from datetime import datetime
        pa_date1 = pa.scalar(datetime(2012, 1, 1), type=pa.date64())
        pa_date2 = pa.scalar(datetime(2022, 11, 11), type=pa.date64())
        pa_array = pa.array([pa_date1, pa_date2], type=pa.date64())

        pa_record_batch = pa.RecordBatch.from_arrays([pa_array], names=['f1'])
        pa_table = pa.Table.from_batches([pa_record_batch])
        new_table = self.session.import_table(pa_table)
        pa_table2 = new_table.to_arrow()
        self.assertEqual(pa_table, pa_table2)

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

    def test_blink_input_table(self):
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
        with Session() as session:
            dh_table = session.import_table(pa_table)

            with self.subTest("Create blink Input Table"):
                with self.assertRaises(ValueError):
                    session.input_table(schema=schema, key_cols="f1", blink_table=True)
                blink_input_table = session.input_table(schema=schema, blink_table=True)
                pa_table = blink_input_table.to_arrow()
                self.assertEqual(schema, pa_table.schema)
                session.bind_table("t", blink_input_table)
                console_script = ("""
from deephaven import empty_table
try:
    del t1
except NameError:
    pass
t1 = empty_table(0) if t.is_blink else None
        """)
                session.run_script(console_script)
                self.assertIn("t1", session.tables)

                with self.assertRaises(ValueError):
                    session.input_table(schema=schema, init_table=blink_input_table, blink_table=True)
                with self.assertRaises(ValueError):
                    session.input_table(key_cols="f0", blink_table=True)

            with self.subTest("blink InputTable ops"):
                session.bind_table("dh_table", dh_table)
                console_script = ("""
from deephaven import empty_table
try:
    del t1
except NameError:
    pass
t.add(dh_table)
t.await_update()
t1 = empty_table(0) if t.size == 2 else None
        """)
                session.run_script(console_script)
                self.assertIn("t1", session.tables)

                with self.assertRaises(PermissionError):
                    blink_input_table.delete(dh_table.select(["f1"]))

    def test_publish_table(self):
        pub_session = Session()
        t = pub_session.empty_table(1000).update(["X = i", "Y = 2*i"])
        self.assertEqual(t.size, 1000)
        shared_ticket = SharedTicket.random_ticket()
        pub_session.publish_table(shared_ticket, t)

        sub_session1 = Session()
        t1 = sub_session1.fetch_table(shared_ticket)
        self.assertEqual(t1.size, 1000)
        pa_table = t1.to_arrow()
        self.assertEqual(pa_table.num_rows, 1000)

        with self.subTest("the 1st subscriber session is gone, shared ticket is still valid"):
            sub_session1.close()
            sub_session2 = Session()
            t2 = sub_session2.fetch_table(shared_ticket)
            self.assertEqual(t2.size, 1000)

        with self.subTest("the publisher session is gone, shared ticket becomes invalid"):
            pub_session.close()
            with self.assertRaises(DHError):
                 sub_session2.fetch_table(shared_ticket)
            sub_session2.close()

    # Note no 'test_' prefix; we don't want this to be picked up
    # on every run; you can still ask the test runner to run it by manually asking
    # for it with, eg, `python -m unittest tests.test_session.SessionTestCase.mt_session`
    def mt_session(self):
        # There is already a Session object in the parent class, using that
        # is important to ensure you get only one Session involved,
        # otherwise debugging is hard.
        import datetime
        import threading
        session = self.session
        num_threads = 200
        run_time_seconds = 60*60
        deadline = time() + run_time_seconds
        def _interact_with_server(ti):
            print(f'THREAD {ti} START at {datetime.datetime.now()}', flush=True)
            while time() < deadline:
                session.run_script(f'import deephaven; t1_{ti} = deephaven.time_table("PT1S")')
                sleep(2)
                table = session.open_table(f't1_{ti}')
                pa_table = table.to_arrow()
                sleep(1)
            print(f'THREAD {ti} END at {datetime.datetime.now()}', flush=True)

        threads = []
        for ti in range(num_threads):
            t = threading.Thread(target=_interact_with_server, args=(ti,))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()


if __name__ == '__main__':
    unittest.main()
