#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
import gc
import threading
import unittest
import weakref
from time import sleep, time

import pandas as pd
import pyarrow as pa
from pyarrow import csv

from pydeephaven import DHError, Session
from pydeephaven.ticket import ScopeTicket, SharedTicket
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
            Session(port=80)

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

            console_script = """
from deephaven import empty_table
try:
    del t1
except NameError:
    pass
t1 = empty_table(0) if t.is_blink else None
"""
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
        result_table = session.merge_tables(
            tables=[table1, table2, table3], order_by="a"
        )

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
        self.assertEqual(1000, len(df.index))

    def test_input_table(self):
        pa_types = [
            pa.bool_(),
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.timestamp("ns", tz="UTC"),
            pa.float32(),
            pa.float64(),
            pa.string(),
        ]
        pa_data = [
            pa.array([True, False]),
            pa.array([2**7 - 1, -(2**7) + 1]),
            pa.array([2**15 - 1, -(2**15) + 1]),
            pa.array([2**31 - 1, -(2**31) + 1]),
            pa.array([2**63 - 1, -(2**63) + 1]),
            pa.array(
                [
                    pd.Timestamp("2017-01-01T12:01:01", tz="UTC"),
                    pd.Timestamp("2017-01-01T11:01:01", tz="Europe/Paris"),
                ]
            ),
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

    def test_close_stops_session_threads(self):
        session = Session()
        keep_alive_timer = session._keep_alive_timer
        release_thread = session._release_thread
        self.assertTrue(keep_alive_timer.is_alive())
        self.assertTrue(release_thread.is_alive())
        session.close()
        keep_alive_timer.join(5)
        release_thread.join(5)
        self.assertFalse(keep_alive_timer.is_alive())
        self.assertFalse(release_thread.is_alive())

    def test_garbage_collected_table_is_released(self):
        for name, columns in (("with columns", ["X = i"]), ("without columns", [])):
            with self.subTest(name):
                t = self.session.empty_table(10)
                if columns:
                    t = t.update(columns)
                shared_ticket = SharedTicket.random_ticket()
                self.session.publish_table(shared_ticket, t)
                with Session() as sub_session:
                    self.assertEqual(sub_session.fetch_table(shared_ticket).size, 10)
                    del t
                    gc.collect()
                    # The session's release thread releases the ticket shortly after.
                    deadline = time() + 10
                    while True:
                        try:
                            sub_session.fetch_table(shared_ticket)
                        except DHError:
                            break
                        if time() > deadline:
                            self.fail("the table was not released")
                        sleep(0.1)

    def test_garbage_collection_during_rpc_does_not_deadlock(self):
        """A Table finalized in the middle of a gRPC call on the same thread must not make an RPC itself. gRPC holds
        its channel lock there, so that RPC would wait on the lock forever. This forces a collection at that point:
        gRPC starts its channel spin thread while it holds the lock.
        """
        import grpc._channel as grpc_channel

        original_spin = getattr(grpc_channel, "_run_channel_spin_thread", None)
        if original_spin is None:
            self.skipTest(
                "grpc._channel._run_channel_spin_thread is gone; update this test"
            )

        collected_under_lock = threading.Event()

        def spin_thread_with_gc(state):
            gc.collect()
            collected_under_lock.set()
            return original_spin(state)

        table_refs = []
        worker_errors = []

        def run():
            try:
                # A separate session, so a deadlocked thread does not block tearDown.
                session = Session()
                t = session.empty_table(5).update(["X = i"])
                t.cycle = t  # a reference cycle, so only the garbage collector frees it
                table_refs.append(weakref.ref(t))
                del t
                grpc_channel._run_channel_spin_thread = spin_thread_with_gc
                # The spin thread starts on a call made while no other call is in flight, so retry until one does.
                for _ in range(50):
                    session.empty_table(1)
                    if collected_under_lock.is_set():
                        break
                    sleep(0.05)
                grpc_channel._run_channel_spin_thread = original_spin
                session.close()
            except BaseException as e:
                worker_errors.append(e)

        gc.disable()  # only the collection under the lock may free the table
        try:
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            thread.join(60)
        finally:
            # Restored here, not in the worker: a deadlocked worker never gets to its own cleanup.
            grpc_channel._run_channel_spin_thread = original_spin
            gc.enable()
        self.assertFalse(
            thread.is_alive(), "deadlocked: a finalizer made an RPC inside a gRPC call"
        )
        if worker_errors:
            raise worker_errors[0]
        self.assertTrue(
            collected_under_lock.is_set(), "gRPC never started its spin thread"
        )
        self.assertIsNone(table_refs[0](), "the collection did not free the table")

    def test_blink_input_table(self):
        pa_types = [
            pa.bool_(),
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.timestamp("ns", tz="UTC"),
            pa.float32(),
            pa.float64(),
            pa.string(),
        ]
        pa_data = [
            pa.array([True, False]),
            pa.array([2**7 - 1, -(2**7) + 1]),
            pa.array([2**15 - 1, -(2**15) + 1]),
            pa.array([2**31 - 1, -(2**31) + 1]),
            pa.array([2**63 - 1, -(2**63) + 1]),
            pa.array(
                [
                    pd.Timestamp("2017-01-01T12:01:01", tz="UTC"),
                    pd.Timestamp("2017-01-01T11:01:01", tz="Europe/Paris"),
                ]
            ),
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
                console_script = """
from deephaven import empty_table
try:
    del t1
except NameError:
    pass
t1 = empty_table(0) if t.is_blink else None
        """
                session.run_script(console_script)
                self.assertIn("t1", session.tables)

                with self.assertRaises(ValueError):
                    session.input_table(
                        schema=schema, init_table=blink_input_table, blink_table=True
                    )
                with self.assertRaises(ValueError):
                    session.input_table(key_cols="f0", blink_table=True)

            with self.subTest("blink InputTable ops"):
                session.bind_table("dh_table", dh_table)
                console_script = """
from deephaven import empty_table
try:
    del t1
except NameError:
    pass
t.add(dh_table)
t.await_update()
t1 = empty_table(0) if t.size == 2 else None
        """
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

        with self.subTest(
            "the 1st subscriber session is gone, shared ticket is still valid"
        ):
            sub_session1.close()
            sub_session2 = Session()
            t2 = sub_session2.fetch_table(shared_ticket)
            self.assertEqual(t2.size, 1000)

        with self.subTest(
            "the publisher session is gone, shared ticket becomes invalid"
        ):
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
        run_time_seconds = 60 * 60
        deadline = time() + run_time_seconds

        def _interact_with_server(ti):
            print(f"THREAD {ti} START at {datetime.datetime.now()}", flush=True)
            while time() < deadline:
                session.run_script(
                    f'import deephaven; t1_{ti} = deephaven.time_table("PT1S")'
                )
                sleep(2)
                table = session.open_table(f"t1_{ti}")
                table.to_arrow()
                sleep(1)
            print(f"THREAD {ti} END at {datetime.datetime.now()}", flush=True)

        threads = []
        for ti in range(num_threads):
            t = threading.Thread(target=_interact_with_server, args=(ti,))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

    def test_systemic_scripts(self):
        fields = [pa.field("S", pa.bool_())]
        schema = pa.schema(fields)

        with Session() as session:
            # Run the setup script.
            session.run_script(
                """
from deephaven import time_table
import jpy

j_sot = jpy.get_type("io.deephaven.engine.util.systemicmarking.SystemicObjectTracker")
"""
            )
            table_script = """
t1 = time_table("PT1S").update("A=ii")
t2 = empty_table(1).update("S = (boolean)j_sot.isSystemic(t1.j_table)")
"""
            # Make sure defaults apply (expected false)
            session.run_script(table_script)
            t = session.fetch_table(ticket=ScopeTicket.scope_ticket("t2"))
            pa_table = pa.table([pa.array([False])], schema=schema)
            self.assertTrue(pa_table.equals(t.to_arrow()))

            session.run_script(table_script, True)
            t = session.fetch_table(ticket=ScopeTicket.scope_ticket("t2"))
            pa_table = pa.table([pa.array([True])], schema=schema)
            self.assertTrue(pa_table.equals(t.to_arrow()))

            session.run_script(table_script, False)
            t = session.fetch_table(ticket=ScopeTicket.scope_ticket("t2"))
            pa_table = pa.table([pa.array([False])], schema=schema)
            self.assertTrue(pa_table.equals(t.to_arrow()))


if __name__ == "__main__":
    unittest.main()
