#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
import gc
import subprocess
import sys
import threading
import unittest
import weakref
from time import sleep, time
from unittest import mock

import pandas as pd
import pyarrow as pa
from pyarrow import csv

from pydeephaven import DHError, Session
from pydeephaven.table import Table
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

    def test_close_stops_worker_thread(self):
        def failing_close():
            raise DHError("simulated CloseSession failure")

        for case in ("normal", "CloseSession fails", "already disconnected"):
            with self.subTest(case):
                session = Session()
                worker = session._worker_thread
                self.assertTrue(worker.is_alive())
                if case == "CloseSession fails":
                    session.session_service.close = failing_close
                    with self.assertRaises(DHError):
                        session.close()
                else:
                    if case == "already disconnected":
                        session.is_connected = (
                            False  # as a failed token refresh leaves it
                        )
                    session.close()
                self.assertFalse(session.is_connected)
                worker.join(5)
                self.assertFalse(worker.is_alive())

    def test_token_is_refreshed(self):
        refreshes = []
        original_refresh = Session._refresh_token

        def counting_refresh(session):
            refreshes.append(time())
            return original_refresh(session)

        with mock.patch.multiple(
            Session,
            _seconds_until_refresh=lambda self: 0.05,
            _refresh_token=counting_refresh,
        ):
            with Session() as session:
                deadline = time() + 10
                while len(refreshes) < 3:
                    if time() > deadline:
                        self.fail(f"only {len(refreshes)} token refreshes")
                    sleep(0.05)
                self.assertEqual(session.empty_table(3).size, 3)

    def test_close_during_token_refresh(self):
        refreshes = []
        refresh_started = threading.Event()
        finish_refresh = threading.Event()

        def slow_refresh(session):
            refreshes.append(time())
            refresh_started.set()
            finish_refresh.wait(10)
            return True

        with mock.patch.multiple(
            Session,
            _seconds_until_refresh=lambda self: 0.05,
            _refresh_token=slow_refresh,
        ):
            session = Session()
            worker = session._worker_thread
            self.assertTrue(refresh_started.wait(10))
            session.close()
            finish_refresh.set()
            worker.join(10)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(refreshes), 1, "refreshed after close")

    def test_failing_job_does_not_stop_worker(self):
        from pydeephaven.session import _SessionJob

        ran_after_failure = threading.Event()

        class FailingJob(_SessionJob):
            def run(self, session):
                raise RuntimeError("simulated job failure")

        class MarkerJob(_SessionJob):
            def run(self, session):
                ran_after_failure.set()

        session = Session()
        try:
            with self.assertLogs("pydeephaven.session", level="ERROR") as logs:
                session._worker_queue.put(FailingJob())
                session._worker_queue.put(MarkerJob())
                self.assertTrue(ran_after_failure.wait(10))
            self.assertIn("FailingJob failed", logs.output[0])
            self.assertTrue(session._worker_thread.is_alive())
        finally:
            session.close()

    def test_reconnect_leaves_one_worker(self):
        session = Session()
        old_worker = session._worker_thread
        session.is_connected = False  # as a failed is_alive check leaves it
        with session:  # reconnects
            new_worker = session._worker_thread
            self.assertIsNot(new_worker, old_worker)
            old_worker.join(5)
            self.assertFalse(old_worker.is_alive())
            self.assertTrue(new_worker.is_alive())

    def test_dropped_session_is_collected(self):
        session = Session()
        worker = session._worker_thread
        session_ref = weakref.ref(session)
        del session
        # The worker holds the session for a moment on each pass, so retry until it lets go.
        deadline = time() + 5
        while session_ref() is not None and time() < deadline:
            gc.collect()
            sleep(0.05)
        self.assertIsNone(session_ref(), "the worker thread kept the session alive")
        worker.join(5)
        self.assertFalse(worker.is_alive())

    def test_unclosed_session_does_not_block_interpreter_exit(self):
        # A session left open in a reference cycle is collected during interpreter shutdown.
        script = (
            "from pydeephaven import Session\n"
            "session = Session()\n"
            "session.cycle = session\n"
            "session.empty_table(1)\n"
        )
        try:
            result = subprocess.run([sys.executable, "-c", script], timeout=60)
        except subprocess.TimeoutExpired:
            self.fail("the interpreter did not exit")
        self.assertEqual(result.returncode, 0)

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

    def test_garbage_collected_borrowed_wrapper_is_not_released(self):
        t = self.session.empty_table(10)
        # Shares t's export, as InputTable(session, t.ticket) does.
        wrapper = Table(self.session, ticket=t.ticket)
        with mock.patch.object(self.session, "_queue_release") as queue_release:
            del wrapper
            gc.collect()
        queue_release.assert_not_called()
        self.assertEqual(t.update(["X = i"]).size, 10)

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
            # The patch is global. Only the test thread's call may collect, or another session's RPC could pass the test.
            if threading.current_thread() is thread:
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
