#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import time
import unittest
from typing import List, Union, Optional, Dict

import numpy
import jpy

from deephaven import time_table, new_table, input_table, DHError, empty_table
from deephaven.column import bool_col, string_col
from deephaven.experimental import time_window
from deephaven.jcompat import to_sequence
from deephaven.table import Table
from deephaven.table_listener import listen, TableListener, TableListenerHandle, MergedListener, TableUpdate, \
    MergedListenerHandle, merged_listen
from deephaven.execution_context import get_exec_ctx
from deephaven.update_graph import exclusive_lock
from tests.testbase import BaseTestCase

_JColumnVectors = jpy.get_type("io.deephaven.engine.table.vectors.ColumnVectors")


class TableUpdateRecorder:
    def __init__(self, table: Optional[Table] = None, chunk_size: int = None, cols: Union[str, List[str]] = None):
        self.table = table
        self.chunk_size = chunk_size
        self.cols = cols

        self.added = []
        self.removed = []
        self.modified = []
        self.modified_prev = []
        self.replays = []
        self.modified_columns_list = []

    def record(self, update: TableUpdate, is_replay: bool):
        if not update:
            return

        if self.chunk_size is None:
            self.added.append(update.added())
            self.removed.append(update.removed())
            self.modified.append(update.modified())
            self.modified_prev.append(update.modified_prev())
        else:
            for chunk in update.added_chunks(chunk_size=self.chunk_size, cols=self.cols):
                self.added.append(chunk)
            for chunk in update.removed_chunks(chunk_size=self.chunk_size, cols=self.cols):
                self.removed.append(chunk)
            for chunk in update.modified_chunks(chunk_size=self.chunk_size, cols=self.cols):
                self.modified.append(chunk)
            for chunk in update.modified_prev_chunks(chunk_size=self.chunk_size, cols=self.cols):
                self.modified_prev.append(chunk)

        self.replays.append(is_replay)
        self.modified_columns_list.append(update.modified_columns)


def ensure_ugp_cycles(table_update_recorder: TableUpdateRecorder, cycles: int = 2) -> None:
    while len(table_update_recorder.replays) < cycles:
        time.sleep(1)


class TableListenerTestCase(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        with exclusive_lock(get_exec_ctx().update_graph):
            self.test_table = time_table("PT00:00:00.001").update(["X=i%11"]).sort("X").tail(16)
            source_table = time_table("PT00:00:00.001").update(["TS=now()"])
            self.test_table2 = time_window(source_table, ts_col="TS", window=10 ** 7, bool_col="InWindow")

    def tearDown(self) -> None:
        self.test_table = None
        self.test_table2 = None
        super().tearDown()

    def check_update_recorder(self, table_update_recorder: TableUpdateRecorder,
                              cols: Union[str, List[str]] = None, *, has_replay: bool = False, has_added: bool = False,
                              has_removed: bool = False, has_modified: bool = False):
        if has_added:
            self.verify_data_changes(table_update_recorder.added, cols)
        if has_removed:
            self.verify_data_changes(table_update_recorder.removed, cols)
        if has_modified:
            self.verify_data_changes(table_update_recorder.modified, cols)
            self.verify_data_changes(table_update_recorder.modified_prev, cols)
            self.assertEqual(table_update_recorder.modified_columns_list[-1], [cols])
        else:
            self.assertTrue(not any(table_update_recorder.modified_columns_list))

        if not has_replay:
            self.assertTrue(not any(table_update_recorder.replays))
        else:
            self.assertTrue(any(table_update_recorder.replays))
            self.assertTrue(not all(table_update_recorder.replays))

    def verify_data_changes(self, changes, cols: Union[str, List[str]]):
        changes = [c for c in changes if c]
        self.assertGreater(len(changes), 0)
        cols = to_sequence(cols)
        for change in changes:
            self.assertTrue(isinstance(change, dict))
            if not cols:
                cols = self.test_table.column_names
            for col in cols:
                self.assertIn(col, change.keys())
                self.assertTrue(isinstance(change[col], numpy.ndarray))
                self.assertEqual(change[col].ndim, 1)

    def test_listener_obj(self):
        table_update_recorder = TableUpdateRecorder(self.test_table)

        class ListenerClass(TableListener):
            def on_update(self, update, is_replay):
                table_update_recorder.record(update, is_replay)

        listener = ListenerClass()

        table_listener_handle = listen(self.test_table, listener)
        ensure_ugp_cycles(table_update_recorder)
        table_listener_handle.stop()

        self.check_update_recorder(table_update_recorder=table_update_recorder, cols="X", has_replay=False,
                                   has_added=True, has_removed=True, has_modified=False)

    def test_listener_func(self):
        table_update_recorder = TableUpdateRecorder(self.test_table)

        def listener_func(update, is_replay):
            table_update_recorder.record(update, is_replay)

        table_listener_handle = TableListenerHandle(self.test_table, listener_func)
        table_listener_handle.start(do_replay=True)
        ensure_ugp_cycles(table_update_recorder, cycles=3)
        table_listener_handle.stop()

        self.check_update_recorder(table_update_recorder, has_replay=True, has_added=True, has_removed=True,
                                   has_modified=False)

    def test_table_listener_handle_stop(self):
        table_update_recorder = TableUpdateRecorder(self.test_table)

        def listener_func(update, is_replay):
            table_update_recorder.record(update, is_replay)

        table_listener_handle = TableListenerHandle(self.test_table, listener_func)
        table_listener_handle.start()
        ensure_ugp_cycles(table_update_recorder)
        table_listener_handle.stop()
        call_counter = len(table_update_recorder.replays)
        ensure_ugp_cycles(table_update_recorder)

        self.assertEqual(call_counter, len(table_update_recorder.replays))

    def test_listener_func_chunk(self):
        table_update_recorder = TableUpdateRecorder(self.test_table, chunk_size=4)

        def listener_func(update, is_replay):
            table_update_recorder.record(update, is_replay)

        table_listener_handle = listen(self.test_table, listener_func, do_replay=True)
        ensure_ugp_cycles(table_update_recorder, cycles=3)
        table_listener_handle.stop()

        self.check_update_recorder(table_update_recorder, has_replay=True, has_added=True, has_removed=True,
                                   has_modified=False)

    def test_listener_obj_chunk(self):
        table_update_recorder = TableUpdateRecorder(self.test_table, chunk_size=4)

        class ListenerClass(TableListener):
            def on_update(self, update, is_replay):
                table_update_recorder.record(update, is_replay)

        listener = ListenerClass()
        table_listener_handle = listen(self.test_table, listener)
        ensure_ugp_cycles(table_update_recorder)
        table_listener_handle.stop()

        self.check_update_recorder(table_update_recorder, has_replay=False, has_added=True, has_removed=True,
                                   has_modified=False)

    def test_listener_func_modified_chunk(self):
        cols = "InWindow"
        table_update_recorder = TableUpdateRecorder(self.test_table2, chunk_size=1000, cols=cols)

        def listener_func(update, is_replay):
            table_update_recorder.record(update, is_replay)

        table_listener_handle = listen(self.test_table2, listener=listener_func)
        ensure_ugp_cycles(table_update_recorder)
        table_listener_handle.stop()

        self.check_update_recorder(table_update_recorder=table_update_recorder, cols=cols, has_replay=False,
                                   has_added=True, has_removed=False, has_modified=True)


    def test_listener_obj_with_deps(self):
        dep_table = time_table("PT00:00:05").update("X = i % 11")
        ec = get_exec_ctx()

        with self.subTest("view op on deps"):
            table_update_recorder = TableUpdateRecorder(self.test_table)
            j_arrays = []

            class ListenerClass(TableListener):
                def on_update(self, update, is_replay):
                    table_update_recorder.record(update, is_replay)
                    with ec:
                        t2 = dep_table.view(["Y = i % 8"])
                        j_arrays.append(_JColumnVectors.of(t2.j_table, "Y").copyToArray())

            listener = ListenerClass()
            table_listener_handle = listen(self.test_table, listener, dependencies=dep_table)
            ensure_ugp_cycles(table_update_recorder, cycles=3)
            table_listener_handle.stop()

            self.check_update_recorder(table_update_recorder=table_update_recorder, cols="X", has_replay=False,
                                       has_added=True, has_removed=True, has_modified=False)
            self.assertTrue(all([len(ja) > 0 for ja in j_arrays]))

        with (self.subTest("update/group_by op on deps")):
            table_update_recorder = TableUpdateRecorder(self.test_table)
            j_arrays = []

            class ListenerClass(TableListener):
                def on_update(self, update, is_replay):
                    table_update_recorder.record(update, is_replay)
                    with ec:
                        t2 = dep_table.update(["Y = i % 8"]).group_by("X")
                        j_arrays.append(_JColumnVectors.of(t2.j_table, "Y").copyToArray())

            listener = ListenerClass()
            self.test_table.await_update()
            table_listener_handle = listen(self.test_table, listener, dependencies=dep_table)
            ensure_ugp_cycles(table_update_recorder, cycles=3)
            table_listener_handle.stop()

            self.check_update_recorder(table_update_recorder=table_update_recorder, cols="X", has_replay=False,
                                       has_added=True, has_removed=True, has_modified=False)
            self.assertTrue(all([len(ja) > 0 for ja in j_arrays]))

        with (self.subTest("join op on deps")):
            table_update_recorder = TableUpdateRecorder(self.test_table)
            j_arrays = []
            dep_table_2 = time_table("PT00:00:05").update("X = i % 11")

            class ListenerClass(TableListener):
                def on_update(self, update, is_replay):
                    table_update_recorder.record(update, is_replay)
                    with ec:
                        t2 = dep_table.update(["Y = i % 8"]).group_by("X").join(dep_table_2, on="X",
                                                                                joins="Ts2=Timestamp")
                        j_arrays.append(_JColumnVectors.of(t2.j_table, "Y").copyToArray())

            listener = ListenerClass()
            self.test_table.await_update()
            table_listener_handle = listen(self.test_table, listener, dependencies=[dep_table, dep_table_2])
            ensure_ugp_cycles(table_update_recorder, cycles=3)
            table_listener_handle.stop()

            self.check_update_recorder(table_update_recorder=table_update_recorder, cols="X", has_replay=False,
                                       has_added=True, has_removed=True, has_modified=False)
            self.assertTrue(all([len(ja) > 0 for ja in j_arrays]))

        dep_table = dep_table_2 = None


    def test_listener_func_with_deps(self):
        cols = [
            bool_col(name="Boolean", data=[True, False]),
            string_col(name="String", data=["foo", "bar"]),
        ]
        t = new_table(cols=cols)
        self.assertEqual(t.size, 2)
        dep_table = input_table(col_defs=t.definition)

        def listener_func(update, is_replay):
            table_update_recorder.record(update, is_replay)
            try:
                dep_table.add(t)
            except Exception as e:
                self.assertIn("Attempted to make a blocking input table edit", str(e))
                pass

        with self.subTest("do_replay=False"):
            table_update_recorder = TableUpdateRecorder(self.test_table)
            table_listener_handle = TableListenerHandle(self.test_table, listener_func, dependencies=dep_table)
            table_listener_handle.start(do_replay=False)
            ensure_ugp_cycles(table_update_recorder, cycles=3)
            table_listener_handle.stop()
            self.check_update_recorder(table_update_recorder, has_replay=False, has_added=True, has_removed=True,
                                   has_modified=False)
            self.assertEqual(dep_table.size, 0)

        with self.subTest("do_replay=True, replay_lock='exclusive'"):
            table_update_recorder = TableUpdateRecorder(self.test_table)
            table_listener_handle.start(do_replay=True)
            ensure_ugp_cycles(table_update_recorder, cycles=3)
            table_listener_handle.stop()
            self.check_update_recorder(table_update_recorder, has_replay=True, has_added=True, has_removed=True, has_modified=False)

        with self.subTest("do_replay=True, replay_lock='shared'"):
            table_update_recorder = TableUpdateRecorder(self.test_table)
            table_listener_handle.start(do_replay=True) # noqa
            ensure_ugp_cycles(table_update_recorder, cycles=3)
            table_listener_handle.stop()
            self.check_update_recorder(table_update_recorder, has_replay=True, has_added=True, has_removed=True, has_modified=False)

    def test_listener_obj_with_deps_error(self):
        _JPUG = jpy.get_type('io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph')
        update_graph = _JPUG.newBuilder("TestUG").existingOrBuild()

        from deephaven import execution_context
        _JEC = jpy.get_type('io.deephaven.engine.context.ExecutionContext')
        ug_ctx = execution_context.ExecutionContext(j_exec_ctx=_JEC.newBuilder()
                                                    .emptyQueryScope()
                                                    .newQueryLibrary()
                                                    .captureQueryCompiler()
                                                    .setUpdateGraph(update_graph)
                                                    .build())

        with ug_ctx:
            dep_table = time_table("PT1s")

        def listener_func(update, is_replay):
            pass

        with self.assertRaises(DHError):
            table_listener_handle = TableListenerHandle(self.test_table, listener_func, dependencies=dep_table)

    def test_merged_listener_obj(self):
        t1 = time_table("PT1s").update(["X=i % 11"])
        t2 = time_table("PT2s").update(["Y=i % 8"])
        t3 = time_table("PT3s").update(["Z=i % 5"])

        class TestMergedListener(MergedListener):
            def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                for update in updates.values():
                    tur.record(update, is_replay)

        tml = TestMergedListener()
        with self.subTest("Direct Handle"):
            tur = TableUpdateRecorder()
            mlh = MergedListenerHandle([t1, t2, t3], tml)
            mlh.start()
            ensure_ugp_cycles(tur, cycles=3)
            mlh.stop()
            mlh.start()
            ensure_ugp_cycles(tur, cycles=6)
            mlh.stop()
            self.assertGreaterEqual(len(tur.replays), 6)

        with self.subTest("Convenience function"):
            tur = TableUpdateRecorder()
            mlh = merged_listen([t1, t2, t3], tml)
            ensure_ugp_cycles(tur, cycles=3)
            mlh.stop()
            mlh.start()
            ensure_ugp_cycles(tur, cycles=6)
            mlh.stop()
            self.assertGreaterEqual(len(tur.replays), 6)

        t1 = t2 = t3 = None

    def test_merged_listener_func(self):
        t1 = time_table("PT1s").update(["X=i % 11"])
        t2 = time_table("PT2s").update(["Y=i % 8"])
        t3 = time_table("PT3s").update(["Z=i % 5"])

        def test_ml_func(updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
            if updates[t1] or updates[t3]:
                tur.record(updates[t1], is_replay)

        with self.subTest("Direct Handle"):
            tur = TableUpdateRecorder()
            mlh = MergedListenerHandle([t1, t2, t3], test_ml_func)
            mlh.start()
            ensure_ugp_cycles(tur, cycles=3)
            mlh.stop()
            mlh.start()
            ensure_ugp_cycles(tur, cycles=6)
            mlh.stop()
            self.assertGreaterEqual(len(tur.replays), 6)

        with self.subTest("Convenience function"):
            tur = TableUpdateRecorder()
            mlh = merged_listen([t1, t2, t3], test_ml_func)
            ensure_ugp_cycles(tur, cycles=3)
            mlh.stop()
            mlh.start()
            ensure_ugp_cycles(tur, cycles=6)
            mlh.stop()
            self.assertGreaterEqual(len(tur.replays), 6)

        t1 = t2 = t3 = None

    def test_merged_listener_with_deps(self):
        t1 = time_table("PT1s").update(["X=i % 11"])
        t2 = time_table("PT2s").update(["Y=i % 8"])
        t3 = time_table("PT3s").update(["Z=i % 5"])

        dep_table = time_table("PT00:00:05").update("X = i % 11")
        ec = get_exec_ctx()

        tur = TableUpdateRecorder()
        j_arrays = []
        class TestMergedListener(MergedListener):
            def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                if updates[t1] and updates[t2]:
                    tur.record(updates[t2], is_replay)

                with ec:
                    t = dep_table.view(["Y = i % 8"])
                    j_arrays.append(_JColumnVectors.of(t.j_table, "Y").copyToArray())

        tml = TestMergedListener()
        mlh = MergedListenerHandle(tables=[t1, t2, t3], listener=tml, dependencies=dep_table)
        mlh.start()
        ensure_ugp_cycles(tur, cycles=3)
        mlh.stop()
        mlh.start()
        ensure_ugp_cycles(tur, cycles=6)
        mlh.stop()
        self.assertGreaterEqual(len(tur.replays), 6)
        self.assertTrue(len(j_arrays) > 0 and all([len(ja) > 0 for ja in j_arrays]))

        t1 = t2 = t3 = None

    def test_merged_listener_error(self):
        t1 = time_table("PT1s").update(["X=i % 11"])

        def test_ml_func(updates: Dict[Table, TableUpdate]) -> None:
            pass

        with self.assertRaises(DHError) as cm:
            mlh = MergedListenerHandle([t1], test_ml_func)
        self.assertIn("at least 2 refreshing tables", str(cm.exception))

        et = empty_table(1)
        with self.assertRaises(DHError) as cm:
            mlh = merged_listen([t1, et], test_ml_func)
        self.assertIn("must be a refreshing table", str(cm.exception))

        t1 = et = None

    def test_merged_listener_replay(self):
        t1 = time_table("PT1s").update(["X=i % 11"])
        t2 = time_table("PT2s").update(["Y=i % 8"])
        t3 = time_table("PT3s").update(["Z=i % 5"])

        class TestMergedListener(MergedListener):
            def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                for update in updates.values():
                    tur.record(update, is_replay)

        tml = TestMergedListener()
        t1.await_update()
        t2.await_update()
        t3.await_update()
        with self.subTest("MergedListener - replay"):
            tur = TableUpdateRecorder()
            mlh = merged_listen([t1, t2, t3], tml, do_replay=True)
            ensure_ugp_cycles(tur, cycles=3)
            mlh.stop()
            mlh.start(do_replay=True)
            ensure_ugp_cycles(tur, cycles=6)
            mlh.stop()
            self.assertGreaterEqual(len(tur.replays), 6)
            self.assertEqual(tur.replays.count(True), 2 * 3)

        def test_ml_func(updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
            tur.record(updates[t3], is_replay)

        with self.subTest("Direct Handle - replay"):
            tur = TableUpdateRecorder()
            mlh = MergedListenerHandle([t1, t2, t3], test_ml_func)
            mlh.start(do_replay=True)
            ensure_ugp_cycles(tur, cycles=3)
            mlh.stop()
            mlh.start(do_replay=True)
            ensure_ugp_cycles(tur, cycles=6)
            mlh.stop()
            self.assertGreaterEqual(len(tur.replays), 6)
            self.assertEqual(tur.replays.count(True), 2)

        t1 = t2 = t3 = None

    def test_on_error_listener_func(self):
        t = time_table("PT1S").update("X = i")
        with self.subTest("Bad Listener Good Error Callback"):
            def bad_listner_func(table_udpate, is_replay: bool) -> None:
                raise ValueError("invalid value")

            def on_error(e: Exception) -> None:
                nonlocal error_caught
                error_caught = True
                self.assertIn("invalid value", str(e))

            error_caught = False
            tlh = listen(t, bad_listner_func, on_error=on_error)
            t.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(tlh.j_object.isFailed())

        with self.subTest("Good Listener Good Error Callback"):
            def good_listner_func(table_udpate, is_replay: bool) -> None:
                pass

            error_caught = False
            tlh = listen(t, good_listner_func, on_error=on_error)
            t.await_update()
            self.assertFalse(error_caught)
            self.assertFalse(tlh.j_object.isFailed())

        with self.subTest("Bad Listener Bad Error Callback"):
            error_caught: bool = False

            def bad_listner_func(table_udpate, is_replay: bool) -> None:
                raise ValueError("invalid value")

            def on_error(e: Exception) -> None:
                nonlocal error_caught
                error_caught = True
                self.assertIn("invalid value", str(e))
                raise ValueError("reraise the exception") from e

            tlh = listen(t, bad_listner_func, on_error=on_error)
            t.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(tlh.j_object.isFailed())

        t = None

    def test_on_error_listener_obj(self):
        test_self = self
        t = time_table("PT1S").update("X = i")

        with self.subTest("Bad Listener Good Error Callback"):
            class BadListener(TableListener):
                def on_update(self, update: TableUpdate, is_replay: bool) -> None:
                    raise ValueError("invalid value")

                def on_error(self, e: Exception) -> None:
                    nonlocal error_caught
                    error_caught = True
                    test_self.assertIn("invalid value", str(e))

            error_caught = False
            bad_listener_obj = BadListener()
            tlh = listen(t, bad_listener_obj)
            t.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(tlh.j_object.isFailed())

            with self.assertRaises(DHError):
                def on_error(e: Exception) -> None:
                    ...
                tlh = listen(t, bad_listener_obj, on_error=on_error)

        with self.subTest("Good Listener Good Error Callback"):
            class GoodListener(TableListener):
                def on_update(self, update: TableUpdate, is_replay: bool) -> None:
                    ...

                def on_error(self, e: Exception) -> None:
                    nonlocal error_caught
                    error_caught = True
                    test_self.assertIn("invalid value", str(e))

            error_caught = False
            good_listener_obj = GoodListener()
            tlh = listen(t, good_listener_obj)
            t.await_update()
            self.assertFalse(error_caught)
            self.assertFalse(tlh.j_object.isFailed())

        with self.subTest("Bad Listener Bad Error Callback"):
            class GoodListener(TableListener):
                def on_update(self, update: TableUpdate, is_replay: bool) -> None:
                    raise ValueError("invalid value")

                def on_error(self, e: Exception) -> None:
                    nonlocal error_caught
                    error_caught = True
                    test_self.assertIn("invalid value", str(e))
                    raise ValueError("reraise the exception") from e

            error_caught = False

            good_listener_obj = GoodListener()
            tlh = listen(t, good_listener_obj)
            t.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(tlh.j_object.isFailed())

        t = None

    def test_on_error_merged_listener_func(self):
        t1 = time_table("PT1s").update(["X=i % 11"])
        t2 = time_table("PT2s").update(["Y=i % 8"])
        t3 = time_table("PT3s").update(["Z=i % 5"])

        with self.subTest("Bad Listener Good Error Callback"):
            def bad_listner_func(updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                raise ValueError("invalid value")

            def on_error(e: Exception) -> None:
                nonlocal error_caught
                error_caught = True
                self.assertIn("invalid value", str(e))

            error_caught = False
            mlh = merged_listen([t1, t2, t3], bad_listner_func, on_error=on_error)
            t1.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(mlh.j_object.isFailed())

        with self.subTest("Good Listener Good Error Callback"):
            def good_listner_func(updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                pass

            error_caught = False
            mlh = merged_listen([t1, t2, t3], good_listner_func, on_error=on_error)
            t1.await_update()
            self.assertFalse(error_caught)
            self.assertFalse(mlh.j_object.isFailed())

        with self.subTest("Bad Listener Bad Error Callback"):
            def bad_listner_func(updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                raise ValueError("invalid value")

            def bad_on_error(e: Exception) -> None:
                nonlocal error_caught
                error_caught = True
                self.assertIn("invalid value", str(e))
                raise ValueError("reraise the exception") from e

            error_caught = False
            mlh = merged_listen([t1, t2, t3], bad_listner_func, on_error=bad_on_error)
            t1.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(mlh.j_object.isFailed())

        t1 = t2 = t3 = None

    def test_on_error_merged_listener_obj(self):
        test_self = self
        t1 = time_table("PT1s").update(["X=i % 11"])
        t2 = time_table("PT2s").update(["Y=i % 8"])
        t3 = time_table("PT3s").update(["Z=i % 5"])

        with self.subTest("Bad Listener Good Error Callback"):
            class BadListener(MergedListener):
                def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                    raise ValueError("invalid value")

                def on_error(self, e: Exception) -> None:
                    nonlocal error_caught
                    error_caught = True
                    test_self.assertIn("invalid value", str(e))

            error_caught = False
            bad_listener_obj = BadListener()
            mlh = merged_listen([t1, t2, t3], bad_listener_obj)
            t1.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(mlh.j_object.isFailed())

            with self.assertRaises(DHError):
                def on_error(e: Exception) -> None:
                    ...
                tlh = merged_listen([t1, t2, t3], bad_listener_obj, on_error=on_error)


        with self.subTest("Good Listener Good Error Callback"):
            class GoodListener(MergedListener):
                def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                    ...

                def on_error(self, e: Exception) -> None:
                    nonlocal error_caught
                    error_caught = True
                    test_self.assertIn("invalid value", str(e))

            error_caught = False
            good_listener_obj = GoodListener()
            mlh = merged_listen([t1, t2, t3], good_listener_obj)
            t1.await_update()
            self.assertFalse(error_caught)
            self.assertFalse(mlh.j_object.isFailed())

        with self.subTest("Bad Listener Bad Error Callback"):
            class BadListener(MergedListener):
                def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                    raise ValueError("invalid value")

                def on_error(self, e: Exception) -> None:
                    nonlocal error_caught
                    error_caught = True
                    test_self.assertIn("invalid value", str(e))
                    raise ValueError("reraise the exception") from e

            error_caught = False
            bad_listener_obj = BadListener()
            mlh = merged_listen([t1, t2, t3], bad_listener_obj)
            t1.await_update()
            self.assertTrue(error_caught)
            self.assertTrue(mlh.j_object.isFailed())

        t1 = t2 = t3 = None

    def test_default_on_error(self):
        t = time_table("PT1S").update("X = i")

        def bad_listner_func(table_udpate, is_replay: bool) -> None:
            raise ValueError("invalid value")

        error_caught = False
        tlh = listen(t, bad_listner_func)
        t.await_update()
        # the default on_error only logs the error
        self.assertFalse(error_caught)
        self.assertTrue(tlh.j_object.isFailed())

        class BadListener(TableListener):
            def on_update(self, update, is_replay):
                raise ValueError("invalid value")

        tlh = listen(t, BadListener())
        t.await_update()
        # the default on_error only logs the error
        self.assertFalse(error_caught)
        self.assertTrue(tlh.j_object.isFailed())

        t2 = time_table("PT1S").update("X = i")
        def bad_listner_func(updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
            raise ValueError("invalid value")

        mlh = merged_listen([t, t2], bad_listner_func)
        t.await_update()
        # the default on_error only logs the error
        self.assertFalse(error_caught)
        self.assertTrue(mlh.j_object.isFailed())

        class BadListener(MergedListener):
            def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
                raise ValueError("invalid value")

        mlh = merged_listen([t, t2], BadListener())
        t.await_update()
        # the default on_error only logs the error
        self.assertFalse(error_caught)
        self.assertTrue(mlh.j_object.isFailed())

        t = t2 = None


if __name__ == "__main__":
    unittest.main()
