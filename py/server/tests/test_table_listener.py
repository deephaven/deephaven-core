#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import time
import unittest
from pprint import pprint
from types import SimpleNamespace
from typing import List, Dict, Union

import jpy
import numpy
from deephaven.experimental import time_window

from deephaven import time_table
from deephaven.jcompat import to_sequence
from deephaven.table_listener import listen, TableListener, TableUpdate, TableListenerHandle
from tests.testbase import BaseTestCase


class TableListenerTestCase(BaseTestCase):

    def setUp(self) -> None:
        with self.ugp_lock_exclusive():
            self.test_table = time_table("00:00:00.001").update(["X=i%2"]).sort("X").tail(1)
            source_table = time_table("00:00:00.001").update(["TS=currentTime()"])
            self.test_table2 = time_window(source_table, ts_col="TS", window=10 ** 7, bool_col="InWindow")

    def tearDown(self) -> None:
        self.test_table = None
        self.test_table2 = None

    def check_result(self, changes: List[Dict[str, numpy.ndarray]], cols: Union[str, List[str]] = None):
        if not changes:
            return
        cols = to_sequence(cols)
        for change in changes:
            if not change:
                continue
            self.assertTrue(isinstance(change, dict))
            if not cols:
                cols = [col.name for col in self.test_table.columns]
            for col in cols:
                self.assertIn(col, change.keys())
                self.assertTrue(isinstance(change[col], numpy.ndarray))
                self.assertEqual(change[col].ndim, 1)
                # print(change[col])

    def test_listener_obj(self):
        class ListenerClass(TableListener):
            def __init__(self):
                self.t_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                                Shifted=None, modified_columns=None)

            def on_update(self, update, is_replay):
                self.t_update.added = []
                self.t_update.added.append(update.added())

                self.t_update.removed = []
                self.t_update.removed.append(update.removed())

                self.t_update.modified = []
                self.t_update.modified.append(update.modified())

                self.t_update.modified_prev = []
                self.t_update.modified_prev.append(update.modified_prev())

                # self.table_update.shifted = update.shifted

        listener = ListenerClass()

        table_listener_handle = listen(self.test_table, listener)
        time.sleep(1)
        pprint(listener.t_update.added)
        pprint("----" * 10)
        pprint(listener.t_update.removed)
        self.check_result(listener.t_update.added, cols="X")
        self.check_result(listener.t_update.removed)
        self.check_result(listener.t_update.modified)
        self.check_result(listener.t_update.modified_prev)

        table_listener_handle.stop()

    def test_listener_func(self):
        replay_recorder = []
        t_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                       Shifted=None, modified_columns=None)

        def listener_func(update, is_replay):
            t_update.added = []
            t_update.removed = []
            t_update.modified = []
            t_update.modified_prev = []
            t_update.added.append(update.added())
            t_update.removed.append(update.removed())
            t_update.modified.append(update.modified())
            t_update.modified_prev.append(update.modified_prev())
            replay_recorder.append(is_replay)

        table_listener_handle = TableListenerHandle(self.test_table, listener_func)
        table_listener_handle.start(do_replay=True)
        time.sleep(1)
        self.assertGreaterEqual(len(t_update.added), 1)
        self.check_result(t_update.added)
        self.check_result(t_update.removed)
        self.check_result(t_update.modified)
        self.check_result(t_update.modified_prev)
        self.assertFalse(all(replay_recorder))

        table_listener_handle.stop()
        listener_called = len(replay_recorder)
        time.sleep(1)
        self.assertEqual(listener_called, len(replay_recorder))

        # del table_listener_handle
        # pprint(f"{'==='*10}TableListenerHandle has been deleted!")
        # for _ in range(2):
        #     _JSystem = jpy.get_type("java.lang.System")
        #     _JSystem.gc()
        #
        # for i in range(50):
        #     time.sleep(1)
        #     pprint(f"the table listener has been called {len(replay_recorder)} times.")

    def test_listener_func_chunk(self):
        replay_recorder = []
        t_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                       Shifted=None, modified_columns=None)

        def listener_func(update, is_replay):
            t_update.added = []
            t_update.removed = []
            t_update.modified = []
            t_update.modified_prev = []
            for chunk in update.added_chunks(chunk_size=128):
                t_update.added.append(chunk)
            for chunk in update.removed_chunks(chunk_size=128):
                t_update.removed.append(chunk)
            for chunk in update.modified_chunks(chunk_size=128):
                t_update.modified.append(chunk)
            for chunk in update.modified_prev_chunks(chunk_size=128):
                t_update.modified_prev.append(chunk)
            replay_recorder.append(is_replay)

        table_listener_handle = listen(self.test_table, listener_func, do_replay=True)
        time.sleep(1)
        self.assertGreaterEqual(len(t_update.added), 1)
        self.check_result(t_update.added)
        self.check_result(t_update.removed)
        self.check_result(t_update.modified)
        self.check_result(t_update.modified_prev)
        self.assertFalse(all(replay_recorder))

        table_listener_handle.stop()

    def test_listener_func_modified_chunk(self):
        replay_recorder = []
        t_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                       Shifted=None, modified_columns=None)
        cols = "InWindow"
        t_update.modified_columns_list = []

        def listener_func(update, is_replay):
            t_update.modified_columns_list.append(update.modified_columns)

            t_update.added = []
            t_update.removed = []
            t_update.modified = []
            t_update.modified_prev = []
            for chunk in update.added_chunks(chunk_size=1000, cols=cols):
                t_update.added.append(chunk)
            for chunk in update.removed_chunks(chunk_size=1000, cols=cols):
                t_update.removed.append(chunk)
            for chunk in update.modified_chunks(chunk_size=1000, cols=cols):
                t_update.modified.append(chunk)
            for chunk in update.modified_prev_chunks(chunk_size=1000, cols=cols):
                t_update.modified_prev.append(chunk)
            replay_recorder.append(is_replay)

        table_listener_handle = listen(self.test_table2, listener=listener_func)
        time.sleep(2)
        pprint("-----modified-----")
        pprint(t_update.modified)
        pprint("------modified-prev------")
        pprint(t_update.modified_prev)
        pprint("------modified-columns-list------")
        pprint(t_update.modified_columns_list)
        self.assertGreaterEqual(len(t_update.added), 1)
        self.check_result(t_update.added, cols)
        self.check_result(t_update.removed, cols)
        self.check_result(t_update.modified, cols)
        self.check_result(t_update.modified_prev, cols)
        self.assertFalse(all(replay_recorder))

        table_listener_handle.stop()

    def test_listener_obj_chunk(self):
        class ListenerClass(TableListener):
            def __init__(self):
                self.t_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                                Shifted=None, modified_columns=None)

            def on_update(self, update: TableUpdate, is_replay: bool):
                self.t_update.added = []
                self.t_update.removed = []
                self.t_update.modified_prev = []
                self.t_update.modified = []
                for chunk in update.added_chunks(chunk_size=4):
                    self.t_update.added.append(chunk)
                for chunk in update.removed_chunks(chunk_size=4):
                    self.t_update.removed.append(chunk)
                for chunk in update.modified_chunks(chunk_size=4):
                    self.t_update.modified.append(chunk)
                for chunk in update.modified_prev_chunks(chunk_size=4):
                    self.t_update.modified_prev.append(chunk)

        listener = ListenerClass()
        table_listener_handle = listen(self.test_table, listener)
        time.sleep(1)
        # pprint(listener.table_update.added)
        # pprint("----" * 10)
        # pprint(listener.table_update.removed)
        self.check_result(listener.t_update.added, cols="X")
        self.check_result(listener.t_update.removed)
        self.check_result(listener.t_update.modified)
        self.check_result(listener.t_update.modified_prev)

        table_listener_handle.stop()


if __name__ == "__main__":
    unittest.main()
