#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import time
import unittest
from pprint import pprint
from types import SimpleNamespace
from typing import List, Dict, Union

import numpy
from deephaven.experimental import time_window

from deephaven import time_table
from deephaven.jcompat import to_sequence
from deephaven.table_listener import listen, TableListener
from tests.testbase import BaseTestCase


class TableListenerTestCase(BaseTestCase):

    def setUp(self) -> None:
        with self.ugp_lock_exclusive():
            self.test_table = time_table("00:00:00.001").update(["X=i%2"]).sort("X").tail(16)
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
                self.table_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                                    Shifted=None, modified_columns=None)

            def onUpdate(self, is_replay, update):
                update.chunk_size = 4
                self.table_update.added = []
                for chunk in update.added("X"):
                    self.table_update.added.append(chunk)

                self.table_update.removed = []
                for chunk in update.removed():
                    self.table_update.removed.append(chunk)

                self.table_update.modified = []
                for chunk in update.modified():
                    self.table_update.modified.append(chunk)

                self.table_update.modified_prev = []
                for chunk in update.modified_prev():
                    self.table_update.modified_prev.append(chunk)

                # self.table_update.shifted = update.shifted

        listener = ListenerClass()

        table_listener_handle = listen(self.test_table, listener, replay_initial=False)
        time.sleep(1)
        pprint(listener.table_update.added)
        pprint("----" * 10)
        pprint(listener.table_update.removed)
        self.check_result(listener.table_update.added, cols="X")
        self.check_result(listener.table_update.removed)
        self.check_result(listener.table_update.modified)
        self.check_result(listener.table_update.modified_prev)

        table_listener_handle.deregister()

    def test_listener_func(self):
        replay_recorder = []
        table_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                       Shifted=None, modified_columns=None)

        def listener_func(is_replay, update):
            update.chunk_size = 128
            table_update.added = []
            for chunk in update.added():
                table_update.added.append(chunk)

            table_update.removed = []
            for chunk in update.removed():
                table_update.removed.append(chunk)

            table_update.modified = []
            for chunk in update.modified():
                table_update.modified.append(chunk)

            table_update.modified_prev = []
            for chunk in update.modified_prev():
                table_update.modified_prev.append(chunk)

            replay_recorder.append(is_replay)

        table_listener_handle = listen(self.test_table, listener_func, replay_initial=True)
        time.sleep(1)
        # pprint(table_update.added)
        self.assertGreaterEqual(len(table_update.added), 1)
        self.check_result(table_update.added)
        self.check_result(table_update.removed)
        self.check_result(table_update.modified)
        self.check_result(table_update.modified_prev)
        self.assertFalse(all(replay_recorder))

        table_listener_handle.deregister()

    def test_listener_func_modified(self):
        replay_recorder = []
        table_update = SimpleNamespace(added=None, removed=None, modified=None, modified_prev=None,
                                       Shifted=None, modified_columns=None)
        cols = "InWindow"
        table_update.modified_columns_list = []

        def listener_func(is_replay, update):
            table_update.modified_columns_list.append(update.modified_columns)

            update.chunk_size = 1000
            table_update.added = []
            for chunk in update.added(cols):
                table_update.added.append(chunk)

            table_update.removed = []
            for chunk in update.removed(cols):
                table_update.removed.append(chunk)

            table_update.modified = []
            for chunk in update.modified(cols):
                table_update.modified.append(chunk)

            table_update.modified_prev = []
            for chunk in update.modified_prev(cols):
                table_update.modified_prev.append(chunk)

            replay_recorder.append(is_replay)

        table_listener_handle = listen(self.test_table2, listener=listener_func, replay_initial=False)
        time.sleep(2)
        pprint("-----modified-----")
        pprint(table_update.modified)
        pprint("------modified-prev------")
        pprint(table_update.modified_prev)
        pprint("------modified-columns-list------")
        pprint(table_update.modified_columns_list)
        self.assertGreaterEqual(len(table_update.added), 1)
        self.check_result(table_update.added, cols)
        self.check_result(table_update.removed, cols)
        self.check_result(table_update.modified, cols)
        self.check_result(table_update.modified_prev, cols)
        self.assertFalse(all(replay_recorder))

        table_listener_handle.deregister()


if __name__ == "__main__":
    unittest.main()
