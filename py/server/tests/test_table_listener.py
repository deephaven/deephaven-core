#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import time
import unittest
from typing import List, Union

import numpy

from deephaven import time_table
from deephaven.experimental import time_window
from deephaven.jcompat import to_sequence
from deephaven.table import Table
from deephaven.table_listener import listen, TableListener, TableListenerHandle
from deephaven.ugp import exclusive_lock
from tests.testbase import BaseTestCase


class TableUpdateRecorder:
    def __init__(self, table: Table, chunk_size: int = None, cols: Union[str, List[str]] = None):
        self.table = table
        self.chunk_size = chunk_size
        self.cols = cols

        self.added = []
        self.removed = []
        self.modified = []
        self.modified_prev = []
        self.replays = []
        self.modified_columns_list = []

    def record(self, update, is_replay):
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


def ensure_ugp_cycles(table_update_recorder: TableUpdateRecorder, cycles: int = 2):
    while len(table_update_recorder.replays) < cycles:
        time.sleep(1)


class TableListenerTestCase(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        with exclusive_lock():
            self.test_table = time_table("00:00:00.001").update(["X=i%11"]).sort("X").tail(16)
            source_table = time_table("00:00:00.001").update(["TS=currentTime()"])
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
                cols = [col.name for col in self.test_table.columns]
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


if __name__ == "__main__":
    unittest.main()
