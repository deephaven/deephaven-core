#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import time
import unittest

from deephaven2 import time_table
from deephaven2.table_listener import listen, TableListener

from tests.testbase import BaseTestCase


class TableListenerTestCase(BaseTestCase):
    def setUp(self) -> None:
        with self.ugp_lock_exclusive():
            self.table = time_table("00:00:01").update(["X=i"]).tail(5)
        self.table_change_processed = None

    def test_shift_oblivious_listener_obj(self):
        class ListenerClass(TableListener):
            def __init__(self):
                self.table_change_added = None
                self.table_change_removed = None
                self.table_change_modified = None

            def onUpdate(self, added, removed, modified):
                self.table_change_added = added
                self.table_change_removed = removed
                self.table_change_modified = modified

            def has_table_changed(self):
                return (
                    self.table_change_removed
                    or self.table_change_modified
                    or self.table_change_added
                )

        listener = ListenerClass()
        table_listener_handle = listen(self.table, listener)
        time.sleep(2)
        self.assertTrue(listener.has_table_changed())
        table_listener_handle.deregister()

    def test_shift_oblivious_listener_func(self):
        def listener_func(added, removed, modified):
            added_iterator = added.iterator()
            while added_iterator.hasNext():
                idx = added_iterator.nextLong()
                ts = self.table.j_table.getColumnSource("Timestamp").get(idx)
                x = self.table.j_table.getColumnSource("X").get(idx)
                self.table_change_processed = ts or x

            removed_iterator = removed.iterator()
            while removed_iterator.hasNext():
                idx = removed_iterator.nextLong()
                ts = self.table.j_table.getColumnSource("Timestamp").getPrev(idx)
                x = self.table.j_table.getColumnSource("X").getPrev(idx)
                self.table_change_processed = ts or x

        table_listener_handle = listen(self.table, listener_func)
        time.sleep(2)
        self.assertTrue(self.table_change_processed)
        table_listener_handle.deregister()

    def test_shift_aware_listener_obj(self):
        class ListenerClass(TableListener):
            def __init__(self):
                self.table_update = None

            def onUpdate(self, update):
                self.table_update = update

            def has_table_changed(self):
                return self.table_update

        listener = ListenerClass()
        table_listener_handle = listen(self.table, listener)
        time.sleep(2)
        self.assertTrue(listener.has_table_changed())
        table_listener_handle.deregister()

    def test_shift_aware_listener_func(self):
        def listener_func(update):
            added_iterator = update.added.iterator()
            while added_iterator.hasNext():
                idx = added_iterator.nextLong()
                ts = self.table.j_table.getColumnSource("Timestamp").get(idx)
                x = self.table.j_table.getColumnSource("X").get(idx)
                self.table_change_processed = ts or x

            removed_iterator = update.removed.iterator()
            while removed_iterator.hasNext():
                idx = removed_iterator.nextLong()
                ts = self.table.j_table.getColumnSource("Timestamp").getPrev(idx)
                x = self.table.j_table.getColumnSource("X").getPrev(idx)
                self.table_change_processed = ts or x

        table_listener_handle = listen(self.table, listener_func)
        time.sleep(2)
        self.assertTrue(self.table_change_processed)
        table_listener_handle.deregister()


if __name__ == "__main__":
    unittest.main()
