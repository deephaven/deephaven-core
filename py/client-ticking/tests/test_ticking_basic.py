#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

import pydeephaven as dh
import time
import sys

class TickingBasicTestCase(unittest.TestCase):
    def test_ticking_basic_time_table(self):
        session = dh.Session()
        fifth_second_in_nanos = 200 * 1000 * 1000
        table = session.time_table(period=fifth_second_in_nanos).update(formulas=["Col1 = i"])
        session.bind_table(name="my_ticking_table", table=table)
        table_added_last_col1_seen = -1
        table_added_update_count = 0
        def update_table_added(added):
            nonlocal table_added_update_count
            nonlocal table_added_last_col1_seen
            table_added_update_count += 1
            if not added:
                # Only allow the initial snapshot (if it was fast enough) to be empty.
                self.assertTrue(table_added_update_count == 1)
                return
            for value in added['Col1'].to_pylist():
                prev = table_added_last_col1_seen
                table_added_last_col1_seen = value
                if prev != -1:
                    self.assertTrue(prev + 1 == table_added_last_col1_seen)
        listener_handle = dh.listen(table, lambda update : update_table_added(update.added('Col1')))
        listener_handle.start()
        seen_rows = 0
        start_seconds = time.time()
        # Wait until we see a given number of updates or timeout.  Note the callback for the updates
        # is already checking they are of the right form.
        col1_target = 10
        timeout_seconds = 10
        while True:
            time.sleep(1)
            if table_added_last_col1_seen >= col1_target:
                break
            now_seconds = time.time()
            self.assertTrue(now_seconds - start_seconds < timeout_seconds)  # eventually fail
        self.assertTrue(4 >= table_added_update_count)
        listener_handle.stop()
        session.close()

if __name__ == '__main__':
    unittest.main()
