#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

import pydeephaven as dh
import time

class TickingBasicTestCase(unittest.TestCase):
    def test_ticking_basic_time_table(self):
        session = dh.Session()
        half_second_in_nanos = 500 * 1000 * 1000
        table = session.time_table(period=half_second_in_nanos).update(formulas=["Col1 = i"])
        table_added_last_col1_seen = -1
        table_added_update_count = 0
        def update_table_added(added):
            nonlocal table_added_update_count
            nonlocal table_added_last_col1_seen
            for value in added.to_pylist():
                prev = table_added_last_col1_seen
                table_added_last_col1_seen = value
                if prev != -1:
                    self.assertTrue(prev + 1 == table_added_last_col1_seen)
            table_added_update_count += 1
        listener_handle = dh.listen(table, lambda update : update_table_added(update.added('Col1')))
        listener_handle.start()
        time.sleep(3)
        self.assertTrue(2 >= table_added_update_count)
        listener_handle.stop()
        session.close()

if __name__ == '__main__':
    unittest.main()
