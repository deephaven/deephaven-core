#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

import pydeephaven as dh
import pydeephaven_ticking.table_listener as dhtl
import time

class TickingBasicTestCase(unittest.TestCase):
    def test_ticking_basic_time_table(self):
        session = dh.Session()
        half_second_in_nanos = 500 * 1000 * 1000
        table = session.time_table(period=half_second_in_nanos).update(formulas=["Col1 = i"])
        table_added_count = 0
        def update_table_added(added):
            nonlocal table_added_count
            table_added_count += len(added)
        listener_handle = dhtl.listen(table, lambda update : update_table_added(update.added()))
        listener_handle.start()
        time.sleep(3)
        self.assertTrue(4 <= table_added_count and table_added_count <= 10)
        listener_handle.stop()
        session.close()

if __name__ == '__main__':
    unittest.main()
