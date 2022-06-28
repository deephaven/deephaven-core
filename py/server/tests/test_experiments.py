#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import time_table
from deephaven.ugp import exclusive_lock
from deephaven.experimental import time_window
from tests.testbase import BaseTestCase


class ExperimentalTestCase(BaseTestCase):

    def test_time_window(self):
        with exclusive_lock():
            source_table = time_table("00:00:00.01").update(["TS=currentTime()"])
            t = time_window(source_table, ts_col="TS", window=10**8, bool_col="InWindow")

        self.assertEqual("InWindow", t.columns[-1].name)
        self.wait_ticking_table_update(t, row_count=20, timeout=60)
        self.assertIn("true", t.to_string(1000))
        self.assertIn("false", t.to_string(1000))


if __name__ == '__main__':
    unittest.main()
