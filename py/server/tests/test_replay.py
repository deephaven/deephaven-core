#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import DHError, new_table, TableReplayer, time
from deephaven.column import int_col, datetime_col
from tests.testbase import BaseTestCase


class ReplayTestCase(BaseTestCase):
    def historical_table_replayer(self, start_time, end_time):
        dt1 = time.to_j_instant("2000-01-01T00:00:01 ET")
        dt2 = time.to_j_instant("2000-01-01T00:00:02 ET")
        dt3 = time.to_j_instant("2000-01-01T00:00:04 ET")

        hist_table = new_table(
            [datetime_col("DateTime", [dt1, dt2, dt3]), int_col("Number", [1, 3, 6])]
        )

        hist_table2 = new_table(
            [datetime_col("DateTime", [dt1, dt2, dt3]), int_col("Number", [1, 3, 6])]
        )

        replayer = TableReplayer(start_time, end_time)
        replay_table = replayer.add_table(hist_table, "DateTime")
        replay_table2 = replayer.add_table(hist_table2, "DateTime")
        self.assert_table_equals(replay_table, replay_table2)

        replayer.start()
        self.assertTrue(replay_table.is_refreshing)
        self.assertTrue(replay_table2.is_refreshing)
        self.wait_ticking_table_update(replay_table, row_count=3, timeout=60)
        self.wait_ticking_table_update(replay_table2, row_count=3, timeout=60)
        self.assert_table_equals(replay_table, replay_table2)
        replayer.shutdown()

        with self.subTest("replayer can't be reused after shutdown."):
            with self.assertRaises(DHError) as cm:
                replayer.add_table(hist_table, "DateTime")
            self.assertIn("RuntimeError", cm.exception.root_cause)

        with self.subTest("replayer can't be restarted after shutdown."):
            with self.assertRaises(DHError):
                replayer.start()

        with self.subTest("Add table after replayer is restarted."):
            replayer = TableReplayer(start_time, end_time)
            replayer.start()
            replay_table = replayer.add_table(hist_table, "DateTime")
            self.assertTrue(replay_table.is_refreshing)
            self.wait_ticking_table_update(replay_table, row_count=3, timeout=60)
            replayer.shutdown()

    def test_historical_table_replayer_instant(self):
        start_time = time.to_j_instant("2000-01-01T00:00:00 ET")
        end_time = time.to_j_instant("2000-01-01T00:00:05 ET")

        self.historical_table_replayer(start_time, end_time)

    def test_historical_table_replayer_str(self):
        start_time = "2000-01-01T00:00:00 ET"
        end_time = "2000-01-01T00:00:05 ET"

        self.historical_table_replayer(start_time, end_time)

    def test_historical_table_replayer_datetime(self):
        start_time = time.to_datetime(time.to_j_instant("2000-01-01T00:00:00 ET"))
        end_time = time.to_datetime(time.to_j_instant("2000-01-01T00:00:05 ET"))

        self.historical_table_replayer(start_time, end_time)

if __name__ == "__main__":
    unittest.main()
