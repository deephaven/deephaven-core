#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
from time import sleep

import unittest

from deephaven2 import DHError, new_table, TableReplayer
from deephaven2.column import int_col, datetime_col
from deephaven2.time import to_datetime
from tests.testbase import BaseTestCase


class ReplayTestCase(BaseTestCase):
    def test_historical_table_replayer(self):
        dt1 = to_datetime("2000-01-01T00:00:01 NY")
        dt2 = to_datetime("2000-01-01T00:00:02 NY")
        dt3 = to_datetime("2000-01-01T00:00:04 NY")

        hist_table = new_table([
            datetime_col("DateTime", [dt1, dt2, dt3]),
            int_col("Number", [1, 3, 6])]
        )

        start_time = to_datetime("2000-01-01T00:00:00 NY")
        end_time = to_datetime("2000-01-01T00:00:05 NY")

        replayer = TableReplayer(start_time, end_time)
        replay_table = replayer.add_table(hist_table, "DateTime")
        replay_table2 = replayer.add_table(hist_table, "DateTime")
        self.assertEqual(replay_table, replay_table2)

        replayer.start()
        self.assertTrue(replay_table.is_refreshing)
        self.assertTrue(replay_table2.is_refreshing)
        self.wait_ticking_table_update(replay_table, row_count=3, timeout=8)
        replayer.shutdown()

        with self.subTest("replayer can't be reused after shutdown."):
            with self.assertRaises(DHError) as cm:
                replay_table3 = replayer.add_table(hist_table, "DateTime")
            self.assertIn("RuntimeError", cm.exception.root_cause)

        with self.assertRaises(DHError) as cm:
            replayer.start()
        self.assertIn("RuntimeError", cm.exception.root_cause)

        with self.subTest("Add table after replay started."):
            replayer = TableReplayer(start_time, end_time)
            replayer.start()
            replay_table = replayer.add_table(hist_table, "DateTime")
            self.assertTrue(replay_table.is_refreshing)
            self.wait_ticking_table_update(replay_table, row_count=3, timeout=8)
            replayer.shutdown()


if __name__ == '__main__':
    unittest.main()
