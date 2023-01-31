#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import time_table, empty_table
from deephaven.ugp import exclusive_lock
from deephaven.experimental import time_window
from tests.testbase import BaseTestCase
from deephaven import read_csv
from deephaven.experimental.outer_joins import full_outer_join, left_outer_join


class ExperimentalTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_full_outer_join(self):
        with self.subTest("full outer join with matching keys"):
            t1 = time_table("00:00:00.001").update(["a = i", "b = i * 2"])
            t2 = time_table("00:00:00.001").update(["a = i", "c = i * 2"])
            self.wait_ticking_table_update(t1, row_count=100, timeout=5)
            self.wait_ticking_table_update(t2, row_count=100, timeout=5)
            rt = full_outer_join(t1, t2, on="a", joins="c")
            self.assertEqual(4, len(rt.columns))
            self.assertTrue(rt.is_refreshing)
            self.wait_ticking_table_update(rt, row_count=100, timeout=5)

        with self.subTest("full outer join with no matching keys"):
            t1 = empty_table(2).update("X = i")
            rt = full_outer_join(self.test_table, t1, on=[])
            self.assertEqual(rt.size, t1.size * self.test_table.size)

    def test_left_outer_join(self):
        with self.subTest("left outer join with matching keys"):
            t1 = time_table("00:00:00.001").update(["a = i", "b = i * 2"])
            t2 = time_table("00:00:00.001").update(["a = i", "c = i * 2"])
            self.wait_ticking_table_update(t1, row_count=100, timeout=5)
            self.wait_ticking_table_update(t2, row_count=100, timeout=5)
            rt = left_outer_join(t1, t2, on="a", joins="c")
            self.assertEqual(4, len(rt.columns))
            self.assertTrue(rt.is_refreshing)
            self.wait_ticking_table_update(rt, row_count=100, timeout=5)

        with self.subTest("left outer join with no matching keys"):
            t1 = empty_table(2).update("X = i")
            rt = left_outer_join(self.test_table, t1, on=[])
            self.assertEqual(rt.size, t1.size * self.test_table.size)

    def test_time_window(self):
        with exclusive_lock():
            source_table = time_table("00:00:00.01").update(["TS=currentTime()"])
            t = time_window(source_table, ts_col="TS", window=10 ** 8, bool_col="InWindow")

        self.assertEqual("InWindow", t.columns[-1].name)
        self.wait_ticking_table_update(t, row_count=20, timeout=60)
        self.assertIn("true", t.to_string(1000))
        self.assertIn("false", t.to_string(1000))


if __name__ == '__main__':
    unittest.main()
