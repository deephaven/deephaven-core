#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import time_table, empty_table, DHError
from deephaven.update_graph import exclusive_lock
from deephaven.experimental import time_window
from deephaven.execution_context import get_exec_ctx
from tests.testbase import BaseTestCase
from deephaven import read_csv
from deephaven.experimental.outer_joins import full_outer_join, left_outer_join


class ExperimentalTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")
        self.test_update_graph = get_exec_ctx().update_graph

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_full_outer_join(self):
        with self.subTest("full outer join with matching keys"):
            t1 = time_table("PT00:00:00.001").update(["a = i", "b = i * 2"])
            t2 = empty_table(100).update(["c = i", "d = i * 2"])
            self.wait_ticking_table_update(t1, row_count=100, timeout=5)
            rt = full_outer_join(t1, t2, on="a = c")
            self.assertTrue(rt.is_refreshing)
            self.wait_ticking_table_update(rt, row_count=100, timeout=5)
            self.assertEqual(len(rt.definition), len(t1.definition) + len(t2.definition))

        with self.subTest("full outer join with no matching keys"):
            t1 = empty_table(2).update(["X = i", "a = i"])
            rt = full_outer_join(self.test_table, t1, joins=["Y = a"])
            self.assertEqual(rt.size, t1.size * self.test_table.size)
            self.assertEqual(len(rt.definition), 1 + len(self.test_table.definition))

        with self.subTest("Conflicting column names"):
            with self.assertRaises(DHError) as cm:
                rt = full_outer_join(self.test_table, t1)
            self.assertRegex(str(cm.exception), r"Conflicting column names")

    def test_left_outer_join(self):
        with self.subTest("left outer join with matching keys"):
            t1 = time_table("PT00:00:00.001").update(["a = i", "b = i * 2"])
            t2 = empty_table(100).update(["c = i", "d = i * 2"])
            self.wait_ticking_table_update(t1, row_count=100, timeout=5)
            rt = left_outer_join(t1, t2, on="a = c")
            self.assertTrue(rt.is_refreshing)
            self.wait_ticking_table_update(rt, row_count=100, timeout=5)
            self.assertEqual(len(rt.definition), len(t1.definition) + len(t2.definition))

        with self.subTest("left outer join with no matching keys"):
            t1 = empty_table(2).update(["X = i", "a = i"])
            rt = left_outer_join(self.test_table, t1, joins=["Y = a"])
            self.assertEqual(rt.size, t1.size * self.test_table.size)
            self.assertEqual(len(rt.definition), 1 + len(self.test_table.definition))

        with self.subTest("Conflicting column names"):
            with self.assertRaises(DHError) as cm:
                rt = left_outer_join(self.test_table, t1)
            self.assertRegex(str(cm.exception), r"Conflicting column names")

    def test_time_window(self):
        with self.subTest("user-explicit lock"):
            with exclusive_lock(self.test_update_graph):
                source_table = time_table("PT00:00:00.01").update(["TS=now()"])
                t = time_window(source_table, ts_col="TS", window=10 ** 8, bool_col="InWindow")

            self.assertEqual("InWindow", t.columns[-1].name)
            self.wait_ticking_table_update(t, row_count=20, timeout=60)
            self.assertIn("true", t.to_string(1000))
            self.assertIn("false", t.to_string(1000))

        with self.subTest("auto-lock"):
            source_table = time_table("PT00:00:00.01").update(["TS=now()"])
            t = time_window(source_table, ts_col="TS", window=10 ** 8, bool_col="InWindow")

            self.assertEqual("InWindow", t.columns[-1].name)
            self.wait_ticking_table_update(t, row_count=20, timeout=60)
            self.assertIn("true", t.to_string(1000))
            self.assertIn("false", t.to_string(1000))

if __name__ == '__main__':
    unittest.main()
