#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import DHError, empty_table, read_csv, time_table
from deephaven.execution_context import get_exec_ctx
from deephaven.experimental import TailInitializationFilter, time_window
from deephaven.experimental.outer_joins import full_outer_join, left_outer_join
from deephaven.update_graph import exclusive_lock
from tests.testbase import BaseTestCase


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
            self.assertEqual(
                len(rt.definition), len(t1.definition) + len(t2.definition)
            )

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
            self.assertEqual(
                len(rt.definition), len(t1.definition) + len(t2.definition)
            )

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
                t = time_window(
                    source_table, ts_col="TS", window=10**8, bool_col="InWindow"
                )

            self.assertEqual("InWindow", t.columns[-1].name)
            self.wait_ticking_table_update(t, row_count=20, timeout=60)
            self.assertIn("true", t.to_string(1000))
            self.assertIn("false", t.to_string(1000))

        with self.subTest("auto-lock"):
            source_table = time_table("PT00:00:00.01").update(["TS=now()"])
            t = time_window(
                source_table, ts_col="TS", window=10**8, bool_col="InWindow"
            )

            self.assertEqual("InWindow", t.columns[-1].name)
            self.wait_ticking_table_update(t, row_count=20, timeout=60)
            self.assertIn("true", t.to_string(1000))
            self.assertIn("false", t.to_string(1000))

    def test_tail_initialization_most_recent(self):
        from datetime import timedelta

        with self.subTest("keep rows within a period of the newest timestamp (str)"):
            source = time_table("PT00:00:00.01")
            self.wait_ticking_table_update(source, row_count=100, timeout=60)
            rt = TailInitializationFilter.most_recent(
                source, "Timestamp", "PT00:00:00.1"
            )
            self.assertTrue(rt.is_refreshing)
            self.assertLessEqual(rt.size, source.size)
            self.assertGreater(rt.size, 0)

        with self.subTest("period as int nanoseconds"):
            source = time_table("PT00:00:00.01")
            self.wait_ticking_table_update(source, row_count=100, timeout=60)
            rt = TailInitializationFilter.most_recent(source, "Timestamp", 100_000_000)
            self.assertTrue(rt.is_refreshing)
            self.assertLessEqual(rt.size, source.size)

        with self.subTest("period as datetime.timedelta"):
            source = time_table("PT00:00:00.01")
            self.wait_ticking_table_update(source, row_count=100, timeout=60)
            rt = TailInitializationFilter.most_recent(
                source, "Timestamp", timedelta(milliseconds=100)
            )
            self.assertTrue(rt.is_refreshing)
            self.assertLessEqual(rt.size, source.size)

        with self.subTest("static snapshot keeps only rows within the window"):
            source = time_table("PT00:00:00.01")
            self.wait_ticking_table_update(source, row_count=100, timeout=60)
            static = source.snapshot()
            # a static (non-refreshing) table is a single partition and is add-only
            rt = TailInitializationFilter.most_recent(static, "Timestamp", 0)
            # with a zero period only rows sharing the newest timestamp survive
            self.assertGreaterEqual(rt.size, 1)
            self.assertLessEqual(rt.size, static.size)
            self.assertFalse(rt.is_refreshing)

    def test_tail_initialization_most_recent_rows(self):
        with self.subTest("static snapshot keeps an exact number of rows"):
            source = time_table("PT00:00:00.01")
            self.wait_ticking_table_update(source, row_count=100, timeout=60)
            static = source.snapshot()
            rt = TailInitializationFilter.most_recent_rows(static, 10)
            self.assertEqual(rt.size, 10)
            self.assertFalse(rt.is_refreshing)

        with self.subTest("refreshing source stays refreshing"):
            source = time_table("PT00:00:00.01")
            self.wait_ticking_table_update(source, row_count=100, timeout=60)
            rt = TailInitializationFilter.most_recent_rows(source, 10)
            self.assertTrue(rt.is_refreshing)
            self.assertGreaterEqual(rt.size, 10)

    def test_tail_initialization_errors(self):
        with self.subTest("non-add-only source raises"):
            source = time_table("PT00:00:00.01")
            self.wait_ticking_table_update(source, row_count=10, timeout=60)
            # last_by produces modifies, so the result is not add-only
            not_add_only = source.last_by()
            with self.assertRaises(DHError):
                TailInitializationFilter.most_recent(not_add_only, "Timestamp", "PT1S")
            with self.assertRaises(DHError):
                TailInitializationFilter.most_recent_rows(not_add_only, 5)

        with self.subTest("null timestamps raise"):
            static = empty_table(10).update(["Timestamp = (Instant) null"])
            with self.assertRaises(DHError):
                TailInitializationFilter.most_recent(static, "Timestamp", "PT1S")

        with self.subTest("missing timestamp column raises"):
            static = empty_table(10).update(["X = i"])
            with self.assertRaises(DHError):
                TailInitializationFilter.most_recent(static, "Timestamp", "PT1S")


if __name__ == "__main__":
    unittest.main()
