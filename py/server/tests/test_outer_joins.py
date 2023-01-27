#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, time_table
from deephaven.outer_joins import full_outer_join, left_outer_join
from tests.testbase import BaseTestCase


class TableOuterJoinsTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_full_outer_join(self):
        t1 = time_table("00:00:00.001").update(["a = i", "b = i * 2"])
        t2 = time_table("00:00:00.001").update(["a = i", "c = i * 2"])
        self.wait_ticking_table_update(t1, row_count=100, timeout=5)
        self.wait_ticking_table_update(t2, row_count=100, timeout=5)
        rt = full_outer_join(t1, t2, on="a", joins="c")
        self.assertEqual(4, len(rt.columns))
        self.assertTrue(rt.is_refreshing)
        self.wait_ticking_table_update(rt, row_count=100, timeout=5)

    def test_left_outer_join(self):
        t1 = time_table("00:00:00.001").update(["a = i", "b = i * 2"])
        t2 = time_table("00:00:00.001").update(["a = i", "c = i * 2"])
        self.wait_ticking_table_update(t1, row_count=100, timeout=5)
        self.wait_ticking_table_update(t2, row_count=100, timeout=5)
        rt = left_outer_join(t1, t2, on="a", joins="c")
        self.assertEqual(4, len(rt.columns))
        self.assertTrue(rt.is_refreshing)
        self.wait_ticking_table_update(rt, row_count=100, timeout=5)


if __name__ == '__main__':
    unittest.main()
