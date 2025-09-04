#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, time_table, empty_table, new_table, update_graph, DHError
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg
from deephaven.table import keyed_transpose, NewColumnBehaviorType
from tests.testbase import BaseTestCase
from deephaven.execution_context import get_exec_ctx

class KeyedTransposeTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        genArr = ["a=`a`+(i%3)","b=`b`+(i%4)","c=i%100","d=i%5"]
        self.static_table = empty_table(20).update(genArr)
        self.test_update_graph = get_exec_ctx().update_graph
        with update_graph.exclusive_lock(self.test_update_graph):
            self.ticking_table = time_table("PT00:00:00.001").update(genArr)

    def tearDown(self) -> None:
        self.static_table = None
        self.ticking_table = None
        super().tearDown()

    def test_static_no_initial_groups(self):
        t = keyed_transpose(self.static_table, [agg.count_("Count")], ["a"], ["b"])
        self.assertFalse(t.is_refreshing)
        self.assertEqual(t.size, 3)

    def test_static_with_initial_groups(self):
        initial_groups = (new_table([string_col("b", ["b0", "b1", "b2", "b3"])])
            .join(self.static_table.select_distinct(["a"])))
        t = keyed_transpose(self.static_table, [agg.count_("Count")], ["a"], ["b"],
            initial_groups)
        self.assertFalse(t.is_refreshing)
        self.assertEqual(t.size, 3)

    def test_ticking(self):
        initial_groups = (new_table([string_col("b", ["b0", "b1", "b2", "b3"])])
            .join(self.static_table.select_distinct(["a"])))
        t = keyed_transpose(self.ticking_table, [agg.count_("Count")], ["a"], ["b"],
            initial_groups, NewColumnBehaviorType.IGNORE)
        self.assertTrue(t.is_refreshing)
        with update_graph.exclusive_lock(self.test_update_graph):
            self.assertEqual(t.size, 3)

    def test_errors(self):
        with self.assertRaises(DHError):
            t = keyed_transpose(self.static_table, [], [], [])

        with self.assertRaises(DHError):
            t = keyed_transpose(self.static_table, [agg.count_("Count")], [], [])

        with self.assertRaises(DHError):
            t = keyed_transpose(self.static_table, [agg.count_("Count")], ["a"], [])


if __name__ == '__main__':
    unittest.main()
