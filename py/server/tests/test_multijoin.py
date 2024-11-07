#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, time_table, update_graph, DHError
from deephaven.table import MultiJoinInput, MultiJoinTable, multi_join
from tests.testbase import BaseTestCase
from deephaven.execution_context import get_exec_ctx


class MultiJoinTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.static_tableA = read_csv("tests/data/test_table.csv").rename_columns(["c1=c","d1=d","e1=e"])
        self.static_tableB = self.static_tableA.update(["c2=c1+1","d2=d1+2","e2=e1+3"]).drop_columns(["c1","d1","e1"])
        self.test_update_graph = get_exec_ctx().update_graph
        with update_graph.exclusive_lock(self.test_update_graph):
            self.ticking_tableA = time_table("PT00:00:00.001").update(
                ["a = i", "b = i*i % 13", "c1 = i * 13 % 23", "d1 = a + b", "e1 = a - b"]).drop_columns(["Timestamp"])
            self.ticking_tableB = self.ticking_tableA.update(["c2=c1+1","d2=d1+2","e2=e1+3"]).drop_columns(["c1","d1","e1"])

    def tearDown(self) -> None:
        self.static_tableA = None
        self.static_tableB = None
        self.ticking_tableA = None
        self.ticking_tableB = None
        super().tearDown()

    def test_static_simple(self):
        # Test with multiple input tables
        mj_table = multi_join(input=[self.static_tableA, self.static_tableB], on=["a","b"])

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)
        self.assertEqual(mj_table.table.size, self.static_tableB.size)

        # Test with a single input table
        mj_table = multi_join(self.static_tableA, ["a","b"])

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)


    def test_ticking_simple(self):
        # Test with multiple input tables
        mj_table = multi_join(input=[self.ticking_tableA, self.ticking_tableB], on=["a","b"])

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        with update_graph.exclusive_lock(self.test_update_graph):
            self.assertEqual(mj_table.table.size, self.ticking_tableA.size)
            self.assertEqual(mj_table.table.size, self.ticking_tableB.size)

        # Test with a single input table
        mj_table = multi_join(input=self.ticking_tableA, on=["a","b"])

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        with update_graph.exclusive_lock(self.test_update_graph):
            self.assertEqual(mj_table.table.size, self.ticking_tableA.size)


    def test_static(self):
        # Test with multiple input
        mj_input = [
            MultiJoinInput(table=self.static_tableA, on=["key1=a","key2=b"], joins=["c1","e1"]),
            MultiJoinInput(table=self.static_tableB, on=["key1=a","key2=b"], joins=["d2"])
        ]
        mj_table = multi_join(mj_input)

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)
        self.assertEqual(mj_table.table.size, self.static_tableB.size)

        # Test with a single input
        mj_table = multi_join(MultiJoinInput(table=self.static_tableA, on=["key1=a","key2=b"], joins="c1"))

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)


    def test_ticking(self):
        # Test with multiple input
        mj_input = [
            MultiJoinInput(table=self.ticking_tableA, on=["key1=a","key2=b"], joins=["c1","e1"]),
            MultiJoinInput(table=self.ticking_tableB, on=["key1=a","key2=b"], joins=["d2"])
        ]
        mj_table = multi_join(mj_input)

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        with update_graph.exclusive_lock(self.test_update_graph):
            self.assertEqual(mj_table.table.size, self.ticking_tableA.size)
            self.assertEqual(mj_table.table.size, self.ticking_tableB.size)

        # Test with a single input
        mj_table = multi_join(input=MultiJoinInput(table=self.ticking_tableA, on=["key1=a","key2=b"], joins="c1"))

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        with update_graph.exclusive_lock(self.test_update_graph):
            self.assertEqual(mj_table.table.size, self.ticking_tableA.size)

    def test_errors(self):
        # Assert the exception is raised when providing MultiJoinInput and the on parameter is not None (omitted).
        mj_input = [
            MultiJoinInput(table=self.ticking_tableA, on=["key1=a","key2=b"], joins=["c1","e1"]),
            MultiJoinInput(table=self.ticking_tableB, on=["key1=a","key2=b"], joins=["d2"])
        ]
        with self.assertRaises(DHError):
            mj_table = multi_join(mj_input, on=["key1=a","key2=b"])


if __name__ == '__main__':
    unittest.main()
