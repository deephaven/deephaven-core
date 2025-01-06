#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import unittest

from pyarrow import csv

from pydeephaven import DHError, Session
from pydeephaven.table import MultiJoinInput, multi_join
from tests.testbase import BaseTestCase


class MultiJoinTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        pa_table = csv.read_csv(self.csv_file)
        self.static_tableA = self.session.import_table(pa_table).select(["a", "b", "c1=c", "d1=d", "e1=e"])
        self.static_tableB = self.static_tableA.update(["c2=c1+1", "d2=d1+2", "e2=e1+3"]).drop_columns(
            ["c1", "d1", "e1"])
        self.ticking_tableA = self.session.time_table("PT00:00:00.001").update(
            ["a = i", "b = i*i % 13", "c1 = i * 13 % 23", "d1 = a + b", "e1 = a - b"]).drop_columns(["Timestamp"])
        self.ticking_tableB = self.ticking_tableA.update(["c2=c1+1", "d2=d1+2", "e2=e1+3"]).drop_columns(
            ["c1", "d1", "e1"])

    def tearDown(self) -> None:
        self.static_tableA = None
        self.static_tableB = None
        self.ticking_tableA = None
        self.ticking_tableB = None
        super().tearDown()

    def test_static_simple(self):
        # Test with multiple input tables
        mj_table = multi_join(input=[self.static_tableA, self.static_tableB], on=["a", "b"])

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)
        self.assertEqual(mj_table.table.size, self.static_tableB.size)

        # Test with a single input table
        mj_table = multi_join(self.static_tableA, ["a", "b"])

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)

    def test_ticking_simple(self):
        # Test with multiple input tables
        mj_table = multi_join(input=[self.ticking_tableA, self.ticking_tableB], on=["a", "b"])

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)

        # Test with a single input table
        mj_table = multi_join(input=self.ticking_tableA, on=["a", "b"])

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)

    def test_static(self):
        # Test with multiple input
        mj_input = [
            MultiJoinInput(table=self.static_tableA, on=["key1=a", "key2=b"], joins=["c1", "e1"]),
            MultiJoinInput(table=self.static_tableB, on=["key1=a", "key2=b"], joins=["d2"])
        ]
        mj_table = multi_join(mj_input)

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)
        self.assertEqual(mj_table.table.size, self.static_tableB.size)

        # Test with a single input
        mj_table = multi_join(MultiJoinInput(table=self.static_tableA, on=["key1=a", "key2=b"], joins="c1"))

        # Output table is static
        self.assertFalse(mj_table.table.is_refreshing)
        # Output table has same # rows as sources
        self.assertEqual(mj_table.table.size, self.static_tableA.size)

    def test_ticking(self):
        # Test with multiple input
        mj_input = [
            MultiJoinInput(table=self.ticking_tableA, on=["key1=a", "key2=b"], joins=["c1", "e1"]),
            MultiJoinInput(table=self.ticking_tableB, on=["key1=a", "key2=b"], joins=["d2"])
        ]
        mj_table = multi_join(mj_input)

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)

        # Test with a single input
        mj_table = multi_join(input=MultiJoinInput(table=self.ticking_tableA, on=["key1=a", "key2=b"], joins="c1"))

        # Output table is refreshing
        self.assertTrue(mj_table.table.is_refreshing)

    def test_errors(self):
        # Assert the exception is raised when providing MultiJoinInput and the on parameter is not None (omitted).
        mj_input = [
            MultiJoinInput(table=self.ticking_tableA, on=["key1=a", "key2=b"], joins=["c1", "e1"]),
            MultiJoinInput(table=self.ticking_tableB, on=["key1=a", "key2=b"], joins=["d2"])
        ]
        with self.assertRaises(DHError) as cm:
            mj_table = multi_join(mj_input, on=["key1=a", "key2=b"])
        self.assertIn("on parameter is not permitted", str(cm.exception))

        session = Session()
        t = session.time_table("PT00:00:00.001").update(
            ["a = i", "b = i*i % 13", "c1 = i * 13 % 23", "d1 = a + b", "e1 = a - b"]).drop_columns(["Timestamp"])

        # Assert the exception is raised when to-be-joined tables are not from the same session.
        mj_input = [
            MultiJoinInput(table=self.ticking_tableA, on=["key1=a", "key2=b"], joins=["c1", "e1"]),
            MultiJoinInput(table=t, on=["key1=a", "key2=b"], joins=["d2"])
        ]
        with self.assertRaises(DHError) as cm:
            mj_table = multi_join(mj_input)
        self.assertIn("all tables must be from the same session", str(cm.exception))


if __name__ == '__main__':
    unittest.main()
