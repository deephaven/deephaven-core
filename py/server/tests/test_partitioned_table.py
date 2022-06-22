#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven.filters import Filter

from deephaven import read_csv, DHError, new_table
from tests.testbase import BaseTestCase


class PartitionedTableTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")
        self.partitioned_table = self.test_table.partition_by(by=["c", "e"])

    def tearDown(self):
        self.partitioned_table = None
        self.test_table = None

    def test_table(self):
        self.assertIsNotNone(self.partitioned_table.table)

    def test_key_columns(self):
        self.assertEqual(self.partitioned_table.key_columns, ["c", "e"])

    def test_constituent_column(self):
        self.assertEqual(self.partitioned_table.constituent_column, "__CONSTITUENT__")

    def test_unique_keys(self):
        self.assertTrue(self.partitioned_table.unique_keys)

    def test_constituent_change_permitted(self):
        self.assertFalse(self.partitioned_table.constituent_changes_permitted)

    def test_constituent_table_columns(self):
        self.assertEqual(self.test_table.columns, self.partitioned_table.constituent_table_columns)

    def test_merge(self):
        t = self.partitioned_table.merge()
        self.assert_table_equals(t, self.test_table)

    def test_filter(self):
        conditions = ["c < 0", "e > 0"]
        filters = Filter.from_(conditions)
        pt = self.partitioned_table.filter(filters)
        self.assertIsNotNone(pt)

        filters = ["c < 0", "e > 0"]
        pt = self.partitioned_table.filter(filters)
        self.assertIsNotNone(pt)

        with self.assertRaises(DHError) as cm:
            conditions = ["a > 100", "b < 1000"]
            filters = Filter.from_(conditions)
            pt = self.partitioned_table.filter(filters)
        self.assertIn("RuntimeError", str(cm.exception))

    def test_sort(self):
        new_pt = self.partitioned_table.sort(order_by=["c"])
        self.assertIsNotNone(new_pt)

        with self.assertRaises(DHError) as cm:
            new_pt = self.partitioned_table.sort(order_by=["a", "b"])
        self.assertIn("NoSuchColumnException", str(cm.exception))

        with self.assertRaises(DHError) as cm:
            new_pt = self.partitioned_table.sort(order_by=self.partitioned_table.constituent_column)
        self.assertIn("Unsupported sort on constituent column", str(cm.exception))

    def test_get_constituent(self):
        keys = [917, 167]
        self.assertIsNotNone(self.partitioned_table.get_constituent(keys))

        from deephaven.column import string_col, int_col, double_col

        houses = new_table([
            string_col("HomeType", ["Colonial", "Contemporary", "Contemporary", "Condo", "Colonial", "Apartment"]),
            int_col("HouseNumber", [1, 3, 4, 15, 4, 9]),
            string_col("StreetName", ["Test Drive", "Test Drive", "Test Drive", "Deephaven Road", "Community Circle",
                                      "Community Circle"]),
            int_col("SquareFeet", [2251, 1914, 4266, 1280, 3433, 981]),
            int_col("Price", [450000, 400000, 1250000, 300000, 600000, 275000]),
            double_col("LotSizeAcres", [0.41, 0.26, 1.88, 0.11, 0.95, 0.10])
        ])

        houses_by_type = houses.partition_by("HomeType")
        colonial_homes = houses_by_type.get_constituent("Colonial")
        self.assertIsNotNone(colonial_homes)

    def test_constituents(self):
        constituent_tables = self.partitioned_table.constituent_tables
        self.assertGreater(len(constituent_tables), 0)


if __name__ == '__main__':
    unittest.main()
