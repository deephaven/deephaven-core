#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy
import unittest

from deephaven.agg import partition
from deephaven.table import Table, PartitionedTable

from deephaven.filters import Filter

from deephaven import read_csv, DHError, new_table, ugp, time_table
from tests.testbase import BaseTestCase


def transform_func(t: Table) -> Table:
    return t.update("f = a + b")


def partitioned_transform_func(t: Table, ot: Table) -> Table:
    return t.natural_join(ot, on=["a", "b"], joins=["f"])


class Transformer:
    @staticmethod
    def apply(t: Table) -> Table:
        return t.update("f = a + b")


class PartitionedTransformer:
    def apply(self, t: Table, ot: Table) -> Table:
        return t.natural_join(ot, on=["a", "b"], joins=["f"])


class PartitionedTableTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv").tail(num_rows=100)
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
        keys = [967, 203]
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

    def test_transform(self):
        _JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
        context = _JExecutionContext.newBuilder() \
                .captureQueryCompiler()           \
                .captureQueryLibrary()            \
                .emptyQueryScope()                \
                .build().open()
        pt = self.partitioned_table.transform(transform_func)
        self.assertIn("f", [col.name for col in pt.constituent_table_columns])

        pt = self.partitioned_table.transform(Transformer)
        self.assertIn("f", [col.name for col in pt.constituent_table_columns])

        with self.assertRaises(DHError) as cm:
            pt = self.partitioned_table.transform(lambda t, t1: t.join(t1))
        self.assertRegex(str(cm.exception), r"missing .* argument")
        context.close()

    def test_partitioned_transform(self):
        _JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
        context = _JExecutionContext.newBuilder() \
                .captureQueryCompiler()           \
                .captureQueryLibrary()            \
                .emptyQueryScope()                \
                .build().open()
        other_pt = self.partitioned_table.transform(transform_func)
        pt = self.partitioned_table.partitioned_transform(other_pt, partitioned_transform_func)
        self.assertIn("f", [col.name for col in pt.constituent_table_columns])

        pt = self.partitioned_table.partitioned_transform(other_pt, PartitionedTransformer())
        self.assertIn("f", [col.name for col in pt.constituent_table_columns])
        context.close()

    def test_partition_agg(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
        self.wait_ticking_table_update(test_table, row_count=1, timeout=5)
        agg = partition("aggPartition", include_by_columns=True)
        pt = PartitionedTable.from_partitioned_table(test_table.agg_by(agg, ["Y"]))
        self.assertEqual(['Y'], pt.key_columns)
        # includes Timestamp column
        self.assertEqual(4, len(pt.constituent_table_columns))

        agg = partition("aggPartition", include_by_columns=False)
        pt = PartitionedTable.from_partitioned_table(test_table.agg_by(agg, ["Y"]))
        self.assertEqual(['Y'], pt.key_columns)
        print(pt.constituent_table_columns)
        # includes Timestamp column, no "Y"
        self.assertEqual(3, len(pt.constituent_table_columns))

    def test_from_partitioned_table(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])

        pt = test_table.partition_by("Y")
        self.assertTrue(pt.is_refreshing)

        agg = partition("aggPartition")
        agg_table = test_table.agg_by(agg, ["Y"])
        pt1 = PartitionedTable.from_partitioned_table(
            table=agg_table,
            key_cols="Y",
            unique_keys=True,
            constituent_column="aggPartition",
            constituent_table_columns=test_table.columns,
            constituent_changes_permitted=True,
        )
        self.assertEqual(pt.key_columns, pt1.key_columns)
        self.assertEqual(len(pt.constituent_table_columns), len(pt1.constituent_table_columns))
        self.assertTrue(pt1.is_refreshing)

        with self.assertRaises(DHError) as cm:
            PartitionedTable.from_partitioned_table(
                table=agg_table,
                key_cols="Y",
                unique_keys=True,
                constituent_column="Non-existing",
                constituent_table_columns=test_table.columns,
                constituent_changes_permitted=True,
            )
        self.assertIn("no column named", str(cm.exception))

    def test_from_constituent_tables(self):
        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
            test_table1 = time_table("00:00:01").update(["X=i", "Y=i%23", "Z=X*Y"])
            test_table2 = time_table("00:00:00.001").update(["X=i", "Y=i%23", "Z=`foo`"])
            test_table3 = time_table("00:00:00.001").update(["X=i", "Y=i%23", "Z=(int)(X*Y)"])

        pt = PartitionedTable.from_constituent_tables([test_table, test_table1])
        self.assertEqual("__CONSTITUENT__", pt.constituent_column)

        with self.subTest("Incompatible Table Definition"):
            with self.assertRaises(DHError) as cm:
                pt = PartitionedTable.from_constituent_tables([test_table, test_table1, test_table2])
            self.assertIn("IncompatibleTableDefinitionException", str(cm.exception))

        with self.subTest("Compatible table definition"):
            pt = PartitionedTable.from_constituent_tables([test_table, test_table1, test_table3], test_table.columns)

    def test_keys(self):
        keys_table = self.partitioned_table.keys()
        select_distinct_table = self.test_table.select_distinct(["c", "e"])
        self.assertEqual(keys_table.size, select_distinct_table.size)

        with ugp.shared_lock():
            test_table = time_table("00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])
        pt = test_table.partition_by("Y")
        self.wait_ticking_table_update(test_table, row_count=20, timeout=5)
        keys_table = pt.keys()
        select_distinct_table = test_table.select_distinct(["Y"])
        self.assertEqual(keys_table.size, select_distinct_table.size)


if __name__ == '__main__':
    unittest.main()
