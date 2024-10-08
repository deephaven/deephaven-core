#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import time
import unittest

import numpy as np
from pyarrow import csv

from pydeephaven import DHError
from pydeephaven import SortDirection
from pydeephaven.agg import sum_, avg, pct, weighted_avg, count_, partition, median, unique, count_distinct, distinct
from pydeephaven.table import Table
from tests.testbase import BaseTestCase


class TableTestCase(BaseTestCase):
    def test_close(self):
        pa_table = csv.read_csv(self.csv_file)
        table = self.session.import_table(pa_table)
        table.close()
        self.assertTrue(table.is_closed)
        table = self.session.empty_table(10)
        table.close()
        self.assertTrue(table.is_closed)

    def test_time_table(self):
        SEC_TO_NS = 1_000_000_000
        periods = [10 * SEC_TO_NS, 300 * SEC_TO_NS, "PT10S", "PT00:05:00.000"]
        start_times = [None, "2023-01-01T11:00 ET", "2023-02-01T11:00:34.001 PT"]

        for p in periods:
            for st in start_times:
                t = self.session.time_table(period=p, start_time=st)
                column_specs = ["Col1 = i", "Col2 = i * 2"]
                t2 = t.update(formulas=column_specs)
                # time table has a default timestamp column
                self.assertEqual(len(column_specs) + 1, len(t2.schema))

    def test_update(self):
        t = self.session.time_table(period=10000000)
        column_specs = ["Col1 = i", "Col2 = i * 2"]
        t2 = t.update(formulas=column_specs)
        # time table has a default timestamp column
        self.assertEqual(len(column_specs) + 1, len(t2.schema))

    def test_to_arrow_timetable(self):
        t = self.session.time_table(period=1000000000)
        pa_table = t.to_arrow()
        self.assertIsNotNone(pa_table)

    def test_create_data_table_then_update(self):
        pa_table = csv.read_csv(self.csv_file)
        new_table = self.session.import_table(pa_table).update(formulas=['Sum = a + b + c + d'])
        pa_table2 = new_table.to_arrow()
        df = pa_table2.to_pandas()
        self.assertEquals(df.shape[1], 6)
        self.assertEquals(1000, len(df.index))

    def test_drop_columns(self):
        pa_table = csv.read_csv(self.csv_file)
        table1 = self.session.import_table(pa_table)
        column_names = []
        for f in table1.schema:
            column_names.append(f.name)
        table2 = table1.drop_columns(cols=column_names[:-1])
        self.assertEquals(1, len(table2.schema))

    def test_usv(self):
        ops = [
            Table.update,
            Table.lazy_update,
            Table.view,
            Table.update_view,
            Table.select
        ]
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        for op in ops:
            result_table = op(test_table, formulas=["a", "c", "Sum = a + b + c + d"])
            self.assertIsNotNone(result_table)
            self.assertTrue(len(result_table.schema) >= 3)
            self.assertEqual(result_table.size, pa_table.num_rows)

    def test_select_distinct(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        unique_table = test_table.select_distinct(cols=["a"])
        self.assertLessEqual(unique_table.size, pa_table.num_rows)
        unique_table = test_table.select_distinct(cols=[])
        self.assertLessEqual(unique_table.size, pa_table.num_rows)

    def test_where(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        filtered_table = test_table.where(["a > 10", "b < 100"])
        self.assertLessEqual(filtered_table.size, pa_table.num_rows)

    def test_sort(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        sorted_table = test_table.sort(order_by=["a", "b"], order=[SortDirection.DESCENDING])
        df = sorted_table.to_arrow().to_pandas()

        self.assertTrue(df.iloc[:, 0].is_monotonic_decreasing)

    def test_sort_desc(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        sorted_table = test_table.sort_descending(order_by=["a", "b"])
        df = sorted_table.to_arrow().to_pandas()

        self.assertTrue(df.iloc[:, 0].is_monotonic_decreasing)

    def test_head_tail(self):
        ops = [Table.head,
               Table.tail]
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        for op in ops:
            result_table = op(test_table, num_rows=50)
            self.assertEqual(result_table.size, 50)

    def test_natural_join(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        left_table = test_table.drop_columns(["d", "e"])
        right_table = test_table.drop_columns(["b", "c"])
        with self.assertRaises(DHError):
            result_table = left_table.natural_join(right_table, on=["a"], joins=["RD = d", "e"])
            self.assertEqual(test_table.size, result_table.size)

    def test_exact_join(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        left_table = test_table.drop_columns(["d", "e"])
        right_table = test_table.drop_columns(["b", "c"])
        with self.assertRaises(DHError):
            result_table = left_table.exact_join(right_table, on=["a"], joins=["d", "e"])
            self.assertEqual(test_table.size, result_table.size)

    def test_cross_join(self):
        pa_table = csv.read_csv(self.csv_file)
        left_table = self.session.import_table(pa_table)
        right_table = left_table.where(["a % 2 > 0 && b % 3 == 1"]).drop_columns(cols=["b", "c", "d"])
        left_table = left_table.drop_columns(cols=["e"])
        result_table = left_table.join(right_table, on=["a"], joins=["e"])
        self.assertTrue(result_table.size < left_table.size)
        result_table = left_table.join(right_table, joins=["e"])
        self.assertTrue(result_table.size > left_table.size)

    def test_as_of_join(self):
        tt_left = self.session.time_table(period=100000).update(formulas=["Col1=i"])
        tt_right = self.session.time_table(period=200000).update(formulas=["Col1=i"])
        time.sleep(2)
        left_table = self.session.import_table(tt_left.to_arrow())
        right_table = self.session.import_table(tt_right.to_arrow())
        result_table = left_table.aj(right_table, on=["Col1", "Timestamp"])
        self.assertGreater(result_table.size, 0)
        self.assertLessEqual(result_table.size, left_table.size)
        result_table = left_table.raj(right_table, on=["Col1", "Timestamp"])
        self.assertGreater(result_table.size, 0)
        self.assertLessEqual(result_table.size, left_table.size)

    def test_head_tail_by(self):
        ops = [Table.head_by,
               Table.tail_by]
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        for op in ops:
            result_table = op(test_table, num_rows=1, by=["a"])
            self.assertLessEqual(result_table.size, test_table.size)

    def test_group(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        grouped_table = test_table.group_by(by=["a", "c"])
        self.assertLessEqual(grouped_table.size, test_table.size)
        grouped_table = test_table.group_by()
        self.assertLessEqual(grouped_table.size, 1)

    def test_ungroup(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        grouped_table = test_table.group_by(by=["a", "c"])
        ungrouped_table = grouped_table.ungroup(cols=["b"])
        self.assertLessEqual(ungrouped_table.size, test_table.size)

    def test_dedicated_agg(self):
        ops = [Table.first_by, Table.last_by, Table.sum_by, Table.avg_by, Table.std_by, Table.var_by, Table.median_by,
               Table.min_by, Table.max_by]

        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        num_distinct_a = test_table.select_distinct(cols=["a", "b"]).size
        for op in ops:
            result_table = op(test_table, by=["a", "b"])
            self.assertEqual(result_table.size, num_distinct_a)

        for op in ops:
            result_table = op(test_table, by=[])
            self.assertEqual(result_table.size, 1)

    def test_count_by(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        num_distinct_a = test_table.select_distinct(cols=["a"]).size
        result_table = test_table.count_by(col="b", by=["a"])
        self.assertEqual(result_table.size, num_distinct_a)

    def test_snapshot(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        result_table = test_table.snapshot()
        self.assertEqual(test_table.schema, result_table.schema)
        self.assertEqual(test_table.size, result_table.size)

        test_table = self.session.time_table(period=10000000).update(formulas=["Col1 = i", "Col2 = i * 2"])
        result_table = test_table.snapshot()
        self.assertEqual(test_table.schema, result_table.schema)

    def test_snapshot_when(self):
        source_table = (self.session.time_table(period=10_000_000)
                        .update(formulas=["Col1= i", "Col2 = i * 2"]).drop_columns(["Timestamp"]))
        trigger_table = self.session.time_table(period=1_000_000_000)
        result_table = source_table.snapshot_when(trigger_table=trigger_table, stamp_cols=["Timestamp"],
                                                  initial=True, incremental=True, history=False)
        self.assertEqual(len(source_table.schema) + 1, len(result_table.schema))

        result_table = source_table.snapshot_when(trigger_table=trigger_table, stamp_cols=["Timestamp"],
                                                  initial=False, incremental=False, history=True)
        self.assertEqual(len(source_table.schema) + 1, len(result_table.schema))

        with self.assertRaises(DHError):
            result_table = source_table.snapshot_when(trigger_table=trigger_table, stamp_cols=["Timestamp"],
                                                      initial=True, incremental=False, history=True)
        source_table = trigger_table = result_table = None

    def test_agg_by(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        num_distinct_a = test_table.select_distinct(cols=["a"]).size

        aggs = [sum_(cols=["SumC=c"]),
                avg(cols=["AvgB = b", "AvgD = d"]),
                pct(percentile=0.5, cols=["PctC = c"]),
                weighted_avg(wcol="d", cols=["WavGD = d"]),
                count_(col="ca"),
                partition(col="aggPartition"),
                ]

        result_table = test_table.agg_by(aggs=aggs, by=["a"])
        self.assertEqual(result_table.size, num_distinct_a)

        aggs = [sum_(),
                avg(),
                pct(percentile=0.5),
                weighted_avg(wcol="d"),
                ]

        with self.assertRaises(DHError) as cm:
            test_table.agg_by(aggs=aggs, by=["a"])

    def test_agg_all_by(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        num_distinct_a = test_table.select_distinct(cols=["a"]).size

        aggs = [sum_(),
                avg(),
                pct(percentile=0.5),
                weighted_avg(wcol="d"),
                ]
        for agg in aggs:
            with self.subTest(agg):
                result_table = test_table.agg_all_by(agg=agg, by=["a"])
                self.assertEqual(result_table.size, num_distinct_a)

        # cols will be ignored
        aggs = [sum_(cols=["SumC=c"]),
                avg(cols=["AvgB = b", "AvgD = d"]),
                pct(percentile=0.5, cols=["PctC = c"]),
                weighted_avg(wcol="d", cols=["WavGD = d"]),
                ]

        for agg in aggs:
            with self.subTest(agg):
                result_table = test_table.agg_all_by(agg=agg, by=["a"])
                self.assertEqual(result_table.size, num_distinct_a)

        with self.subTest("unsupported aggregations"):
            with self.assertRaises(DHError) as cm:
                test_table.agg_all_by(agg=partition(col="aggPartition"), by=["a"])
            with self.assertRaises(DHError) as cm:
                test_table.agg_all_by(agg=count_(col="ca"), by=["a"])

    def test_where_in(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)

        unique_table = test_table.head(num_rows=50).select_distinct(
            cols=["a", "c"]
        )

        with self.subTest("where-in filter"):
            result_table = test_table.where_in(unique_table, cols=["c"])
            self.assertLessEqual(unique_table.size, result_table.size)

        with self.subTest("where-not-in filter"):
            result_table2 = test_table.where_not_in(unique_table, cols=["c"])
            self.assertEqual(result_table.size, test_table.size - result_table2.size)

    def test_meta_table(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table).drop_columns(["e"])
        self.assertEqual(len(test_table.schema), len(test_table.meta_table.to_arrow()))

    def test_agg_with_options(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table).update(["b = a % 10 > 5 ? null : b", "c = c % 10",
                                                                 "d = (char)i"])

        aggs = [
            median(cols=["ma = a", "mb = b"], average_evenly_divided=False),
            pct(0.20, cols=["pa = a", "pb = b"], average_evenly_divided=True),
            unique(cols=["ua = a", "ub = b"], include_nulls=True, non_unique_sentinel=np.int16(-1)),
            unique(cols=["ud = d"], include_nulls=True, non_unique_sentinel=np.uint16(128)),
            count_distinct(cols=["csa = a", "csb = b"], count_nulls=True),
            distinct(cols=["da = a", "db = b"], include_nulls=True),
            ]
        rt = test_table.agg_by(aggs=aggs, by=["c"])
        self.assertEqual(rt.size, test_table.select_distinct(["c"]).size)

        with self.assertRaises(TypeError):
            aggs = [unique(cols=["ua = a", "ub = b"], include_nulls=True, non_unique_sentinel=np.uint32(128))]

        aggs_default = [
            median(cols=["ma = a", "mb = b"]),
            pct(0.20, cols=["pa = a", "pb = b"]),
            unique(cols=["ua = a", "ub = b"]),
            count_distinct(cols=["csa = a", "csb = b"]),
            distinct(cols=["da = a", "db = b"]),
        ]

        for agg_option, agg_default in zip(aggs, aggs_default):
            with self.subTest(agg_option):
                rt_option = test_table.agg_by(aggs=agg_option, by=["c"])
                rt_default = test_table.agg_by(aggs=agg_default, by=["c"])
                pa_table1 = rt_option.to_arrow()
                pa_table2 = rt_default.to_arrow()
                self.assertNotEqual(pa_table2, pa_table1)


if __name__ == '__main__':
    unittest.main()
