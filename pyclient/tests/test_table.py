#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import time

from pyarrow import csv

from pydeephaven import ComboAggregation, SortDirection
from pydeephaven import DHError
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

    def test_update(self):
        t = self.session.time_table(period=10000000)
        column_specs = ["Col1 = i", "Col2 = i * 2"]
        t2 = t.update(column_specs=column_specs)
        # time table has a default timestamp column
        self.assertEqual(len(column_specs) + 1, len(t2.schema))

    def test_snapshot_timetable(self):
        t = self.session.time_table(period=10000000)
        time.sleep(1)
        pa_table = t.snapshot()
        self.assertGreaterEqual(pa_table.num_rows, 1)

    def test_create_data_table_then_update(self):
        pa_table = csv.read_csv(self.csv_file)
        new_table = self.session.import_table(pa_table).update(column_specs=['Sum = a + b + c + d'])
        pa_table2 = new_table.snapshot()
        df = pa_table2.to_pandas()
        self.assertEquals(df.shape[1], 6)
        self.assertEquals(1000, len(df.index))

    def test_drop_columns(self):
        pa_table = csv.read_csv(self.csv_file)
        table1 = self.session.import_table(pa_table)
        column_names = []
        for f in table1.schema:
            column_names.append(f.name)
        table2 = table1.drop_columns(column_names=column_names[:-1])
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
            result_table = op(test_table, column_specs=["a", "c", "Sum = a + b + c + d"])
            self.assertIsNotNone(result_table)
            self.assertTrue(len(result_table.schema) >= 3)
            self.assertEqual(result_table.size, pa_table.num_rows)

    def test_select_distinct(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        unique_table = test_table.select_distinct(column_names=["a"])
        self.assertLessEqual(unique_table.size, pa_table.num_rows)
        unique_table = test_table.select_distinct(column_names=[])
        self.assertLessEqual(unique_table.size, pa_table.num_rows)

    def test_where(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        filtered_table = test_table.where(["a > 10", "b < 100"])
        self.assertLessEqual(filtered_table.size, pa_table.num_rows)

    def test_sort(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        sorted_table = test_table.sort(column_names=["a", "b"], directions=[SortDirection.DESCENDING])
        df = sorted_table.snapshot().to_pandas()

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
            result_table = left_table.natural_join(right_table, keys=["a"], columns_to_add=["RD = d", "e"])
            self.assertEqual(test_table.size, result_table.size)

    def test_exact_join(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        left_table = test_table.drop_columns(["d", "e"])
        right_table = test_table.drop_columns(["b", "c"])
        with self.assertRaises(DHError):
            result_table = left_table.exact_join(right_table, keys=["a"], columns_to_add=["d", "e"])
            self.assertEqual(test_table.size, result_table.size)

    def test_left_join(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        left_table = test_table.drop_columns(["d", "e"])
        right_table = test_table.drop_columns(["b", "c"])
        result_table = left_table.left_join(right_table, keys=["a"], columns_to_add=["d", "e"])
        self.assertEqual(test_table.size, result_table.size)

    def test_cross_join(self):
        pa_table = csv.read_csv(self.csv_file)
        left_table = self.session.import_table(pa_table)
        right_table = left_table.where(["a % 2 > 0 && b % 3 == 1"]).drop_columns(column_names=["b", "c", "d"])
        left_table = left_table.drop_columns(column_names=["e"])
        result_table = left_table.join(right_table, keys=["a"], columns_to_add=["e"])
        self.assertTrue(result_table.size < left_table.size)
        result_table = left_table.join(right_table, columns_to_add=["e"])
        self.assertTrue(result_table.size > left_table.size)

    def test_as_of_join(self):
        tt_left = self.session.time_table(period=100000).update(column_specs=["Col1=i"])
        tt_right = self.session.time_table(period=200000).update(column_specs=["Col1=i"])
        time.sleep(2)
        left_table = self.session.import_table(tt_left.snapshot())
        right_table = self.session.import_table(tt_right.snapshot())
        result_table = left_table.aj(right_table, keys=["Col1", "Timestamp"])
        self.assertGreater(result_table.size, 0)
        self.assertLessEqual(result_table.size, left_table.size)
        result_table = left_table.raj(right_table, keys=["Col1", "Timestamp"])
        self.assertGreater(result_table.size, 0)
        self.assertLessEqual(result_table.size, left_table.size)

    def test_head_tail_by(self):
        ops = [Table.head_by,
               Table.tail_by]
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        for op in ops:
            result_table = op(test_table, num_rows=1, column_names=["a"])
            self.assertLessEqual(result_table.size, test_table.size)

    def test_group(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        grouped_table = test_table.group_by(column_names=["a", "c"])
        self.assertLessEqual(grouped_table.size, test_table.size)
        grouped_table = test_table.group_by()
        self.assertLessEqual(grouped_table.size, 1)

    def test_ungroup(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        grouped_table = test_table.group_by(column_names=["a", "c"])
        ungrouped_table = grouped_table.ungroup(column_names=["b"])
        self.assertLessEqual(ungrouped_table.size, test_table.size)

    def test_dedicated_agg(self):
        ops = [Table.first_by, Table.last_by, Table.sum_by, Table.avg_by, Table.std_by, Table.var_by, Table.median_by,
               Table.min_by, Table.max_by]

        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        num_distinct_a = test_table.select_distinct(column_names=["a", "b"]).size
        for op in ops:
            result_table = op(test_table, column_names=["a", "b"])
            self.assertEqual(result_table.size, num_distinct_a)

        for op in ops:
            result_table = op(test_table, column_names=[])
            self.assertEqual(result_table.size, 1)

    def test_count_by(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        num_distinct_a = test_table.select_distinct(column_names=["a"]).size
        result_table = test_table.count_by(count_column="b", column_names=["a"])
        self.assertEqual(result_table.size, num_distinct_a)

    def test_count(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        df = test_table.count(count_column="a").snapshot().to_pandas()
        self.assertEqual(df.iloc[0]["a"], test_table.size)

    def test_combo_agg(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        num_distinct_a = test_table.select_distinct(column_names=["a"]).size

        combo_agg = (ComboAggregation()
            .sum(column_specs=["SumC=c"])
            .avg(column_specs=["AvgB = b", "AvgD = d"])
            .pct(percentile=0.5, column_specs=["PctC = c"])
            .weighted_avg(weight_column="d", column_specs=["WavGD = d"]))

        result_table = test_table.combo_by(column_names=["a"], combo_aggregation=combo_agg)
        self.assertEqual(result_table.size, num_distinct_a)
