#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven2 import DHError, read_csv, empty_table, SortDirection
from deephaven2.agg import (
    sum_,
    weighted_avg,
    avg,
    pct,
    group,
    count_,
    first,
    last,
    max_,
    median,
    min_,
    std,
    abs_sum,
    var,
    formula,
)
from deephaven2.table import Table
from tests.testbase import BaseTestCase


class TableTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_repr(self):
        self.assertIn(self.test_table.__class__.__name__, repr(self.test_table))

    #
    # Table operation category: Select
    #
    def test_eq(self):
        t = self.test_table.select()
        self.assertEqual(t, self.test_table)

        t = self.test_table.where(["a > 500"])
        self.assertNotEqual(t, self.test_table)

    def test_coalesce(self):
        t = self.test_table.update_view(["A = a * b"])
        ct = t.coalesce()
        self.assertEqual(self.test_table.size, ct.size)

    def test_drop_columns(self):
        column_names = [f.name for f in self.test_table.columns]
        result_table = self.test_table.drop_columns(cols=column_names[:-1])
        self.assertEqual(1, len(result_table.columns))

    def test_move_columns(self):
        column_names = [f.name for f in self.test_table.columns]
        cols_to_move = column_names[::2]

        with self.subTest("move-columns"):
            result_table = self.test_table.move_columns(1, cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual(cols_to_move, result_cols[1 : len(cols_to_move) + 1])

        with self.subTest("move-columns-up"):
            result_table = self.test_table.move_columns_up(cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual(cols_to_move, result_cols[: len(cols_to_move)])

        with self.subTest("move-columns-down"):
            result_table = self.test_table.move_columns_down(cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual(cols_to_move, result_cols[-len(cols_to_move) :])

    def test_rename_columns(self):
        cols_to_rename = [
            f"{f.name + '_2'} = {f.name}" for f in self.test_table.columns[::2]
        ]
        new_names = [cn.split("=")[0].strip() for cn in cols_to_rename]
        result_table = self.test_table.rename_columns(cols_to_rename)
        result_cols = [f.name for f in result_table.columns]
        self.assertEqual(new_names, result_cols[::2])

    def test_update_error(self):
        with self.assertRaises(DHError) as cm:
            t = empty_table(10)
            formulas = ["Col1 = i", "Col2 = Col * 2"]
            t2 = t.update(formulas)
        self.assertTrue(cm.exception.root_cause)
        self.assertIn("RuntimeError", cm.exception.compact_traceback)

    def test_usv(self):
        ops = [
            Table.update,
            Table.lazy_update,
            Table.view,
            Table.update_view,
            Table.select,
        ]
        for op in ops:
            with self.subTest(op=op):
                result_table = op(
                    self.test_table, formulas=["a", "c", "Sum = a + b + c + d"]
                )
                self.assertIsNotNone(result_table)
                self.assertTrue(len(result_table.columns) >= 3)
                self.assertLessEqual(result_table.size, self.test_table.size)

    def test_select_distinct(self):
        unique_table = self.test_table.select_distinct(cols=["a"])
        self.assertLessEqual(unique_table.size, self.test_table.size)
        unique_table = self.test_table.select_distinct(cols=[])
        self.assertLessEqual(unique_table.size, self.test_table.size)

        with self.assertRaises(DHError) as cm:
            unique_table = self.test_table.select_distinct(cols=123)
        self.assertIn("TypeError", cm.exception.root_cause)

    #
    # Table operation category: Filter
    #
    def test_where(self):
        filtered_table = self.test_table.where(filters=["a > 10", "b < 100"])
        self.assertLessEqual(filtered_table.size, self.test_table.size)

        with self.assertRaises(DHError) as cm:
            filtered_table = self.test_table.where(filters="a > 10")
        self.assertIn("RuntimeError", cm.exception.compact_traceback)

    def test_where_in(self):
        unique_table = self.test_table.head(num_rows=50).select_distinct(
            cols=["a", "c"]
        )

        with self.subTest("where-in filter"):
            result_table = self.test_table.where_in(unique_table, cols=["c"])
            self.assertLessEqual(unique_table.size, result_table.size)

        with self.subTest("where-not-in filter"):
            result_table2 = self.test_table.where_not_in(unique_table, cols=["c"])
            self.assertEqual(
                result_table.size, self.test_table.size - result_table2.size
            )

    def test_where_one_of(self):
        result_table = self.test_table.where_one_of(filters=["a > 10", "c < 100"])
        self.assertLess(result_table.size, self.test_table.size)

    def test_head_tail(self):
        ops = [Table.head, Table.tail]
        for op in ops:
            result_table = op(self.test_table, num_rows=50)
            self.assertEqual(result_table.size, 50)

    def test_head_tail_pct(self):
        ops = [Table.head_pct, Table.tail_pct]
        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, pct=0.1)
                self.assertEqual(result_table.size, self.test_table.size * 0.1)

    #
    # Table operation category: Sort
    #
    def test_sort(self):
        sorted_table = self.test_table.sort(
            order_by=["a", "b"], order=[SortDirection.DESCENDING]
        )
        self.assertEqual(sorted_table.size, self.test_table.size)

    def test_restrict_sort_to(self):
        cols = ["b", "e"]
        self.test_table.restrict_sort_to(cols)
        result_table = self.test_table.sort(order_by=cols)
        with self.assertRaises(DHError) as cm:
            self.test_table.sort(order_by=["a"])
        self.assertIn("RuntimeError", cm.exception.compact_traceback)

    def test_sort_descending(self):
        sorted_table = self.test_table.sort(
            order_by=["b"], order=[SortDirection.DESCENDING]
        )
        sorted_table2 = self.test_table.sort_descending(order_by=["b"])
        self.assertEqual(
            sorted_table.to_string(num_rows=500), sorted_table2.to_string(num_rows=500)
        )

    def test_reverse(self):
        reversed_table = self.test_table.reverse()
        self.assertEqual(
            self.test_table.to_string(num_rows=1),
            reversed_table.tail(num_rows=1).to_string(num_rows=1),
        )

    #
    # Table operation category: Join
    #
    def test_natural_join(self):
        left_table = self.test_table.drop_columns(["d", "e"])
        right_table = self.test_table.drop_columns(["b", "c"])
        with self.assertRaises(DHError) as cm:
            result_table = left_table.natural_join(
                right_table, on=["a"], joins=["RD = d", "e"]
            )

        self.assertTrue(cm.exception.root_cause)

    def test_exact_join(self):
        left_table = self.test_table.drop_columns(["d", "e"])
        right_table = self.test_table.drop_columns(["b", "c"])
        with self.assertRaises(DHError) as cm:
            result_table = left_table.exact_join(
                right_table, on=["a"], joins=["d", "e"]
            )
        self.assertTrue(cm.exception.root_cause)

    def test_cross_join(self):
        left_table = self.test_table.drop_columns(cols=["e"])
        right_table = self.test_table.where(["a % 2 > 0 && b % 3 == 1"]).drop_columns(
            cols=["b", "c", "d"]
        )
        with self.subTest("with some join keys"):
            result_table = left_table.join(right_table, on=["a"], joins=["e"])
            self.assertTrue(result_table.size < left_table.size)
        with self.subTest("with no join keys"):
            result_table = left_table.join(right_table, on=[], joins=["e"])
            self.assertTrue(result_table.size > left_table.size)

    def test_as_of_join(self):
        left_table = self.test_table.drop_columns(["d", "e"])
        right_table = self.test_table.where(["a % 2 > 0"]).drop_columns(
            cols=["b", "c", "d"]
        )
        with self.subTest("as-of join"):
            result_table = left_table.aj(right_table, on=["a"])
            self.assertGreater(result_table.size, 0)
            self.assertLessEqual(result_table.size, left_table.size)

        with self.subTest("reverse-as-of join"):
            result_table = left_table.raj(right_table, on=["a"])
            self.assertGreater(result_table.size, 0)
            self.assertLessEqual(result_table.size, left_table.size)

    #
    # Table operation category: Aggregation
    #
    def test_head_tail_by(self):
        ops = [Table.head_by, Table.tail_by]
        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, num_rows=1, by=["a"])
                self.assertLessEqual(result_table.size, self.test_table.size)

    def test_group_by(self):
        with self.subTest("with some columns"):
            grouped_table = self.test_table.group_by(by=["a", "c"])
            self.assertLessEqual(grouped_table.size, self.test_table.size)
        with self.subTest("with no columns"):
            grouped_table = self.test_table.group_by()
            self.assertLessEqual(grouped_table.size, 1)

    def test_ungroup(self):
        grouped_table = self.test_table.group_by(by=["a", "c"])
        ungrouped_table = grouped_table.ungroup(cols=["b"])
        self.assertLessEqual(ungrouped_table.size, self.test_table.size)

    def test_dedicated_agg(self):
        ops = [
            Table.first_by,
            Table.last_by,
            Table.sum_by,
            Table.avg_by,
            Table.std_by,
            Table.var_by,
            Table.median_by,
            Table.min_by,
            Table.max_by,
        ]

        num_distinct_a = self.test_table.select_distinct(cols=["a", "b"]).size
        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, by=["a", "b"])
                self.assertEqual(result_table.size, num_distinct_a)

        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, by=[])
                self.assertEqual(result_table.size, 1)

    def test_count_by(self):
        num_distinct_a = self.test_table.select_distinct(cols=["a"]).size
        result_table = self.test_table.count_by(col="b", by=["a"])
        self.assertEqual(result_table.size, num_distinct_a)

    def test_agg_by(self):
        num_distinct_a = self.test_table.select_distinct(cols=["a"]).size

        aggs = [
            sum_(cols=["SumC=c"]),
            avg(cols=["AvgB = b", "AvgD = d"]),
            pct(percentile=0.5, cols=["PctC = c"]),
            weighted_avg(wcol="d", cols=["WavGD = d"]),
            formula(
                formula="min(each)", formula_param="each", cols=["MinA=a", "MinD=d"]
            ),
        ]

        result_table = self.test_table.agg_by(aggs=aggs, by=["a"])
        self.assertEqual(result_table.size, num_distinct_a)

    def test_agg_by_2(self):
        test_table = empty_table(10)
        test_table = test_table.update(
            ["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"]
        )

        aggs = [
            group(["aggGroup=var"]),
            avg(["aggAvg=var"]),
            count_("aggCount"),
            first(["aggFirst=var"]),
            last(["aggLast=var"]),
            max_(["aggMax=var"]),
            median(["aggMed=var"]),
            min_(["aggMin=var"]),
            pct(0.20, ["aggPct=var"]),
            std(["aggStd=var"]),
            sum_(["aggSum=var"]),
            abs_sum(["aggAbsSum=var"]),
            var(["aggVar=var"]),
            weighted_avg("var", ["weights"]),
        ]

        result_table = test_table.agg_by(aggs, ["grp_id"])

        self.assertGreaterEqual(result_table.size, 1)

    def test_snapshot(self):
        with self.subTest("do_init is False"):
            t = empty_table(0).update(
                formulas=["Timestamp=io.deephaven.time.DateTime.now()"]
            )
            snapshot = t.snapshot(source_table=self.test_table)
            self.assertEqual(1 + len(self.test_table.columns), len(snapshot.columns))
            self.assertEqual(0, snapshot.size)

        with self.subTest("do_init is True"):
            snapshot = t.snapshot(source_table=self.test_table, do_init=True)
            self.assertEqual(self.test_table.size, snapshot.size)

    def test_agg_all_by(self):
        test_table = empty_table(10)
        test_table = test_table.update(
            ["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"]
        )

        aggs = [
            group(),
            avg(),
            first(),
            last(),
            max_(),
            median(),
            min_(),
            pct(0.20),
            std(),
            sum_(),
            abs_sum(),
            var(),
            weighted_avg("var"),
        ]
        for agg in aggs:
            with self.subTest(agg):
                result_table = test_table.agg_all_by(agg, ["grp_id"])
                self.assertGreaterEqual(result_table.size, 1)

        # column names in the Aggregation are ignored
        aggs = [
            group(["aggGroup=var"]),
            avg(["aggAvg=var"]),
            pct(0.20, ["aggPct=var"]),
            std(["aggStd=var"]),
            sum_(["aggSum=var"]),
            abs_sum(["aggAbsSum=var"]),
            var(["aggVar=var"]),
            weighted_avg("var", ["weights"]),
        ]
        for agg in aggs:
            with self.subTest(agg):
                result_table = test_table.agg_all_by(agg, ["grp_id"])
                self.assertGreaterEqual(result_table.size, 1)

        with self.assertRaises(DHError) as cm:
            test_table.agg_all_by(count_("aggCount"), ["grp_id"])
        self.assertIn("unsupported", cm.exception.root_cause)


if __name__ == "__main__":
    unittest.main()
