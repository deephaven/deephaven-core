#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import random
import unittest
from types import SimpleNamespace
from typing import List, Any

from deephaven import DHError, read_csv, empty_table, SortDirection, time_table, update_graph, new_table, dtypes
from deephaven.agg import sum_, weighted_avg, avg, pct, group, count_, first, last, max_, median, min_, std, abs_sum, \
    var, formula, partition, unique, count_distinct, distinct
from deephaven.column import datetime_col
from deephaven.execution_context import make_user_exec_ctx, get_exec_ctx
from deephaven.html import to_html
from deephaven.jcompat import j_hashmap
from deephaven.pandas import to_pandas
from deephaven.table import Table, TableDefinition, SearchDisplayMode, table_diff
from tests.testbase import BaseTestCase, table_equals


# for scoping dependent table operation tests
def global_fn() -> str:
    return "global str"


global_int = 1001
a_number = 10001


class EmptyCls:
    ...


foo = EmptyCls()
foo.name = "GOOG"
foo.price = 1000


class TableTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")
        self.aggs_for_rollup = [
            avg(["aggAvg=var"]),
            count_("aggCount"),
            first(["aggFirst=var"]),
            last(["aggLast=var"]),
            max_(["aggMax=var"]),
            min_(["aggMin=var"]),
            std(["aggStd=var"]),
            sum_(["aggSum=var"]),
            abs_sum(["aggAbsSum=var"]),
            var(["aggVar=var"]),
            weighted_avg("var", ["weights"]),
        ]
        self.aggs_not_for_rollup = [group(["aggGroup=var"]),
                                    partition("aggPartition"),
                                    median(["aggMed=var"]),
                                    pct(0.20, ["aggPct=var"]),
                                    ]
        self.aggs = self.aggs_for_rollup + self.aggs_not_for_rollup
        self.test_update_graph = get_exec_ctx().update_graph

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_repr(self):
        regex = r"deephaven\.table\.Table\(io\.deephaven\.engine\.table\.Table\(objectRef=0x.+\{.+\}\)\)"
        for i in range(0, 8):
            t = empty_table(10 ** i).update("a=i")
            result = repr(t)
            self.assertRegex(result, regex)
            self.assertLessEqual(len(result), 120)
            self.assertIn(t.__class__.__name__, result)

    #
    # Table operation category: Select
    #
    def test_eq(self):
        t = self.test_table.select()
        self.assert_table_equals(t, self.test_table)

        t = self.test_table.where(["a > 500"])
        self.assertNotEqual(t, self.test_table)

    def test_definition(self):
        expected = TableDefinition({
            "a": dtypes.int32,
            "b": dtypes.int32,
            "c": dtypes.int32,
            "d": dtypes.int32,
            "e": dtypes.int32
        })
        self.assertEquals(expected, self.test_table.definition)

    def test_meta_table(self):
        t = self.test_table.meta_table
        self.assertEqual(len(self.test_table.definition), t.size)

    def test_coalesce(self):
        t = self.test_table.update_view(["A = a * b"])
        ct = t.coalesce()
        self.assertEqual(self.test_table.size, ct.size)

    def test_flatten(self):
        t = self.test_table.update_view(["A = a * b"])
        self.assertFalse(t.is_flat)
        ct = t.flatten()
        self.assertTrue(ct.is_flat)

    def test_drop_columns(self):
        column_names = self.test_table.column_names
        result_table = self.test_table.drop_columns(cols=column_names[:-1])
        self.assertEqual(1, len(result_table.definition))
        result_table = self.test_table.drop_columns(cols=column_names[-1])
        self.assertEqual(1, len(self.test_table.definition) - len(result_table.definition))

    def test_move_columns(self):
        column_names = self.test_table.column_names
        cols_to_move = column_names[::2]

        with self.subTest("move-columns"):
            result_table = self.test_table.move_columns(1, cols_to_move)
            result_cols = result_table.column_names
            self.assertEqual(cols_to_move, result_cols[1: len(cols_to_move) + 1])

        with self.subTest("move-columns-up"):
            result_table = self.test_table.move_columns_up(cols_to_move)
            result_cols = result_table.column_names
            self.assertEqual(cols_to_move, result_cols[: len(cols_to_move)])

        with self.subTest("move-columns-down"):
            result_table = self.test_table.move_columns_down(cols_to_move)
            result_cols = result_table.column_names
            self.assertEqual(cols_to_move, result_cols[-len(cols_to_move):])

        cols_to_move = column_names[-1]
        with self.subTest("move-column"):
            result_table = self.test_table.move_columns(1, cols_to_move)
            result_cols = result_table.column_names
            self.assertEqual([cols_to_move], result_cols[1: len(cols_to_move) + 1])

        with self.subTest("move-column-up"):
            result_table = self.test_table.move_columns_up(cols_to_move)
            result_cols = result_table.column_names
            self.assertEqual([cols_to_move], result_cols[: len(cols_to_move)])

        with self.subTest("move-column-down"):
            result_table = self.test_table.move_columns_down(cols_to_move)
            result_cols = result_table.column_names
            self.assertEqual([cols_to_move], result_cols[-len(cols_to_move):])

    def test_rename_columns(self):
        cols_to_rename = [
            f"{f.name + '_2'} = {f.name}" for f in self.test_table.columns[::2]
        ]
        new_names = [cn.split("=")[0].strip() for cn in cols_to_rename]
        result_table = self.test_table.rename_columns(cols_to_rename)
        result_cols = result_table.column_names
        self.assertEqual(new_names, result_cols[::2])
        result_table = self.test_table.rename_columns(cols_to_rename[0])
        result_cols = result_table.column_names
        self.assertEqual(new_names[0], result_cols[::2][0])

    def test_update_error(self):
        with self.assertRaises(DHError) as cm:
            t = empty_table(10)
            formulas = ["Col1 = i", "Col2 = Col * 2"]
            t2 = t.update(formulas)
        self.assertTrue(cm.exception.root_cause)
        self.assertIn("RuntimeError", cm.exception.compact_traceback)

    def test_USV(self):
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
                    self.test_table, formulas=["a", "c", "Sum = a + b + c + d"])
                self.assertIsNotNone(result_table)
                self.assertTrue(len(result_table.definition) >= 3)
                self.assertLessEqual(result_table.size, self.test_table.size)

        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, formulas="Sum = a + b + c + d")
                self.assertIsNotNone(result_table)
                self.assertTrue(len(result_table.definition) >= 1)
                self.assertLessEqual(result_table.size, self.test_table.size)

    def test_select_distinct(self):
        unique_table = self.test_table.select_distinct(formulas=["a"])
        self.assertLessEqual(unique_table.size, self.test_table.size)
        unique_table = self.test_table.select_distinct(formulas="a")
        self.assertLessEqual(unique_table.size, self.test_table.size)
        unique_table = self.test_table.select_distinct(formulas=[])
        self.assertLessEqual(unique_table.size, self.test_table.size)

        with self.assertRaises(DHError) as cm:
            unique_table = self.test_table.select_distinct(formulas=123)
        self.assertIn("RuntimeError", cm.exception.root_cause)

    #
    # Table operation category: Filter
    #
    def test_where(self):
        filtered_table = self.test_table.where(filters=["a > 10", "b < 100"])
        self.assertLessEqual(filtered_table.size, self.test_table.size)

        filtered_table = self.test_table.where(filters="a > 10")
        self.assertLessEqual(filtered_table.size, self.test_table.size)

    def test_where_in(self):
        unique_table = self.test_table.head(num_rows=50).select_distinct(
            formulas=["a", "c"]
        )

        with self.subTest("where-in filter"):
            result_table = self.test_table.where_in(unique_table, cols=["c"])
            self.assertLessEqual(unique_table.size, result_table.size)

        with self.subTest("where-not-in filter"):
            result_table2 = self.test_table.where_not_in(unique_table, cols="c")
            self.assertEqual(result_table.size, self.test_table.size - result_table2.size)

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

    def test_slice_pct(self):
        result_table = self.test_table.slice_pct(start_pct=0.1, end_pct=0.7)
        self.assertEqual(result_table.size, self.test_table.size * (0.7 - 0.1))

    #
    # Table operation category: Sort
    #
    def test_sort(self):
        sorted_table = self.test_table.sort(order_by=["a", "b"],
                                            order=[SortDirection.DESCENDING, SortDirection.ASCENDING])
        self.assertEqual(sorted_table.size, self.test_table.size)
        with self.assertRaises(DHError) as cm:
            sorted_table = self.test_table.sort(order_by=["a", "b"], order=[SortDirection.DESCENDING])
        self.assertEqual(sorted_table.size, self.test_table.size)
        sorted_table = self.test_table.sort(order_by="a", order=SortDirection.DESCENDING)
        self.assertEqual(sorted_table.size, self.test_table.size)
        sorted_table = self.test_table.sort(order_by=[], order=[])
        self.assertEqual(sorted_table, self.test_table)

    def test_restrict_sort_to(self):
        cols = ["b", "e"]
        restricted_table = self.test_table.restrict_sort_to(cols)
        result_table = restricted_table.sort(order_by=cols)
        restricted_table = self.test_table.restrict_sort_to("b")
        result_table = restricted_table.sort(order_by="b")
        with self.assertRaises(DHError) as cm:
            restricted_table.sort(order_by=["a"])
        self.assertIn("RuntimeError", cm.exception.compact_traceback)

    def test_sort_descending(self):
        sorted_table = self.test_table.sort(
            order_by=["b"], order=[SortDirection.DESCENDING]
        )
        sorted_table2 = self.test_table.sort_descending(order_by=["b"])
        self.assertEqual(sorted_table, sorted_table2)
        sorted_table = self.test_table.sort(order_by="b", order=SortDirection.DESCENDING)
        sorted_table2 = self.test_table.sort_descending(order_by="b")
        self.assertEqual(sorted_table, sorted_table2)

        with self.assertRaises(TypeError):
            sorted_table3 = self.test_table.sort_descending()

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
        left_table = self.test_table.drop_columns(["d", "e"]).group_by('a')
        right_table = self.test_table.drop_columns(["b", "c"]).group_by('a')
        result_table = left_table.exact_join(right_table, on='a', joins=["d", "e"])
        self.assertEqual(result_table.size, left_table.size)

        left_table = self.test_table.select_distinct().drop_columns("d")
        right_table = self.test_table.select_distinct().drop_columns("d")
        with self.assertRaises(DHError) as cm:
            result_table = left_table.exact_join(right_table, on='a', joins=["d", "e"])
        self.assertTrue(cm.exception.root_cause)

    def test_cross_join(self):
        left_table = self.test_table.drop_columns(cols=["e"])
        right_table = self.test_table.where(["a % 2 > 0 && b % 3 == 1"]).drop_columns(
            cols=["b", "c", "d"]
        )
        with self.subTest("with some join keys"):
            result_table = left_table.join(right_table, on=["a"], joins=["e"])
            self.assertTrue(result_table.size < left_table.size)
        with self.subTest("with some join keys"):
            result_table = left_table.join(right_table, on="a", joins="e")
            self.assertTrue(result_table.size < left_table.size)
        with self.subTest("with no join keys"):
            result_table = left_table.join(right_table, on=[], joins=["e"])
            self.assertTrue(result_table.size > left_table.size)
        with self.subTest("with no join keys"):
            result_table = left_table.join(right_table, joins="e")
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
            result_table = left_table.aj(right_table, on="a > a", joins="e")
            self.assertGreater(result_table.size, 0)
            self.assertLessEqual(result_table.size, left_table.size)

        with self.subTest("reverse-as-of join"):
            result_table = left_table.raj(right_table, on=["a"])
            self.assertGreater(result_table.size, 0)
            self.assertLessEqual(result_table.size, left_table.size)
            result_table = left_table.raj(right_table, on="a < a", joins="e")
            self.assertGreater(result_table.size, 0)
            self.assertLessEqual(result_table.size, left_table.size)

        with self.assertRaises(DHError) as cm:
            left_table.aj(right_table, on=["a", "b", "c <= c", "d", "e"])
        self.assertRegex(str(cm.exception), r"Invalid column name")

    #
    # Table operation category: Aggregation
    #
    def test_head_tail_by(self):
        ops = [Table.head_by, Table.tail_by]
        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, num_rows=1, by=["a"])
                self.assertLessEqual(result_table.size, self.test_table.size)
                result_table1 = op(self.test_table, num_rows=1, by="a")
                self.assertLessEqual(result_table1.size, self.test_table.size)
                result_table = op(self.test_table, num_rows=1)
                self.assertLessEqual(result_table.size, self.test_table.size)
                result_table1 = op(self.test_table, num_rows=1)
                self.assertLessEqual(result_table1.size, self.test_table.size)

        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, num_rows=1, by=[])
                self.assertLessEqual(result_table.size, self.test_table.size)

    def test_group_by(self):
        with self.subTest("with some columns"):
            grouped_table = self.test_table.group_by(by=["a", "c"])
            self.assertLessEqual(grouped_table.size, self.test_table.size)
        with self.subTest("with one column"):
            grouped_table = self.test_table.group_by(by="a")
            self.assertLessEqual(grouped_table.size, self.test_table.size)
        with self.subTest("with no columns"):
            grouped_table = self.test_table.group_by()
            self.assertLessEqual(grouped_table.size, 1)

    def test_ungroup(self):
        grouped_table = self.test_table.group_by(by=["a", "c"])
        ungrouped_table = grouped_table.ungroup(cols=["b"])
        self.assertLessEqual(ungrouped_table.size, self.test_table.size)
        ungrouped_table = grouped_table.ungroup(cols="b")
        self.assertLessEqual(ungrouped_table.size, self.test_table.size)

    def test_dedicated_agg(self):
        ops = [
            Table.first_by,
            Table.last_by,
            Table.sum_by,
            Table.abs_sum_by,
            Table.avg_by,
            Table.std_by,
            Table.var_by,
            Table.median_by,
            Table.min_by,
            Table.max_by,
        ]

        num_distinct_a = self.test_table.select_distinct(formulas=["a", "b"]).size
        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, by=["a", "b"])
                self.assertEqual(result_table.size, num_distinct_a)

        num_distinct_a = self.test_table.select_distinct(formulas="a").size
        with self.subTest(op=op):
            result_table = op(self.test_table, by="a")
            self.assertEqual(result_table.size, num_distinct_a)

        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, by=[])
                self.assertEqual(result_table.size, 1)

        wops = [Table.weighted_avg_by,
                Table.weighted_sum_by,
                ]

        for wop in wops:
            with self.subTest(wop):
                result_table = wop(self.test_table, wcol='e', by=["a", "b"])
                self.assertEqual(len(result_table.definition), len(self.test_table.definition) - 1)

                result_table = wop(self.test_table, wcol='e')
                self.assertEqual(len(result_table.definition), len(self.test_table.definition) - 1)

    def test_count_by(self):
        num_distinct_a = self.test_table.select_distinct(formulas=["a"]).size
        result_table = self.test_table.count_by(col="b", by=["a"])
        self.assertEqual(result_table.size, num_distinct_a)

    def test_agg_by(self):
        num_distinct_a = self.test_table.select_distinct(formulas=["a"]).size

        aggs = [
            sum_(cols=["SumC=c"]),
            avg(cols=["AvgB = b", "AvgD = d"]),
            pct(percentile=0.5, cols=["PctC = c"]),
            weighted_avg(wcol="d", cols=["WavGD = d"]),
            formula(
                formula="min(each)", formula_param="each", cols=["MinA=a", "MinD=d"]
            ),
            formula(formula="f_const=5.0 + 3"),
            formula(formula="f_min=min(a)"),
            formula(formula="f_sum=sum(a) + sum(b)"),
            formula(formula="f_sum_3_col=sum(a) + sum(b) + max(c)"),
        ]

        result_table = self.test_table.agg_by(aggs=aggs, by=["a"])
        self.assertEqual(result_table.size, num_distinct_a)

    def test_agg_by_2(self):
        test_table = empty_table(10)
        test_table = test_table.update(
            ["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"]
        )

        result_table = test_table.agg_by(self.aggs, ["grp_id"])
        self.assertEqual(result_table.size, 2)

        for agg in self.aggs:
            result_table = test_table.agg_by(agg, "grp_id")
            self.assertEqual(result_table.size, 2)

    def test_agg_by_initial_groups_preserve_empty(self):
        test_table = empty_table(10)
        test_table = test_table.update(
            ["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"]
        )

        with self.subTest("no-initial-groups, no-by, preserve_empty only"):
            t = test_table.where("grp_id > 2")
            result_table = t.agg_by(self.aggs, preserve_empty=False)
            self.assertEqual(result_table.size, 0)
            result_table = t.agg_by(self.aggs, preserve_empty=True)
            self.assertEqual(result_table.size, 1)
            print(result_table.to_string())

        with self.subTest("with initial-groups, no-by, and preserve_empty"):
            init_groups = test_table.update("grp_id=i")
            # can't specify 'initial-groups' without also specifying 'by'
            with self.assertRaises(DHError):
                result_table = test_table.agg_by(self.aggs, initial_groups=init_groups)

        with self.subTest("with initial-groups, by, and preserve_empty"):
            result_table = test_table.agg_by(self.aggs, by="grp_id", initial_groups=init_groups, preserve_empty=False)
            self.assertEqual(result_table.size, 2)
            result_table = test_table.agg_by(self.aggs, by="grp_id", initial_groups=init_groups, preserve_empty=True)
            self.assertEqual(result_table.size, 10)

    def test_partitioned_agg_by(self):
        test_table = empty_table(10)
        test_table = test_table.update(
            ["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"]
        )

        with self.subTest("no-initial-groups"):
            result_pt = test_table.partitioned_agg_by(aggs=self.aggs, by="grp_id")
            self.assertGreaterEqual(result_pt.table.size, 2)
            self.assertEqual(result_pt.constituent_column, "aggPartition")
            self.assertEqual(result_pt.key_columns, ["grp_id"])
            for ct in result_pt.constituent_tables:
                self.assertEqual(ct.size, 5)

        with self.subTest("initial-groups, preserve_empty=True"):
            init_groups = test_table.update("grp_id=i")
            result_pt1 = test_table.partitioned_agg_by(aggs=self.aggs, by="grp_id", preserve_empty=True,
                                                       initial_groups=init_groups)
            self.assertGreaterEqual(result_pt1.table.size, 10)
            self.assertTrue(any([ct.size == 0 for ct in result_pt1.constituent_tables]))
            self.assertTrue(any([ct.size == 5 for ct in result_pt1.constituent_tables]))

        with self.subTest("initial-groups, preserve_empty=False, used to control constituent table order (reversed)"):
            reversed_init_groups = test_table.update("grp_id=i").reverse()
            result_pt2 = test_table.partitioned_agg_by(aggs=self.aggs, by="grp_id", initial_groups=reversed_init_groups)

            self.assertEqual(result_pt2.table.size, 2)
            self.assertEqual(result_pt.keys().to_string(), result_pt2.keys().reverse().to_string())

    def test_snapshot_when(self):
        t = time_table("PT00:00:01").update_view(["X = i * i", "Y = i + i"])
        with self.subTest("with defaults"):
            snapshot = self.test_table.snapshot_when(t)
            self.wait_ticking_table_update(snapshot, row_count=1, timeout=5)
            self.assertEqual(self.test_table.size, snapshot.size)
            self.assertEqual(len(t.definition) + len(self.test_table.definition), len(snapshot.definition))

        with self.subTest("initial=True"):
            snapshot = self.test_table.snapshot_when(t, initial=True)
            self.assertEqual(self.test_table.size, snapshot.size)
            self.assertEqual(len(t.definition) + len(self.test_table.definition), len(snapshot.definition))

        with self.subTest("stamp_cols=\"X\""):
            snapshot = self.test_table.snapshot_when(t, stamp_cols="X")
            self.assertEqual(len(snapshot.definition), len(self.test_table.definition) + 1)

        with self.subTest("stamp_cols=[\"X\", \"Y\"]"):
            snapshot = self.test_table.snapshot_when(t, stamp_cols=["X", "Y"])
            self.assertEqual(len(snapshot.definition), len(self.test_table.definition) + 2)

    def test_snapshot_when_with_history(self):
        t = time_table("PT00:00:01")
        snapshot_hist = self.test_table.snapshot_when(t, history=True)
        self.wait_ticking_table_update(snapshot_hist, row_count=1, timeout=5)
        self.assertEqual(1 + len(self.test_table.definition), len(snapshot_hist.definition))
        self.assertEqual(self.test_table.size, snapshot_hist.size)

        t = time_table("PT0.1S").update("X = i % 2 == 0 ? i : i - 1").sort("X").tail(10)
        with update_graph.shared_lock(t):
            snapshot_hist = self.test_table.snapshot_when(t, history=True)
            self.assertFalse(snapshot_hist.j_table.isFailed())
        self.wait_ticking_table_update(t, row_count=10, timeout=2)
        # we have not waited for a whole cycle yet, wait for the shared lock to guarantee cycle is over
        # to ensure snapshot_hist has had the opportunity to process the update we just saw
        with update_graph.shared_lock(t):
            self.assertTrue(snapshot_hist.j_table.isFailed())

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
            test_table.agg_all_by(count_("aggCount"), "grp_id")
        self.assertIn("unsupported", cm.exception.root_cause)

        for agg in aggs:
            with self.subTest(agg):
                result_table = test_table.agg_all_by(agg)
                self.assertEqual(result_table.size, 1)

    def test_format_columns(self):
        t = self.test_table.format_columns(["a = YELLOW", "b = BLUE"])
        self.assertIsNotNone(t)

        t = self.test_table.format_columns("a = b % 2 == 0? RED : GREEN")
        self.assertIsNotNone(t)

        t = self.test_table.format_columns("a = heatmap(b, 1, 400, BRIGHT_GREEN, BRIGHT_RED)")
        self.assertIsNotNone(t)

    def test_format_column_where(self):
        t = self.test_table.format_column_where("c", "c % 2 = 0", "ORANGE")
        self.assertIsNotNone(t)

        t = self.test_table.format_column_where("c", "c % 2 = 0", "bg(colorRGB(255, 93, 0))")
        self.assertIsNotNone(t)

    def test_format_row_where(self):
        t = self.test_table.format_row_where("e % 3 = 1", "TEAL")
        self.assertIsNotNone(t)

    def test_layout_hints(self):
        def verify_layout_hint(t: Table, layout_hint_str: str):
            attrs = self.test_table.attributes()
            attrs["LayoutHints"] = layout_hint_str
            self.assertIsNotNone(t)
            self.assertEquals(attrs, t.attributes())

        t = self.test_table.layout_hints(front="d", back="b", freeze="c", hide="d", column_groups=[
            {
                "name": "Group1",
                "children": ["a", "b"]
            },
            {
                "name": "Group2",
                "children": ["c", "d"],
                "color": "#123456"
            },
            {
                "name": "Group3",
                "children": ["e", "f"],
                "color": "RED"
            }
        ])
        verify_layout_hint(t,
                           "front=d;back=b;hide=d;freeze=c;columnGroups=name:Group1::children:a,b|name:Group2::children:c,d::color:#123456|name:Group3::children:e,f::color:#ff0000;")

        t = self.test_table.layout_hints(front=["d", "e"], back=["a", "b"], freeze=["c"], hide=["d"])
        verify_layout_hint(t, "front=d,e;back=a,b;hide=d;freeze=c;")

        t = self.test_table.layout_hints(front="e")
        verify_layout_hint(t, "front=e;")

        t = self.test_table.layout_hints(front=["e"])
        verify_layout_hint(t, "front=e;")

        t = self.test_table.layout_hints(search_display_mode=SearchDisplayMode.SHOW)
        verify_layout_hint(t, "searchable=Show;")

        t = self.test_table.layout_hints(search_display_mode=SearchDisplayMode.HIDE)
        verify_layout_hint(t, "searchable=Hide;")

        t = self.test_table.layout_hints(search_display_mode=SearchDisplayMode.DEFAULT)
        verify_layout_hint(t, "")

        t = self.test_table.layout_hints()
        verify_layout_hint(t, "")

        with self.assertRaises(DHError) as cm:
            t = self.test_table.layout_hints(front=["e"], back=True)
        self.assertTrue(cm.exception.root_cause)
        self.assertIn("RuntimeError", cm.exception.compact_traceback)

    def verify_table_data(self, t: Table, expected: List[Any], assert_not_in: bool = False):
        t_data = to_pandas(t, dtype_backend=None).values.flatten()
        for s in expected:
            if assert_not_in:
                self.assertNotIn(s, t_data)
            else:
                self.assertIn(s, t_data)

    def test_update_LEG_closure(self):
        nonlocal_str = "nonlocal str"
        closure_str = "closure str"

        def inner_func(arg: str):
            def local_fn() -> str:
                return "local str"

            # Note, need to bring a nonlocal_str into the local scope before it can be used in formulas
            nonlocal nonlocal_str
            a_number = 20002

            local_int = 101
            with self.subTest("LEG"):
                t = empty_table(1)
                formulas = ["Col1 = local_fn()",
                            "Col2 = global_fn()",
                            "Col3 = nonlocal_str",
                            "Col4 = arg",
                            "Col5 = local_int",
                            "Col6 = global_int",
                            "Col7 = a_number",
                            ]

                rt = t.update(formulas)
                column_data = ["local str", "global str", "nonlocal str", arg, 101, 1001, 20002]
                self.verify_table_data(rt, column_data)

            with self.subTest("Closure"):
                def closure_fn() -> str:
                    return closure_str

                formulas = ["Col1 = closure_fn()"]
                rt = t.update(formulas)
                self.verify_table_data(rt, ["closure str"])
                nonlocal closure_str
                closure_str = "closure str2"
                rt = t.update(formulas)
                self.verify_table_data(rt, ["closure str2"])

            with self.subTest("Changing scope"):
                x = 1
                rt = empty_table(1).update("X = x")
                self.verify_table_data(rt, [1])
                x = 2
                rt = rt.update(formulas="Y = x")
                self.verify_table_data(rt, [1, 2])

        inner_func("param str")

    def test_nested_scopes(self):
        def inner_func(p) -> str:
            t = empty_table(1).update("X = p * 10")
            return t.to_string().split()[2]

        with make_user_exec_ctx():
            t = empty_table(1).update("X = i").update("TableString = inner_func(X + 10)")

        self.assertIn("100", t.to_string())

    def test_nested_scope_ticking(self):
        def inner_func(p) -> str:
            t = empty_table(1).update("X = p * 10")
            return t.to_string().split()[2]

        with make_user_exec_ctx(), update_graph.shared_lock(self.test_update_graph):
            t = time_table("PT00:00:01").update("X = i").update("TableString = inner_func(X + 10)")

        self.wait_ticking_table_update(t, row_count=5, timeout=10)
        self.assertIn("100", t.to_string())

    def test_scope_comprehensions(self):
        with self.subTest("List comprehension"):
            t = empty_table(1)
            a_list = range(3)
            rt_list = [t.update(formulas=["X=a", "Y=a*10"]) for a in a_list]
            for i, rt in enumerate(rt_list):
                self.verify_table_data(rt, [i, i * 10])

        with self.subTest("Set comprehension"):
            rt_set = {(a, t.update(formulas=["X=a", "Y=a*10"])) for a in a_list}
            for i, rt in rt_set:
                self.verify_table_data(rt, [i, i * 10])

        with self.subTest("Dict comprehension"):
            a_dict = {"k1": 101, "k2": 202}
            rt_dict = {k: t.update(formulas=["X=v", "Y=v*10"]) for k, v in a_dict.items()}
            for k, rt in rt_dict.items():
                v = a_dict[k]
                self.verify_table_data(rt, [v, v * 10])

    def test_scope_lambda(self):
        t = empty_table(1)
        lambda_fn = lambda x: t.update(formulas=["X = x", "Y = x * 10"])
        rt = lambda_fn(10)
        self.verify_table_data(rt, [10, 10 * 10])

    @classmethod
    def update_in_class_method(cls, arg1, arg2) -> Table:
        return empty_table(1).update(formulas=["X = arg1", "Y = arg2"])

    @staticmethod
    def update_in_static_method(arg1, arg2) -> Table:
        return empty_table(1).update(formulas=["X = arg1", "Y = arg2"])

    def test_decorated_methods(self):
        rt = self.update_in_class_method("101", "202")
        self.verify_table_data(rt, ["101", "202"])

        rt = self.update_in_static_method(101, 202)
        self.verify_table_data(rt, [101, 202])

    def test_ticking_table_scope(self):
        from deephaven import update_graph
        x = 1
        with update_graph.shared_lock(self.test_update_graph):
            rt = time_table("PT00:00:01").update("X = x")
        self.wait_ticking_table_update(rt, row_count=1, timeout=5)
        self.verify_table_data(rt, [1])
        for i in range(2, 5):
            x = i
            self.wait_ticking_table_update(rt, row_count=i, timeout=5)
        self.verify_table_data(rt, list(range(2, 5)), assert_not_in=True)

        x = SimpleNamespace()
        x.v = 1
        with update_graph.shared_lock(self.test_update_graph):
            rt = time_table("PT00:00:01").update("X = x.v").drop_columns("Timestamp")
        self.wait_ticking_table_update(rt, row_count=1, timeout=5)

        for i in range(2, 5):
            with update_graph.exclusive_lock(self.test_update_graph):
                x.v = i
                self.wait_ticking_table_update(rt, row_count=rt.size + 1, timeout=5)
        self.verify_table_data(rt, list(range(1, 5)))

    def test_long_number_conversion(self):
        long_value = 2 ** 32 + 5
        t = empty_table(1)
        result = t.update("X = long_value").to_string(1)
        self.assertEqual(long_value, int(result.split()[2]))

    def test_python_field_access(self):
        t = empty_table(10)
        t2 = t.update(formulas=["SYM = `AAPL-` + (String)foo.name", "PRICE = i * 1000"]).where(
            "PRICE > (int)foo.price + 100")
        html_output = to_html(t2)
        self.assertIn("AAPL-GOOG", html_output)
        self.assertIn("2000", html_output)

    def test_slice(self):
        with update_graph.shared_lock(self.test_update_graph):
            t = time_table("PT00:00:00.01")
        rt = t.slice(0, 3)
        self.assert_table_equals(t.head(3), rt)

        self.wait_ticking_table_update(t, row_count=5, timeout=5)
        with update_graph.shared_lock(self.test_update_graph):
            rt = t.slice(t.size, -2)
            self.assertEqual(0, rt.size)
        self.wait_ticking_table_update(rt, row_count=1, timeout=5)
        self.assertGreaterEqual(rt.size, 1)

        rt = t.slice(-3, 0)
        self.assert_table_equals(t.tail(3), rt)

        rt = t.slice(-3, -2)
        self.wait_ticking_table_update(rt, row_count=1, timeout=5)
        self.assert_table_equals(t.tail(3).head(1), rt)

        rt = t.slice(1, 3)
        self.wait_ticking_table_update(rt, row_count=2, timeout=5)
        self.assert_table_equals(t.head(3).tail(2), rt)

        with self.assertRaises(DHError):
            rt = t.slice(3, 2)

    def test_rollup(self):
        test_table = empty_table(100)
        test_table = test_table.update(
            ["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"]
        )
        for agg in self.aggs_not_for_rollup:
            with self.assertRaises(DHError) as cm:
                rollup_table = test_table.rollup(aggs=[agg])
            self.assertRegex(str(cm.exception), r".+ is not supported for rollup")

        rollup_table = test_table.rollup(aggs=self.aggs_for_rollup, by='grp_id')
        self.assertIsNotNone(rollup_table)

        rollup_table = test_table.rollup(aggs=self.aggs_for_rollup, include_constituents=True)
        self.assertIsNotNone(rollup_table)

    def test_tree(self):
        # column 'a' contains duplicate values
        with self.assertRaises(DHError) as cm:
            tree_table = self.test_table.tree(id_col='a', parent_col='c')
        self.assertRegex(str(cm.exception), r".+IllegalStateException")

        tree_table = self.test_table.tail(10).tree(id_col='a', parent_col='c')
        self.assertIsNotNone(tree_table)
        self.assertEqual(tree_table.id_col, 'a')
        self.assertEqual(tree_table.parent_col, 'c')

        tree_table = self.test_table.tail(10).tree(id_col='a', parent_col='a')
        self.assertIsNotNone(tree_table)

        tree_table = self.test_table.tail(10).tree(id_col='a', parent_col='c', promote_orphans=True)
        self.assertIsNotNone(tree_table)

    def test_attributes(self):
        attrs = self.test_table.attributes()
        self.assertTrue(attrs == {})

        attrs["PluginName"] = "@deephaven/js-plugin-table-example"
        attrs["PluginType"] = "@deephaven/auth-plugin"
        attrs["PluginPrivate"] = True
        attrs["PluginAttrs"] = j_hashmap({1: 2, 3: 4})
        attrs["BlinkTable"] = True
        rt = self.test_table.with_attributes(attrs)
        rt_attrs = rt.attributes()
        self.assertEqual(attrs, rt_attrs)
        self.assertTrue(rt.j_table is not self.test_table.j_table)

        rt = rt.without_attributes("BlinkTable")
        rt_attrs = rt.attributes()
        self.assertEqual(len(attrs), len(rt_attrs) + 1)
        self.assertIn("BlinkTable", set(attrs.keys()) - set(rt_attrs.keys()))

    def test_remove_blink(self):
        t_blink = time_table("PT1s", blink_table=True)
        t_no_blink = t_blink.remove_blink()
        self.assertEqual(t_blink.is_blink, True)
        self.assertEqual(t_no_blink.is_blink, False)

    def test_grouped_column_as_arg(self):
        t1 = empty_table(100).update(
            ["id = i % 10", "Person = random() > 0.5 ? true : random() > 0.5 ? false : true"]).sort(
            ["id", "Person"]).group_by("id")
        t2 = empty_table(100).update(
            ["id = i % 10", "Person = random() > 0.5 ? `A` : random() > 0.5 ? `B` : `C`"]).sort(
            ["id", "Person"]).group_by("id")
        t3 = empty_table(100).update(["id = i % 10", "Person = random() > 0.5 ? 1 : random() > 0.5 ? 2 : 3"]).sort(
            ["id", "Person"]).group_by("id")
        t4 = empty_table(10).update(["id= i", "Person = new Object[]{`abc`, `def`}"])
        t5 = empty_table(10).update(["id= i", "Person = new String[]{`abc`, `def`}"])

        def make_pairs_1(a):
            return len(a)

        def make_pairs_2(tid, a):
            return len(a) + tid

        def make_pairs_3(tid, a, b):
            return tid + len(a) + len(a)

        for t in [t1, t2, t3, t4, t5]:
            x1 = t.update("Pair=make_pairs_1(Person)")
            x2 = t.update("Pair=make_pairs_2(id, Person)")
            x3 = t.update("Pair=make_pairs_3(id, Person, Person)")
            self.assertEqual(x1.size, 10)
            self.assertEqual(x2.size, 10)
            self.assertEqual(x3.size, 10)

    def test_callable_attrs_in_query(self):
        input_cols = [
            datetime_col(name="DTCol", data=[1, 10000000]),
        ]
        test_table = new_table(cols=input_cols)
        rt = test_table.update("Year = (int)year(DTCol, timeZone(`ET`))")
        self.assertEqual(rt.size, test_table.size)

        class Foo:
            ATTR = 256

            def __call__(self):
                ...

            def do_something_instance(self, p=None):
                return p if p else 1

            @classmethod
            def do_something_cls(cls, p=None):
                return p if p else 1

            @staticmethod
            def do_something_static(p=None):
                return p if p else 1

        def do_something(p=None):
            return p if p else 1

        rt = empty_table(1).update("Col = Foo.ATTR")
        self.assertTrue(rt.columns[0].data_type == dtypes.PyObject)

        rt = empty_table(1).update("Col = (int)Foo.ATTR")
        self.assertTrue(rt.columns[0].data_type == dtypes.int32)

        foo = Foo()
        rt = empty_table(1).update("Col = (int)foo.do_something_instance()")
        self.assertTrue(rt.columns[0].data_type == dtypes.int32)

        rt = empty_table(1).update("Col = (int)Foo.do_something_cls()")
        self.assertTrue(rt.columns[0].data_type == dtypes.int32)

        rt = empty_table(1).update("Col = (int)foo.do_something_static()")
        self.assertTrue(rt.columns[0].data_type == dtypes.int32)

        rt = empty_table(1).update("Col = (int)do_something((byte)Foo.ATTR)")
        df = to_pandas(rt)
        self.assertEqual(df.loc[0]['Col'], 1)
        self.assertTrue(rt.columns[0].data_type == dtypes.int32)

    def test_await_update(self):
        with self.assertRaises(DHError):
            empty_table(10).await_update()

        time_t = time_table("PT00:00:00.001")
        updated = time_t.await_update()
        self.assertTrue(updated)
        updated = time_t.update("X = i % 2").where("X = 2").await_update(0)
        self.assertFalse(updated)
        updated = time_t.update("X = i % 2").where("X = 2").await_update(1)
        self.assertFalse(updated)
        updated = time_t.update("X = i % 2").where("X = 2").await_update(-1)
        self.assertFalse(updated)

    def test_range_join(self):
        aggs = [
            group(cols=["GroupD=d"]),
        ]
        left_table = self.test_table.select_distinct()
        right_table = self.test_table.select_distinct().sort("b").drop_columns("e")
        result_table = left_table.range_join(right_table, on=["a = a", "c < b < e"], aggs=aggs)
        self.assertEqual(result_table.size, left_table.size)
        self.assertEqual(len(result_table.definition), len(left_table.definition) + len(aggs))

        with self.assertRaises(DHError):
            time_table("PT00:00:00.001").update("a = i").range_join(right_table, on=["a = a", "a < b < c"], aggs=aggs)

    def test_agg_with_options(self):
        test_table = self.test_table.update(["b = a % 10 > 5 ? null : b", "c = c % 10"])
        aggs = [
            median(cols=["ma = a", "mb = b"], average_evenly_divided=False),
            pct(0.20, cols=["pa = a", "pb = b"], average_evenly_divided=True),
            unique(cols=["ua = a", "ub = b"], include_nulls=True, non_unique_sentinel=-1),
            count_distinct(cols=["csa = a", "csb = b"], count_nulls=True),
            distinct(cols=["da = a", "db = b"], include_nulls=True),
        ]
        rt = test_table.agg_by(aggs=aggs, by=["c"])
        self.assertEqual(rt.size, test_table.select_distinct(["c"]).size)

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
                self.assertFalse(table_equals(rt_option, rt_default))

        with self.assertRaises(DHError):
            agg = unique(cols=["ua = a", "ub = b"], include_nulls=True, non_unique_sentinel=None)

    def test_has_columns(self):
        t = empty_table(1).update(["A=i", "B=i", "C=i"])
        self.assertTrue(t.has_columns("B"))
        self.assertTrue(t.has_columns(["A", "C"]))
        self.assertFalse(t.has_columns("D"))
        self.assertFalse(t.has_columns(["D", "C"]))

    def test_agg_count_and_partition_error(self):
        t = empty_table(1).update(["A=i", "B=i", "C=i"])
        with self.assertRaises(DHError) as cm:
            t.agg_by(aggs=count_(["A"]), by=["B"])
        self.assertIn("string value", str(cm.exception))

        with self.assertRaises(DHError) as cm:
            t.agg_by(aggs=[partition(["A"])], by=["B"])
        self.assertIn("string value", str(cm.exception))

    def test_agg_formula_scope(self):
        with self.subTest("agg_by_formula"):
            def agg_by_formula():
                def my_fn(vals):
                    import deephaven.dtypes as dht
                    return dht.array(dht.double, [i + 2 for i in vals])

                t = empty_table(1000).update_view(["A=i%2", "B=A+3"])
                t = t.agg_by([formula("(double[])my_fn(each)", formula_param='each', cols=['C=B']), median("B")],
                             by='A')
                return t

            t = agg_by_formula()
            self.assertIsNotNone(t)

        with self.subTest("agg_all_by_formula"):
            def agg_all_by_formula():
                def my_fn(vals):
                    import deephaven.dtypes as dht
                    return dht.array(dht.double, [i + 2 for i in vals])

                t = empty_table(1000).update_view(["A=i%2", "B=A+3"])
                t = t.agg_all_by(formula("(double[])my_fn(each)", formula_param='each', cols=['C=B']), by='A')
                return t

            t = agg_all_by_formula()
            self.assertIsNotNone(t)

        with self.subTest("partitioned_by_formula"):
            def partitioned_by_formula():
                def my_fn(vals):
                    import deephaven.dtypes as dht
                    return dht.array(dht.double, [i + 2 for i in vals])

                t = empty_table(10).update(["grp_id=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)"])
                t = t.partitioned_agg_by(aggs=formula("(double[])my_fn(each)", formula_param='each',
                                                      cols=['C=weights']), by="grp_id")
                return t

            t = partitioned_by_formula()
            self.assertIsNotNone(t)

    def test_arg_validation(self):
        t = empty_table(1).update(["A=i", "B=i", "C=i"])
        with self.assertRaises(DHError) as cm:
            t.sort("A", "B")
        self.assertIn("The sort direction must be", str(cm.exception))

        with self.assertRaises(DHError) as cm:
            t.partition_by("A", "B")
        self.assertIn("drop_keys must be", str(cm.exception))

    def test_table_diff(self):
        with self.subTest("diff"):
            t1 = empty_table(10).update(["A = i", "B = i", "C = i"])
            t2 = empty_table(10).update(["A = i", "B = i % 2 == 0? i: i + 1", "C = i % 2 == 0? i + 1: i"])
            d = table_diff(t1, t2, max_diffs=10).split("\n")
            self.assertEqual(len(d), 3)
            self.assertIn("row 1", d[0])
            self.assertIn("row 0", d[1])

            d = table_diff(t1, t2).split("\n")
            self.assertEqual(len(d), 2)

        with self.subTest("diff - ignore column order"):
            t1 = empty_table(10).update(["A = i", "B = i + 1"])
            t2 = empty_table(10).update(["B = i + 1", "A = i"])
            d = table_diff(t1, t2, max_diffs=10).split("\n")
            self.assertEqual(len(d), 3)

            t1 = empty_table(10).update(["A = i", "B = i"])
            t2 = empty_table(10).update(["B = i", "A = i"])
            d = table_diff(t1, t2, max_diffs=10, ignore_column_order=True)
            self.assertEqual(d, "")

        with self.subTest("diff - floating_comparison = 'absolute'-double"):
            t1 = empty_table(10).update(["A = i", "B = i + 1.0"])
            t2 = empty_table(10).update(["A = i", "B = i + 1.00001"])
            d = table_diff(t1, t2, max_diffs=10, floating_comparison='exact').split("\n")
            self.assertEqual(len(d), 2)

            t1 = empty_table(10).update(["A = i", "B = i + 1.0"])
            t2 = empty_table(10).update(["A = i", "B = i + 1.00001"])
            d = table_diff(t1, t2, max_diffs=10, floating_comparison='absolute')
            self.assertEqual(d, "")

        with self.subTest("diff - floating_comparison = 'absolute'-float"):
            t1 = empty_table(10).update(["A = i", "B = (float)(i + 1.0)"])
            t2 = empty_table(10).update(["A = i", "B = (float)(i + 1.005)"])
            d = table_diff(t1, t2, max_diffs=10, floating_comparison='exact').split("\n")
            self.assertEqual(len(d), 2)

            t1 = empty_table(10).update(["A = i", "B = (float)(i + 1.0)"])
            # 1.005 would cause the difference to be greater than 0.005, something like 0.00500001144
            t2 = empty_table(10).update(["A = i", "B = (float)(i + 1.004999)"])
            d = table_diff(t1, t2, max_diffs=10, floating_comparison='absolute')
            self.assertEqual(d, "")

        with self.subTest("diff - floating_comparison='relative'-double"):
            t1 = empty_table(10).update(["A = i", "B = i + 1.0"])
            t2 = empty_table(10).update(["A = i", "B = i + 1.00001"])
            d = table_diff(t1, t2, max_diffs=10, floating_comparison='relative')
            self.assertEqual(d, "")

        with self.subTest("diff - floating_comparison='relative'-float"):
            t1 = empty_table(10).update(["A = i", "B = (float)(i + 1.0)"])
            t2 = empty_table(10).update(["A = i", "B = (float)(i + 1.005)"])
            d = table_diff(t1, t2, max_diffs=10, floating_comparison='relative')
            self.assertFalse(d)


if __name__ == "__main__":
    unittest.main()
