#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import jpy
import unittest
from types import SimpleNamespace
from typing import List, Any

from deephaven import DHError, read_csv, empty_table, SortDirection, AsOfMatchRule, time_table, ugp
from deephaven.agg import sum_, weighted_avg, avg, pct, group, count_, first, last, max_, median, min_, std, abs_sum, \
    var, formula, partition
from deephaven.html import to_html
from deephaven.pandas import to_pandas
from deephaven.table import Table
from tests.testbase import BaseTestCase


class TableTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

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

    def test_meta_table(self):
        t = self.test_table.meta_table
        self.assertEqual(len(self.test_table.columns), t.size)

    def test_coalesce(self):
        t = self.test_table.update_view(["A = a * b"])
        ct = t.coalesce()
        self.assertEqual(self.test_table.size, ct.size)

    def test_drop_columns(self):
        column_names = [f.name for f in self.test_table.columns]
        result_table = self.test_table.drop_columns(cols=column_names[:-1])
        self.assertEqual(1, len(result_table.columns))
        result_table = self.test_table.drop_columns(cols=column_names[-1])
        self.assertEqual(1, len(self.test_table.columns) - len(result_table.columns))

    def test_move_columns(self):
        column_names = [f.name for f in self.test_table.columns]
        cols_to_move = column_names[::2]

        with self.subTest("move-columns"):
            result_table = self.test_table.move_columns(1, cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual(cols_to_move, result_cols[1: len(cols_to_move) + 1])

        with self.subTest("move-columns-up"):
            result_table = self.test_table.move_columns_up(cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual(cols_to_move, result_cols[: len(cols_to_move)])

        with self.subTest("move-columns-down"):
            result_table = self.test_table.move_columns_down(cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual(cols_to_move, result_cols[-len(cols_to_move):])

        cols_to_move = column_names[-1]
        with self.subTest("move-column"):
            result_table = self.test_table.move_columns(1, cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual([cols_to_move], result_cols[1: len(cols_to_move) + 1])

        with self.subTest("move-column-up"):
            result_table = self.test_table.move_columns_up(cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual([cols_to_move], result_cols[: len(cols_to_move)])

        with self.subTest("move-column-down"):
            result_table = self.test_table.move_columns_down(cols_to_move)
            result_cols = [f.name for f in result_table.columns]
            self.assertEqual([cols_to_move], result_cols[-len(cols_to_move):])

    def test_rename_columns(self):
        cols_to_rename = [
            f"{f.name + '_2'} = {f.name}" for f in self.test_table.columns[::2]
        ]
        new_names = [cn.split("=")[0].strip() for cn in cols_to_rename]
        result_table = self.test_table.rename_columns(cols_to_rename)
        result_cols = [f.name for f in result_table.columns]
        self.assertEqual(new_names, result_cols[::2])
        result_table = self.test_table.rename_columns(cols_to_rename[0])
        result_cols = [f.name for f in result_table.columns]
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
                self.assertTrue(len(result_table.columns) >= 3)
                self.assertLessEqual(result_table.size, self.test_table.size)

        for op in ops:
            with self.subTest(op=op):
                result_table = op(self.test_table, formulas="Sum = a + b + c + d")
                self.assertIsNotNone(result_table)
                self.assertTrue(len(result_table.columns) >= 1)
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
        self.test_table.restrict_sort_to(cols)
        result_table = self.test_table.sort(order_by=cols)
        self.test_table.restrict_sort_to("b")
        result_table = self.test_table.sort(order_by="b")
        with self.assertRaises(DHError) as cm:
            self.test_table.sort(order_by=["a"])
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
            result_table = left_table.aj(right_table, on="a", joins="e", match_rule=AsOfMatchRule.LESS_THAN)
            self.assertGreater(result_table.size, 0)
            self.assertLessEqual(result_table.size, left_table.size)

        with self.subTest("reverse-as-of join"):
            result_table = left_table.raj(right_table, on=["a"])
            self.assertGreater(result_table.size, 0)
            self.assertLessEqual(result_table.size, left_table.size)
            result_table = left_table.raj(right_table, on="a", joins="e", match_rule=AsOfMatchRule.GREATER_THAN)
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
                self.assertEqual(len(result_table.columns), len(self.test_table.columns) - 1)

                result_table = wop(self.test_table, wcol='e')
                self.assertEqual(len(result_table.columns), len(self.test_table.columns) - 1)

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
            partition("aggPartition"),
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

        for agg in aggs:
            result_table = test_table.agg_by(agg, "grp_id")
            self.assertGreaterEqual(result_table.size, 1)

    def test_snapshot(self):
        with self.subTest("do_init is False"):
            t = empty_table(0).update(
                formulas=["Timestamp=io.deephaven.time.DateTime.now()", "X = i * i", "Y = i + i"]
            )
            snapshot = t.snapshot(source_table=self.test_table)
            self.assertEqual(len(t.columns) + len(self.test_table.columns), len(snapshot.columns))
            self.assertEqual(0, snapshot.size)

        with self.subTest("do_init is True"):
            snapshot = t.snapshot(source_table=self.test_table, do_init=True)
            self.assertEqual(self.test_table.size, snapshot.size)

        with self.subTest("with cols"):
            snapshot = t.snapshot(source_table=self.test_table, cols="X")
            self.assertEqual(len(snapshot.columns), len(self.test_table.columns) + 1)
            snapshot = t.snapshot(source_table=self.test_table, cols=["X", "Y"])
            self.assertEqual(len(snapshot.columns), len(self.test_table.columns) + 2)

    def test_snapshot_history(self):
        t = empty_table(1).update(
            formulas=["Timestamp=io.deephaven.time.DateTime.now()"]
        )
        snapshot_hist = t.snapshot_history(source_table=self.test_table)
        self.assertEqual(1 + len(self.test_table.columns), len(snapshot_hist.columns))
        self.assertEqual(self.test_table.size, snapshot_hist.size)

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
        self.assertIsNotNone(t)

        t = self.test_table.layout_hints(front=["d", "e"], back=["a", "b"], freeze=["c"], hide=["d"])
        self.assertIsNotNone(t)

        t = self.test_table.layout_hints(front="e")
        self.assertIsNotNone(t)

        t = self.test_table.layout_hints(front=["e"])
        self.assertIsNotNone(t)

        t = self.test_table.layout_hints()
        self.assertIsNotNone(t)

        with self.assertRaises(DHError) as cm:
            t = self.test_table.layout_hints(front=["e"], back=True)
        self.assertTrue(cm.exception.root_cause)
        self.assertIn("RuntimeError", cm.exception.compact_traceback)

    def verify_table_data(self, t: Table, expected: List[Any], assert_not_in: bool = False):
        t_data = to_pandas(t).values.flatten()
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
        _JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
        context = (_JExecutionContext.newBuilder()
                   .captureQueryCompiler()
                   .captureQueryLibrary()
                   .captureQueryScope()
                   .build())

        def inner_func(p) -> str:
            openContext = context.open()
            t = empty_table(1).update("X = p * 10")
            openContext.close()
            return t.to_string().split()[2]

        t = empty_table(1).update("X = i").update("TableString = inner_func(X + 10)")
        self.assertIn("100", t.to_string())

    def test_nested_scope_ticking(self):
        import jpy
        _JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
        j_context = (_JExecutionContext.newBuilder()
                     .captureQueryCompiler()
                     .captureQueryLibrary()
                     .captureQueryScope()
                     .build())

        def inner_func(p) -> str:
            open_ctx = j_context.open()
            t = empty_table(1).update("X = p * 10")
            open_ctx.close()
            return t.to_string().split()[2]

        with ugp.shared_lock():
            t = time_table("00:00:01").update("X = i").update("TableString = inner_func(X + 10)")
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
        from deephaven import ugp
        x = 1
        with ugp.shared_lock():
            rt = time_table("00:00:01").update("X = x")
        self.wait_ticking_table_update(rt, row_count=1, timeout=5)
        self.verify_table_data(rt, [1])
        for i in range(2, 5):
            x = i
            self.wait_ticking_table_update(rt, row_count=i, timeout=5)
        self.verify_table_data(rt, list(range(2, 5)), assert_not_in=True)

        x = SimpleNamespace()
        x.v = 1
        with ugp.shared_lock():
            rt = time_table("00:00:01").update("X = x.v").drop_columns("Timestamp")
        self.wait_ticking_table_update(rt, row_count=1, timeout=5)

        for i in range(2, 5):
            with ugp.exclusive_lock():
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


def global_fn() -> str:
    return "global str"


global_int = 1001
a_number = 10001


class EmptyCls:
    ...


foo = EmptyCls()
foo.name = "GOOG"
foo.price = 1000

if __name__ == "__main__":
    unittest.main()
