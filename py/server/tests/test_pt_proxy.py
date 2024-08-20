#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, empty_table, SortDirection, DHError, time_table, update_graph
from deephaven.agg import sum_, avg, pct, weighted_avg, formula, group, first, last, max_, median, min_, std, abs_sum, \
    var
from deephaven.table import PartitionedTableProxy
from tests.testbase import BaseTestCase
from deephaven.execution_context import get_exec_ctx


class PartitionedTableProxyTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv").tail(num_rows=100)
        self.partitioned_table = self.test_table.partition_by(by=["c"])
        self.pt_proxy = self.partitioned_table.proxy()
        self.test_update_graph = get_exec_ctx().update_graph

    def tearDown(self):
        self.partitioned_table = None
        self.test_table = None
        super().tearDown()

    def test_target(self):
        self.assertEqual(self.partitioned_table, self.pt_proxy.target)

    def test_is_refreshing(self):
        with update_graph.shared_lock(self.test_update_graph):
            test_table = time_table("PT00:00:00.001").update(["X=i", "Y=i%13", "Z=X*Y"])

        pt = test_table.partition_by("Y")
        proxy = pt.proxy()
        self.assertTrue(pt.is_refreshing)
        self.assertTrue(proxy.is_refreshing)
        self.assertFalse(self.pt_proxy.is_refreshing)

    def test_head_tail(self):
        ops = [PartitionedTableProxy.head, PartitionedTableProxy.tail]
        for op in ops:
            with self.subTest(op):
                pt_proxy = op(self.pt_proxy, 5)
                constituent_tables = pt_proxy.target.constituent_tables
                for ct in constituent_tables:
                    self.assertGreaterEqual(5, ct.size)

    def test_reverse(self):
        pt_proxy = self.pt_proxy.reverse()
        for ct, rct in zip(self.pt_proxy.target.constituent_tables, pt_proxy.target.constituent_tables):
            self.assertEqual(
                ct.to_string(num_rows=1),
                rct.tail(num_rows=1).to_string(num_rows=1),
            )
            self.assertEqual(
                ct.tail(num_rows=1).to_string(num_rows=1),
                rct.to_string(num_rows=1),
            )

    def test_snapshot(self):
        snapshot_proxy = self.pt_proxy.snapshot()
        for ct, snapshot_ct in zip(self.pt_proxy.target.constituent_tables, snapshot_proxy.target.constituent_tables):
            self.assert_table_equals(ct, snapshot_ct)

    def test_snapshot_when(self):
        with self.subTest("snapshot_when with a Table"):
            trigger_proxy = time_table("PT00:00:01")
            result_proxy = self.pt_proxy.snapshot_when(trigger_proxy)
            self.assertEqual(6, len(result_proxy.target.constituent_table_columns))
            self.wait_ticking_proxy_table_update(result_proxy, 1, 5)
            self.assertTrue(all(ct.size > 0 for ct in result_proxy.target.constituent_tables))
            self.assertEqual(len(result_proxy.target.constituent_tables), len(self.pt_proxy.target.constituent_tables))

        with self.subTest("snapshot_when with another Proxy"):
            trigger_proxy = time_table("PT00:00:00.001").update_view(["c = (int)(ii % 1000)"]).partition_by("c", drop_keys=True).proxy()
            lenient_proxy = self.partitioned_table.proxy(require_matching_keys=False)
            result_proxy = lenient_proxy.snapshot_when(trigger_proxy)
            self.wait_ticking_proxy_table_update(result_proxy, 1, 5)
            self.assertTrue(all(ct.size > 0 for ct in result_proxy.target.constituent_tables))

    def test_sort(self):
        sorted_pt_proxy = self.pt_proxy.sort(order_by=["a", "b"],
                                             order=[SortDirection.DESCENDING, SortDirection.ASCENDING])
        for ct, sorted_ct in zip(self.pt_proxy.target.constituent_tables, sorted_pt_proxy.target.constituent_tables):
            self.assertEqual(ct.size, sorted_ct.size)

    def test_sort_descending(self):
        sorted_pt_proxy = self.pt_proxy.sort_descending(order_by=["a", "b"])
        for ct, sorted_ct in zip(self.pt_proxy.target.constituent_tables, sorted_pt_proxy.target.constituent_tables):
            self.assertEqual(ct.size, sorted_ct.size)

    def test_where(self):
        filtered_pt_proxy = self.pt_proxy.where(filters=["a > 10", "b < 100"])
        for ct, filtered_ct in zip(self.pt_proxy.target.constituent_tables,
                                   filtered_pt_proxy.target.constituent_tables):
            self.assertLessEqual(filtered_ct.size, ct.size)

    def test_where_in(self):
        unique_table = self.test_table.head(num_rows=50).select_distinct(
            formulas=["a", "c"]
        )

        with self.subTest("where-in filter"):
            filtered_pt_proxy = self.pt_proxy.where_in(unique_table, cols=["c"])
            for ct, filtered_ct in zip(self.pt_proxy.target.constituent_tables,
                                       filtered_pt_proxy.target.constituent_tables):
                self.assertLessEqual(filtered_ct.size, ct.size)

        with self.subTest("where-not-in filter"):
            filtered_pt_proxy2 = self.pt_proxy.where_not_in(unique_table, cols=["c"])
            for ct, filtered_ct, filtered_ct2 in zip(self.pt_proxy.target.constituent_tables,
                                                     filtered_pt_proxy.target.constituent_tables,
                                                     filtered_pt_proxy2.target.constituent_tables):
                self.assertEqual(ct.size, filtered_ct.size + filtered_ct2.size)

    def test_USV(self):
        ops = [
            PartitionedTableProxy.update,
            PartitionedTableProxy.view,
            PartitionedTableProxy.update_view,
            PartitionedTableProxy.select,
        ]
        for op in ops:
            with self.subTest(op=op):
                result_pt_proxy = op(
                    self.pt_proxy, formulas=["a", "c", "Sum = a + b + c + d"])
                for rct, ct in zip(result_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
                    self.assertTrue(len(rct.definition) >= 3)
                    self.assertLessEqual(rct.size, ct.size)

    def test_select_distinct(self):
        unique_pt_proxy = self.pt_proxy.select_distinct(formulas=["a"])
        for uct, ct in zip(unique_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
            self.assertLessEqual(uct.size, ct.size)
        unique_pt_proxy = self.pt_proxy.select_distinct()
        for uct, ct in zip(unique_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
            self.assertLessEqual(uct.size, ct.size)

    def test_natural_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy()
            right_table = self.test_table.drop_columns(["b", "c"]).head(5)
            joined_pt_proxy = pt_proxy.natural_join(right_table, on="a", joins=["d", "e"])
            for ct in joined_pt_proxy.target.constituent_tables:
                self.assertEqual(len(ct.definition), 5)

        with self.subTest("Join with another Proxy"):
            with self.assertRaises(DHError) as cm:
                pt_proxy = self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy()
                right_proxy = self.test_table.drop_columns(["b", "d"]).partition_by("c").proxy()
                joined_pt_proxy = pt_proxy.natural_join(right_proxy, on="a", joins="e")
            self.assertIn("join keys found in multiple constituents", str(cm.exception))

            with self.assertRaises(DHError) as cm:
                pt_proxy = self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy(sanity_check_joins=False)
                right_proxy = self.test_table.drop_columns(["b", "d"]).partition_by("e").proxy()
                joined_pt_proxy = pt_proxy.natural_join(right_proxy, on="a", joins="e")
            self.assertIn("non-matching keys", str(cm.exception))

            pt_proxy = self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy(sanity_check_joins=False)
            right_proxy = self.test_table.drop_columns(["b", "d"]).partition_by("c").proxy()
            joined_pt_proxy = pt_proxy.natural_join(right_proxy, on="a", joins="e")
            for ct in joined_pt_proxy.target.constituent_tables:
                self.assertEqual(len(ct.definition), 4)

    def test_exact_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy()
            right_table = self.test_table.drop_columns(["b", "c"]).group_by('a')
            joined_pt_proxy = pt_proxy.exact_join(right_table, on="a", joins=["d", "e"])
            for ct, jct in zip(pt_proxy.target.constituent_tables, joined_pt_proxy.target.constituent_tables):
                self.assertEqual(len(jct.definition), 5)
                self.assertEqual(ct.size, jct.size)
                self.assertLessEqual(jct.size, right_table.size)

        with self.subTest("Join with another Proxy"):
            pt_proxy = self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy(sanity_check_joins=False)
            right_proxy = self.test_table.drop_columns(["b", "d"]).partition_by("c").proxy()
            joined_pt_proxy = pt_proxy.exact_join(right_proxy, on="a", joins="e")
            for ct, jct in zip(pt_proxy.target.constituent_tables, joined_pt_proxy.target.constituent_tables):
                self.assertEqual(len(jct.definition), 4)
                self.assertEqual(ct.size, jct.size)
                self.assertLessEqual(jct.size, right_table.size)

    def test_cross_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy()
            right_table = self.test_table.where(["a % 2 > 0 && b % 3 == 1"]).drop_columns(cols=["b", "c"]).head(5)
            with self.subTest("with some join keys"):
                joined_pt_proxy = pt_proxy.join(right_table, on="a", joins=["d", "e"])
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size < 5])

            with self.subTest("with no join keys"):
                joined_pt_proxy = pt_proxy.join(right_table, joins="e")
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size > 5])

        with self.subTest("Join with another Proxy"):
            pt_proxy = self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy(sanity_check_joins=False)
            right_proxy = self.test_table.drop_columns(cols="b").partition_by("c").proxy()
            joined_pt_proxy = pt_proxy.join(right_proxy, on="a", joins=["d", "e"])
            self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size < 5])

    def test_as_of_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy()
            right_table = self.test_table.where(["a % 2 > 0"]).drop_columns(cols=["b", "c", "d"])

            with self.subTest("as-of join"):
                joined_pt_proxy = pt_proxy.aj(right_table, on=["a"])
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size > 0])

                joined_pt_proxy = pt_proxy.aj(right_table, on=["a > a"], joins="e")
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size > 0])

            with self.subTest("reverse as-of join"):
                joined_pt_proxy = pt_proxy.raj(right_table, on=["a"])
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size > 0])

                joined_pt_proxy = pt_proxy.raj(right_table, on=["a < a"], joins="e")
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size > 0])

        with self.subTest("Join with another Proxy"):
            pt_proxy = self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy(sanity_check_joins=False)
            right_proxy = self.test_table.drop_columns(cols="b").partition_by("c").proxy()

            with self.subTest("as-of join"):
                joined_pt_proxy = pt_proxy.aj(right_proxy, on=["a"], joins="ac=c")
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size > 0])

            with self.subTest("reverse as-of join"):
                joined_pt_proxy = pt_proxy.raj(right_proxy, on=["a"], joins="ac=c")
                self.assertTrue([ct for ct in joined_pt_proxy.target.constituent_tables if ct.size > 0])

    def test_group_by(self):
        with self.subTest("with some columns"):
            grouped_pt_proxy = self.pt_proxy.group_by(by=["a", "b"])
            for gct, ct in zip(grouped_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
                self.assertLessEqual(gct.size, ct.size)
        with self.subTest("with no columns"):
            grouped_pt_proxy = self.pt_proxy.group_by()
            for gct, ct in zip(grouped_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
                self.assertLessEqual(gct.size, ct.size)

    def test_count_by(self):
        agg_pt_proxy = self.pt_proxy.count_by(col="cnt", by=["a"])
        for gct, ct in zip(agg_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
            self.assertLessEqual(gct.size, ct.size)
            self.assertEqual(len(gct.definition), 2)

    def test_dedicated_agg(self):
        ops = [
            PartitionedTableProxy.first_by,
            PartitionedTableProxy.last_by,
            PartitionedTableProxy.sum_by,
            PartitionedTableProxy.abs_sum_by,
            PartitionedTableProxy.avg_by,
            PartitionedTableProxy.std_by,
            PartitionedTableProxy.var_by,
            PartitionedTableProxy.median_by,
            PartitionedTableProxy.min_by,
            PartitionedTableProxy.max_by,
        ]

        for op in ops:
            with self.subTest(op=op):
                agg_pt_proxy = op(self.pt_proxy, by=["a", "b"])
                for gct, ct in zip(agg_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
                    self.assertLessEqual(gct.size, ct.size)
                    self.assertEqual(len(gct.definition), len(ct.definition))

        wops = [PartitionedTableProxy.weighted_avg_by,
                PartitionedTableProxy.weighted_sum_by,
                ]

        for wop in wops:
            with self.subTest(wop):
                agg_pt_proxy = wop(self.pt_proxy, wcol="e", by=["a", "b"])
                for gct, ct in zip(agg_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
                    self.assertLessEqual(gct.size, ct.size)
                    self.assertEqual(len(gct.definition), len(ct.definition) - 1)

    def test_agg_by(self):
        aggs = [
            sum_(cols=["SumC=c"]),
            avg(cols=["AvgB = b", "AvgD = d"]),
            pct(percentile=0.5, cols=["PctC = e"]),
            weighted_avg(wcol="d", cols=["WavGD = d"]),
            formula(
                formula="min(each)", formula_param="each", cols=["MinA=a", "MinD=d"]
            ),
        ]

        agg_pt_proxy = self.pt_proxy.agg_by(aggs=aggs, by=["a"])
        for gct, ct in zip(agg_pt_proxy.target.constituent_tables, self.pt_proxy.target.constituent_tables):
            self.assertLessEqual(gct.size, ct.size)
            self.assertEqual(len(gct.definition), 8)

    def test_agg_all_by(self):
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
            weighted_avg("b"),
        ]
        for agg in aggs:
            agg_pt_proxy = self.pt_proxy.agg_all_by(agg=agg, by=["a"])
            for gct in agg_pt_proxy.target.constituent_tables:
                self.assertGreaterEqual(gct.size, 1)

    def test_query_scope(self):

        nonlocal_str = "nonlocal str"
        closure_str = "closure str"

        def inner_func(arg: str):
            def local_fn() -> str:
                return "local str"

            # Note, need to bring a nonlocal_str into the local scope before it can be used in formulas
            nonlocal nonlocal_str
            a_number = 20002

            local_int = 101
            formulas = ["Col1 = local_fn()",
                        "Col2 = global_fn()",
                        "Col3 = nonlocal_str",
                        "Col4 = arg",
                        "Col5 = local_int",
                        "Col6 = global_int",
                        "Col7 = a_number",
                        ]

            return self.pt_proxy.update(formulas)

        self.assertIsNotNone(inner_func("param str"))

    @unittest.skip("https://github.com/deephaven/deephaven-core/issues/4847")
    def test_agg_formula_scope(self):
        with self.subTest("agg_by_formula"):
            def agg_by_formula():
                def my_fn(vals):
                    import deephaven.dtypes as dht
                    return dht.array(dht.double, [i + 2 for i in vals])

                t = empty_table(1000).update_view(["A=i%2", "B=A+3"])
                pt_proxy = t.partition_by("A").proxy()
                rlt_pt_proxy = pt_proxy.agg_by([formula("(double[])my_fn(each)", formula_param='each', cols=['C=B']),
                                      median("B")],
                             by='A')
                return rlt_pt_proxy

            ptp = agg_by_formula()
            self.assertIsNotNone(ptp)


def global_fn() -> str:
    return "global str"


global_int = 1001
a_number = 10001

if __name__ == '__main__':
    unittest.main()
