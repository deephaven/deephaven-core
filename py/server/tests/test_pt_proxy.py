#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import unittest
from collections.abc import Callable

from deephaven import (
    DHError,
    SortDirection,
    empty_table,
    merge,
    read_csv,
    time_table,
    update_graph,
)
from deephaven.agg import (
    abs_sum,
    avg,
    first,
    formula,
    group,
    last,
    max_,
    median,
    min_,
    pct,
    std,
    sum_,
    var,
    weighted_avg,
)
from deephaven.concurrency_control import Barrier
from deephaven.execution_context import get_exec_ctx
from deephaven.filters import Filter
from deephaven.table import NaturalJoinType, PartitionedTableProxy, Selectable, Table
from tests.testbase import BaseTestCase


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

    def test_sanity_check_joins(self):
        # The test data has "a" values under more than one key, which the sanity check rejects.
        with self.subTest("default skips the check"):
            self.assertFalse(self.pt_proxy.sanity_check_joins)
            filter_proxy = self.test_table.partition_by(by=["c"]).proxy()
            self.assertFalse(filter_proxy.sanity_check_joins)
            self.pt_proxy.where_in(filter_proxy, cols=["a"])

        with self.subTest("opt in to the check"):
            checked_proxy = self.partitioned_table.proxy(sanity_check_joins=True)
            self.assertTrue(checked_proxy.sanity_check_joins)
            checked_filter_proxy = self.test_table.partition_by(by=["c"]).proxy(
                sanity_check_joins=True
            )
            with self.assertRaises(DHError):
                checked_proxy.where_in(checked_filter_proxy, cols=["a"])

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
                self.assert_matches_per_constituent(
                    op(self.pt_proxy, 5), lambda ct: getattr(ct, op.__name__)(5)
                )

    def test_reverse(self):
        self.assert_matches_per_constituent(
            self.pt_proxy.reverse(), lambda ct: ct.reverse()
        )

    def test_snapshot(self):
        snapshot_proxy = self.pt_proxy.snapshot()
        for ct, snapshot_ct in zip(
            self.pt_proxy.target.constituent_tables,
            snapshot_proxy.target.constituent_tables,
        ):
            self.assert_table_equals(ct, snapshot_ct)

    def test_snapshot_when(self):
        with self.subTest("snapshot_when with a Table"):
            trigger_proxy = time_table("PT00:00:01")
            result_proxy = self.pt_proxy.snapshot_when(trigger_proxy)
            self.assertEqual(6, len(result_proxy.target.constituent_table_columns))
            self.wait_ticking_proxy_table_update(result_proxy, 1, 5)
            self.assertTrue(
                all(ct.size > 0 for ct in result_proxy.target.constituent_tables)
            )
            self.assertEqual(
                len(result_proxy.target.constituent_tables),
                len(self.pt_proxy.target.constituent_tables),
            )

        with self.subTest("snapshot_when with another Proxy"):
            trigger_proxy = (
                time_table("PT00:00:00.001")
                .update_view(["c = (int)(ii % 1000)"])
                .partition_by("c", drop_keys=True)
                .proxy()
            )
            lenient_proxy = self.partitioned_table.proxy(require_matching_keys=False)
            result_proxy = lenient_proxy.snapshot_when(trigger_proxy)
            self.wait_ticking_proxy_table_update(result_proxy, 1, 5)
            self.assertTrue(
                all(ct.size > 0 for ct in result_proxy.target.constituent_tables)
            )

    def test_sort(self):
        order = [SortDirection.DESCENDING, SortDirection.ASCENDING]
        self.assert_matches_per_constituent(
            self.pt_proxy.sort(order_by=["a", "b"], order=order),
            lambda ct: ct.sort(order_by=["a", "b"], order=order),
        )

    def test_sort_descending(self):
        self.assert_matches_per_constituent(
            self.pt_proxy.sort_descending(order_by=["a", "b"]),
            lambda ct: ct.sort_descending(order_by=["a", "b"]),
        )

    def test_where(self):
        filters = ["a > 10", "b < 100"]
        self.assert_matches_per_constituent(
            self.pt_proxy.where(filters=filters), lambda ct: ct.where(filters=filters)
        )

    def test_where_in(self):
        unique_table = self.test_table.head(num_rows=50).select_distinct(
            formulas=["a", "c"]
        )

        with self.subTest("where-in filter"):
            filtered_pt_proxy = self.pt_proxy.where_in(unique_table, cols=["c"])
            # When filtering on a key column, the fast path removes non-matching partitions
            # entirely rather than keeping them with 0 rows, so compare total sizes.
            self.assertLessEqual(
                sum(ct.size for ct in filtered_pt_proxy.target.constituent_tables),
                sum(ct.size for ct in self.pt_proxy.target.constituent_tables),
            )

        with self.subTest("where-not-in filter"):
            filtered_pt_proxy2 = self.pt_proxy.where_not_in(unique_table, cols=["c"])
            # whereIn and whereNotIn are complementary: every row in the original must
            # appear in exactly one of the two results.
            total_original = sum(
                ct.size for ct in self.pt_proxy.target.constituent_tables
            )
            total_in = sum(
                ct.size for ct in filtered_pt_proxy.target.constituent_tables
            )
            total_not_in = sum(
                ct.size for ct in filtered_pt_proxy2.target.constituent_tables
            )
            self.assertEqual(total_original, total_in + total_not_in)

        with self.subTest("proxy filter table"):
            # The sanity check rejects an "a" value found under more than one key, which the test data has.
            pt_proxy = self.partitioned_table.proxy(sanity_check_joins=False)

            def total_size(proxy: PartitionedTableProxy) -> int:
                return sum(ct.size for ct in proxy.target.constituent_tables)

            def check(
                filter_table: Table, cols: list[str], match_cols: list[str]
            ) -> None:
                filter_proxy = filter_table.partition_by(by=["c"]).proxy(
                    sanity_check_joins=False
                )
                self.assertEqual(
                    self.test_table.where_in(filter_table, cols=match_cols).size,
                    total_size(pt_proxy.where_in(filter_proxy, cols=cols)),
                )
                self.assertEqual(
                    self.test_table.where_not_in(filter_table, cols=match_cols).size,
                    total_size(pt_proxy.where_not_in(filter_proxy, cols=cols)),
                )

            # On the key column, the whole filter table applies. unique_table lacks some keys, so some partitions
            # go.
            self.assertLess(
                unique_table.select_distinct(formulas=["c"]).size,
                self.test_table.select_distinct(formulas=["c"]).size,
            )
            with self.subTest(cols=["c"]):
                check(unique_table, ["c"], ["c"])

            # On another column, each constituent is filtered by the filter constituent with the same key, which is
            # the same as also matching on the key column. The filter proxy must have every key, so add each key with
            # an "a" value no row has.
            with self.subTest(cols=["a"]):
                every_key = self.test_table.select_distinct(formulas=["c"]).view(
                    ["a = -1", "c"]
                )
                check(merge([unique_table, every_key]), ["a"], ["c", "a"])

    def test_USV(self):
        ops = [
            PartitionedTableProxy.update,
            PartitionedTableProxy.view,
            PartitionedTableProxy.update_view,
            PartitionedTableProxy.select,
        ]
        formulas = ["a", "c", "Sum = a + b + c + d"]
        for op in ops:
            with self.subTest(op=op):
                self.assert_matches_per_constituent(
                    op(self.pt_proxy, formulas=formulas),
                    lambda ct: getattr(ct, op.__name__)(formulas=formulas),
                )

    def test_update_empty(self) -> None:
        self.assert_matches_per_constituent(
            self.pt_proxy.update([]), lambda ct: ct.update([])
        )

    def test_select_distinct(self):
        self.assert_matches_per_constituent(
            self.pt_proxy.select_distinct(formulas=["a"]),
            lambda ct: ct.select_distinct(formulas=["a"]),
        )
        self.assert_matches_per_constituent(
            self.pt_proxy.select_distinct(), lambda ct: ct.select_distinct()
        )

    def test_natural_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = (
                self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy()
            )
            right_table = self.test_table.drop_columns(["b", "c"]).head(5)
            self.assert_matches_per_constituent(
                pt_proxy.natural_join(right_table, on="a", joins=["d", "e"]),
                lambda ct: ct.natural_join(right_table, on="a", joins=["d", "e"]),
                source=pt_proxy,
            )

        with self.subTest("Join with another Proxy"):
            with self.assertRaises(DHError) as cm:
                pt_proxy = (
                    self.test_table.drop_columns(["d", "e"])
                    .partition_by("c")
                    .proxy(sanity_check_joins=True)
                )
                right_proxy = (
                    self.test_table.drop_columns(["b", "d"])
                    .partition_by("c")
                    .proxy(sanity_check_joins=True)
                )
                joined_pt_proxy = pt_proxy.natural_join(right_proxy, on="a", joins="e")
            self.assertIn("join keys found in multiple constituents", str(cm.exception))

            with self.assertRaises(DHError) as cm:
                pt_proxy = (
                    self.test_table.drop_columns(["d", "e"])
                    .partition_by("c")
                    .proxy(sanity_check_joins=False)
                )
                right_proxy = (
                    self.test_table.drop_columns(["b", "d"]).partition_by("e").proxy()
                )
                joined_pt_proxy = pt_proxy.natural_join(right_proxy, on="a", joins="e")
            self.assertIn("non-matching keys", str(cm.exception))

            pt_proxy = (
                self.test_table.drop_columns(["d", "e"])
                .partition_by("c")
                .proxy(sanity_check_joins=False)
            )
            right_proxy = (
                self.test_table.drop_columns(["b", "d"]).partition_by("c").proxy()
            )
            joined_pt_proxy = pt_proxy.natural_join(right_proxy, on="a", joins="e")
            for ct in joined_pt_proxy.target.constituent_tables:
                self.assertEqual(len(ct.definition), 4)

    def test_exact_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = (
                self.test_table.drop_columns(["d", "e"]).partition_by("c").proxy()
            )
            right_table = self.test_table.drop_columns(["b", "c"]).group_by("a")
            self.assert_matches_per_constituent(
                pt_proxy.exact_join(right_table, on="a", joins=["d", "e"]),
                lambda ct: ct.exact_join(right_table, on="a", joins=["d", "e"]),
                source=pt_proxy,
            )

        with self.subTest("Join with another Proxy"):
            pt_proxy = (
                self.test_table.drop_columns(["d", "e"])
                .partition_by("c")
                .proxy(sanity_check_joins=False)
            )
            right_proxy = (
                self.test_table.drop_columns(["b", "d"]).partition_by("c").proxy()
            )
            joined_pt_proxy = pt_proxy.exact_join(right_proxy, on="a", joins="e")
            for ct, jct in zip(
                pt_proxy.target.constituent_tables,
                joined_pt_proxy.target.constituent_tables,
            ):
                self.assertEqual(len(jct.definition), 4)
                self.assertEqual(ct.size, jct.size)
                self.assertLessEqual(jct.size, right_table.size)

    def test_cross_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = (
                self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy()
            )
            right_table = (
                self.test_table.where(["a % 2 > 0 && b % 3 == 1"])
                .drop_columns(cols=["b", "c"])
                .head(5)
            )
            with self.subTest("with some join keys"):
                self.assert_matches_per_constituent(
                    pt_proxy.join(right_table, on="a", joins=["d", "e"]),
                    lambda ct: ct.join(right_table, on="a", joins=["d", "e"]),
                    source=pt_proxy,
                )

            with self.subTest("with no join keys"):
                self.assert_matches_per_constituent(
                    pt_proxy.join(right_table, joins="e"),
                    lambda ct: ct.join(right_table, joins="e"),
                    source=pt_proxy,
                )

        with self.subTest("Join with another Proxy"):
            pt_proxy = (
                self.test_table.drop_columns(cols=["d", "e"])
                .partition_by("c")
                .proxy(sanity_check_joins=False)
            )
            right_proxy = (
                self.test_table.drop_columns(cols="b").partition_by("c").proxy()
            )
            joined_pt_proxy = pt_proxy.join(right_proxy, on="a", joins=["d", "e"])
            self.assertTrue(
                [ct for ct in joined_pt_proxy.target.constituent_tables if ct.size < 5]
            )

    def test_cross_join_with_reserve_bits(self):
        with self.subTest("Join with a Table"):
            pt_proxy = (
                self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy()
            )
            right_table = (
                self.test_table.where(["a % 2 > 0 && b % 3 == 1"])
                .drop_columns(cols=["b", "c"])
                .head(5)
            )
            with self.subTest("with some join keys"):
                self.assert_matches_per_constituent(
                    pt_proxy.join(
                        right_table, on="a", joins=["d", "e"], reserve_bits=2
                    ),
                    lambda ct: ct.join(
                        right_table, on="a", joins=["d", "e"], reserve_bits=2
                    ),
                    source=pt_proxy,
                )

            with self.subTest("with no join keys"):
                self.assert_matches_per_constituent(
                    pt_proxy.join(right_table, joins="e", reserve_bits=2),
                    lambda ct: ct.join(right_table, joins="e", reserve_bits=2),
                    source=pt_proxy,
                )

        with self.subTest("Join with another Proxy"):
            pt_proxy = (
                self.test_table.drop_columns(cols=["d", "e"])
                .partition_by("c")
                .proxy(sanity_check_joins=False)
            )
            right_proxy = (
                self.test_table.drop_columns(cols="b").partition_by("c").proxy()
            )
            joined_pt_proxy = pt_proxy.join(
                right_proxy, on="a", joins=["d", "e"], reserve_bits=2
            )
            self.assertTrue(
                [ct for ct in joined_pt_proxy.target.constituent_tables if ct.size < 5]
            )

        with self.subTest("zero reserve_bits"):
            pt_proxy = (
                self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy()
            )
            right_table = self.test_table.drop_columns(cols=["b", "c"])
            with self.assertRaises(DHError) as cm:
                pt_proxy.join(right_table, on="a", joins=["d", "e"], reserve_bits=0)
            self.assertIn("reserveBits must be between 1 and 62", str(cm.exception))

    def test_as_of_join(self):
        with self.subTest("Join with a Table"):
            pt_proxy = (
                self.test_table.drop_columns(cols=["d", "e"]).partition_by("c").proxy()
            )
            right_table = self.test_table.where(["a % 2 > 0"]).drop_columns(
                cols=["b", "c", "d"]
            )

            with self.subTest("as-of join"):
                self.assert_matches_per_constituent(
                    pt_proxy.aj(right_table, on=["a"]),
                    lambda ct: ct.aj(right_table, on=["a"]),
                    source=pt_proxy,
                )
                self.assert_matches_per_constituent(
                    pt_proxy.aj(right_table, on=["a > a"], joins="e"),
                    lambda ct: ct.aj(right_table, on=["a > a"], joins="e"),
                    source=pt_proxy,
                )

            with self.subTest("reverse as-of join"):
                self.assert_matches_per_constituent(
                    pt_proxy.raj(right_table, on=["a"]),
                    lambda ct: ct.raj(right_table, on=["a"]),
                    source=pt_proxy,
                )
                self.assert_matches_per_constituent(
                    pt_proxy.raj(right_table, on=["a < a"], joins="e"),
                    lambda ct: ct.raj(right_table, on=["a < a"], joins="e"),
                    source=pt_proxy,
                )

        with self.subTest("Join with another Proxy"):
            pt_proxy = (
                self.test_table.drop_columns(cols=["d", "e"])
                .partition_by("c")
                .proxy(sanity_check_joins=False)
            )
            right_proxy = (
                self.test_table.drop_columns(cols="b").partition_by("c").proxy()
            )

            with self.subTest("as-of join"):
                joined_pt_proxy = pt_proxy.aj(right_proxy, on=["a"], joins="ac=c")
                self.assertTrue(
                    [
                        ct
                        for ct in joined_pt_proxy.target.constituent_tables
                        if ct.size > 0
                    ]
                )

            with self.subTest("reverse as-of join"):
                joined_pt_proxy = pt_proxy.raj(right_proxy, on=["a"], joins="ac=c")
                self.assertTrue(
                    [
                        ct
                        for ct in joined_pt_proxy.target.constituent_tables
                        if ct.size > 0
                    ]
                )

    def test_group_by(self):
        with self.subTest("with some columns"):
            self.assert_matches_per_constituent(
                self.pt_proxy.group_by(by=["a", "b"]),
                lambda ct: ct.group_by(by=["a", "b"]),
            )
        with self.subTest("with no columns"):
            self.assert_matches_per_constituent(
                self.pt_proxy.group_by(), lambda ct: ct.group_by()
            )

    def test_count_by(self):
        self.assert_matches_per_constituent(
            self.pt_proxy.count_by(col="cnt", by=["a"]),
            lambda ct: ct.count_by(col="cnt", by=["a"]),
        )

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

        # The no-by pass calls a different Java overload, so check it too.
        for op in ops:
            for by in (["a", "b"], None):
                with self.subTest(op=op, by=by):
                    self.assert_matches_per_constituent(
                        op(self.pt_proxy, by=by),
                        lambda ct: getattr(ct, op.__name__)(by=by),
                    )

        wops = [
            PartitionedTableProxy.weighted_avg_by,
            PartitionedTableProxy.weighted_sum_by,
        ]

        for wop in wops:
            for by in (["a", "b"], None):
                with self.subTest(wop=wop, by=by):
                    self.assert_matches_per_constituent(
                        wop(self.pt_proxy, wcol="e", by=by),
                        lambda ct: getattr(ct, wop.__name__)(wcol="e", by=by),
                    )

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

        self.assert_matches_per_constituent(
            self.pt_proxy.agg_by(aggs=aggs, by=["a"]),
            lambda ct: ct.agg_by(aggs=aggs, by=["a"]),
        )

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
            with self.subTest(agg=agg):
                self.assert_matches_per_constituent(
                    self.pt_proxy.agg_all_by(agg=agg, by=["a"]),
                    lambda ct: ct.agg_all_by(agg=agg, by=["a"]),
                )

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
            formulas = [
                "Col1 = local_fn()",
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
                rlt_pt_proxy = pt_proxy.agg_by(
                    [
                        formula(
                            "(double[])my_fn(each)", formula_param="each", cols=["C=B"]
                        ),
                        median("B"),
                    ],
                    by="A",
                )
                return rlt_pt_proxy

            ptp = agg_by_formula()
            self.assertIsNotNone(ptp)

    def test_update_select_concurrency_control(self):
        ops = [
            PartitionedTableProxy.update,
            PartitionedTableProxy.select,
        ]
        barrier1 = Barrier()
        barrier2 = Barrier()
        selectable_a = Selectable.parse("a").with_declared_barriers(
            [barrier1, barrier2]
        )
        selectable_c = (
            Selectable.parse("c").with_respected_barriers(barrier2).with_serial()
        )
        selectable_sum = Selectable.parse(
            "Sum = a + b + c + d"
        ).with_respected_barriers(barrier1)

        for op in ops:
            with self.subTest(op=op):
                result_pt_proxy = op(
                    self.pt_proxy, formulas=[selectable_a, selectable_c, selectable_sum]
                )

                for rct, ct in zip(
                    result_pt_proxy.target.constituent_tables,
                    self.pt_proxy.target.constituent_tables,
                ):
                    self.assertTrue(len(rct.definition) >= 3)
                    self.assertLessEqual(rct.size, ct.size)

    def test_where_concurrency_control(self):
        barrier1 = Barrier()
        filter1 = Filter.from_("a > 10").with_declared_barriers(barrier1)
        filter2 = Filter.from_("b < 100").with_respected_barriers(barrier1)
        filtered_pt_proxy = self.pt_proxy.where(filters=[filter1, filter2])
        for ct, filtered_ct in zip(
            self.pt_proxy.target.constituent_tables,
            filtered_pt_proxy.target.constituent_tables,
        ):
            self.assertLessEqual(filtered_ct.size, ct.size)

    def assert_matches_per_constituent(
        self,
        result: PartitionedTableProxy,
        expected_op: Callable[[Table], Table],
        source: PartitionedTableProxy | None = None,
    ) -> None:
        """Asserts that each constituent of the result equals expected_op applied to the matching source constituent.

        Args:
            result (PartitionedTableProxy): the proxy an op returned
            expected_op (Callable[[Table], Table]): the same op on a plain Table
            source (PartitionedTableProxy | None): the proxy the op ran on. Defaults to None, meaning self.pt_proxy.
        """
        source = self.pt_proxy if source is None else source
        for ct, rct in zip(
            source.target.constituent_tables,
            result.target.constituent_tables,
            strict=True,
        ):
            self.assert_table_equals(expected_op(ct), rct)

    def test_drop_columns(self) -> None:
        self.assert_matches_per_constituent(
            self.pt_proxy.drop_columns(["d", "e"]),
            lambda t: t.drop_columns(["d", "e"]),
        )

    def test_lazy_update(self) -> None:
        self.assert_matches_per_constituent(
            self.pt_proxy.lazy_update("f = a * 2"),
            lambda t: t.lazy_update("f = a * 2"),
        )

    def test_slice(self) -> None:
        self.assert_matches_per_constituent(
            self.pt_proxy.slice(1, -1), lambda t: t.slice(1, -1)
        )

    def test_ungroup(self) -> None:
        grouped = self.pt_proxy.group_by("a")
        ungrouped = grouped.ungroup()
        for gct, uct in zip(
            grouped.target.constituent_tables,
            ungrouped.target.constituent_tables,
            strict=True,
        ):
            self.assert_table_equals(gct.ungroup(), uct)

    def test_range_join(self) -> None:
        aggs = [group(cols=["GroupD=d"])]
        right_table = self.test_table.select_distinct().sort("b").drop_columns("e")
        self.assert_matches_per_constituent(
            self.pt_proxy.range_join(right_table, on=["a = a", "c < b < e"], aggs=aggs),
            lambda t: t.range_join(right_table, on=["a = a", "c < b < e"], aggs=aggs),
        )

    def test_natural_join_type(self) -> None:
        # Merging the table with itself gives every key in "a" a duplicate right row.
        one_copy = self.test_table.drop_columns(["b", "c"])
        right_table = merge([one_copy, one_copy])
        with self.assertRaises(DHError) as cm:
            self.pt_proxy.natural_join(right_table, on="a", joins="d2 = d")
        self.assertIn("duplicate", str(cm.exception).lower())

        self.assert_matches_per_constituent(
            self.pt_proxy.natural_join(
                right_table,
                on="a",
                joins="d2 = d",
                type=NaturalJoinType.FIRST_MATCH,
            ),
            lambda t: t.natural_join(
                right_table, on="a", joins="d2 = d", type=NaturalJoinType.FIRST_MATCH
            ),
        )

    def test_agg_by_preserve_empty_and_initial_groups(self) -> None:
        initial_groups = self.test_table.select_distinct("a")
        aggs = [sum_(cols=["SumB = b"])]
        self.assert_matches_per_constituent(
            self.pt_proxy.agg_by(
                aggs, by="a", preserve_empty=True, initial_groups=initial_groups
            ),
            lambda t: t.agg_by(
                aggs, by="a", preserve_empty=True, initial_groups=initial_groups
            ),
        )

    def test_agg_by_refreshing_initial_groups(self) -> None:
        # This refreshing table has the same rows as the static test table, so both proxies have the same keys.
        with update_graph.shared_lock(self.test_update_graph):
            no_rows = (
                time_table("PT1h")
                .view([f"{col} = (int)0" for col in ["a", "b", "c", "d", "e"]])
                .where("false")
            )
            refreshing_table = merge([self.test_table, no_rows])
        initial_groups = refreshing_table.partition_by("c").proxy()
        self.assertTrue(initial_groups.is_refreshing)
        self.assertFalse(self.pt_proxy.is_refreshing)

        # The receiver is static, so the op must take the lock for the refreshing initial_groups.
        agg_pt_proxy = self.pt_proxy.agg_by(
            [sum_(cols=["SumB = b"])], by="a", initial_groups=initial_groups
        )
        self.assertTrue(agg_pt_proxy.is_refreshing)

    def test_weighted_avg_by_without_by(self) -> None:
        self.assert_matches_per_constituent(
            self.pt_proxy.weighted_avg_by(wcol="e"),
            lambda t: t.weighted_avg_by(wcol="e"),
        )


def global_fn() -> str:
    return "global str"


global_int = 1001
a_number = 10001

if __name__ == "__main__":
    unittest.main()
