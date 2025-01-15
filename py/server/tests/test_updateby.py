#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, time_table, update_graph, empty_table
from deephaven.updateby import BadDataBehavior, MathContext, OperationControl, DeltaControl, ema_tick, ema_time, \
    ems_tick, ems_time, emmin_tick, emmin_time, emmax_tick, emmax_time, emstd_tick, emstd_time,\
    cum_sum, cum_prod, cum_min, cum_max, forward_fill, delta, rolling_sum_tick, rolling_sum_time, \
    rolling_group_tick, rolling_group_time, rolling_avg_tick, rolling_avg_time, rolling_min_tick, rolling_min_time, \
    rolling_max_tick, rolling_max_time, rolling_prod_tick, rolling_prod_time, rolling_count_tick, rolling_count_time, \
    rolling_std_tick, rolling_std_time, rolling_wavg_tick, rolling_wavg_time, rolling_formula_tick, rolling_formula_time, \
    cum_count_where, rolling_count_where_tick, rolling_count_where_time
from deephaven.pandas import to_pandas
from tests.testbase import BaseTestCase
from deephaven.execution_context import get_exec_ctx, make_user_exec_ctx

class UpdateByTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.static_table = read_csv("tests/data/test_table.csv").update("Timestamp=now()")
        self.test_update_graph = get_exec_ctx().update_graph
        with update_graph.exclusive_lock(self.test_update_graph):
            self.ticking_table = time_table("PT00:00:00.001").update(
                ["a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b"])

    def tearDown(self) -> None:
        self.static_table = None
        self.ticking_table = None
        super().tearDown()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.em_op_ctrl = OperationControl(on_null=BadDataBehavior.THROW,
                                      on_nan=BadDataBehavior.RESET,
                                      big_value_context=MathContext.UNLIMITED)

        cls.em_ops = [
            # exponential moving average
            ema_tick(decay_ticks=100, cols="ema_a = a"),
            ema_tick(decay_ticks=100, cols="ema_a = a", op_control=cls.em_op_ctrl),
            ema_time(ts_col="Timestamp", decay_time=10, cols="ema_a = a"),
            ema_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="ema_c = c",
                           op_control=cls.em_op_ctrl),
            # exponential moving sum
            ems_tick(decay_ticks=100, cols="ems_a = a"),
            ems_tick(decay_ticks=100, cols="ems_a = a", op_control=cls.em_op_ctrl),
            ems_time(ts_col="Timestamp", decay_time=10, cols="ems_a = a"),
            ems_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="ems_c = c",
                           op_control=cls.em_op_ctrl),
            # exponential moving minimum
            emmin_tick(decay_ticks=100, cols="emmin_a = a"),
            emmin_tick(decay_ticks=100, cols="emmin_a = a", op_control=cls.em_op_ctrl),
            emmin_time(ts_col="Timestamp", decay_time=10, cols="emmin_a = a"),
            emmin_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="emmin_c = c",
                             op_control=cls.em_op_ctrl),
            # exponential moving maximum
            emmax_tick(decay_ticks=100, cols="emmax_a = a"),
            emmax_tick(decay_ticks=100, cols="emmax_a = a", op_control=cls.em_op_ctrl),
            emmax_time(ts_col="Timestamp", decay_time=10, cols="emmax_a = a"),
            emmax_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="emmax_c = c",
                             op_control=cls.em_op_ctrl),
            # exponential moving standard deviation
            emstd_tick(decay_ticks=100, cols="emstd_a = a"),
            emstd_tick(decay_ticks=100, cols="emstd_a = a", op_control=cls.em_op_ctrl),
            emstd_time(ts_col="Timestamp", decay_time=10, cols="emstd_a = a"),
            emstd_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="emtd_c = c",
                       op_control=cls.em_op_ctrl),
        ]

        simple_op_pairs = ["UA=a", "UB=b"]
        cls.simple_ops = [
            cum_sum(cols=simple_op_pairs),
            cum_prod(cols=simple_op_pairs),
            cum_min(cols=simple_op_pairs),
            cum_max(cols=simple_op_pairs),
            forward_fill(cols=simple_op_pairs),
            delta(cols=simple_op_pairs),
            delta(cols=simple_op_pairs, delta_control=DeltaControl.NULL_DOMINATES),
            delta(cols=simple_op_pairs, delta_control=DeltaControl.VALUE_DOMINATES),
            delta(cols=simple_op_pairs, delta_control=DeltaControl.ZERO_DOMINATES),
        ]

        cls.simple_ops_one_output = [
            cum_count_where(col='count_1', filters='a > 5'),
            cum_count_where(col='count_2', filters='a > 0 && a < 5'),
            cum_count_where(col='count_3', filters=['a > 0', 'a < 5']),
        ]

        # Rolling Operators list shared with test_rolling_ops / test_rolling_ops_proxy
        cls.rolling_ops = [
            # rolling sum
            rolling_sum_tick(cols=["rsum_a = a", "rsum_d = d"], rev_ticks=10),
            rolling_sum_tick(cols=["rsum_a = a", "rsum_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time="PT00:00:10"),
            rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time="PT30S",
                             fwd_time="-PT00:00:20"),
            # rolling group
            rolling_group_tick(cols=["rgroup_a = a", "rgroup_d = d"], rev_ticks=10),
            rolling_group_tick(cols=["rgroup_a = a", "rgroup_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time="PT00:00:10"),
            rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time=10_000_000_000,
                               fwd_time=-10_000_000_00),
            rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time="PT30S",
                               fwd_time="-PT00:00:20"),
            # rolling average
            rolling_avg_tick(cols=["ravg_a = a", "ravg_d = d"], rev_ticks=10),
            rolling_avg_tick(cols=["ravg_a = a", "ravg_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time="PT00:00:10"),
            rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time="PT30S",
                             fwd_time="-PT00:00:20"),
            # rolling minimum
            rolling_min_tick(cols=["rmin_a = a", "rmin_d = d"], rev_ticks=10),
            rolling_min_tick(cols=["rmin_a = a", "rmin_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time="PT00:00:10"),
            rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time="PT30S",
                             fwd_time="-PT00:00:20"),
            # rolling maximum
            rolling_max_tick(cols=["rmax_a = a", "rmax_d = d"], rev_ticks=10),
            rolling_max_tick(cols=["rmax_a = a", "rmax_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time="PT00:00:10"),
            rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time="PT30S",
                             fwd_time="-PT00:00:20"),
            # rolling product
            rolling_prod_tick(cols=["rprod_a = a", "rprod_d = d"], rev_ticks=10),
            rolling_prod_tick(cols=["rprod_a = a", "rprod_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time="PT00:00:10"),
            rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time=10_000_000_000,
                              fwd_time=-10_000_000_00),
            rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time="PT30S",
                              fwd_time="-PT00:00:20"),
            # rolling count
            rolling_count_tick(cols=["rcount_a = a", "rcount_d = d"], rev_ticks=10),
            rolling_count_tick(cols=["rcount_a = a", "rcount_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_count_time(ts_col="Timestamp", cols=["rcount_b = b", "rcount_e = e"], rev_time="PT00:00:10"),
            rolling_count_time(ts_col="Timestamp", cols=["rcount_b = b", "rcount_e = e"], rev_time=10_000_000_000,
                               fwd_time=-10_000_000_00),
            rolling_count_time(ts_col="Timestamp", cols=["rcount_b = b", "rcount_e = e"], rev_time="PT30S",
                               fwd_time="-PT00:00:20"),
            # rolling standard deviation
            rolling_std_tick(cols=["rstd_a = a", "rstd_d = d"], rev_ticks=10),
            rolling_std_tick(cols=["rstd_a = a", "rstd_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_std_time(ts_col="Timestamp", cols=["rstd_b = b", "rstd_e = e"], rev_time="PT00:00:10"),
            rolling_std_time(ts_col="Timestamp", cols=["rstd_b = b", "rstd_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_std_time(ts_col="Timestamp", cols=["rstd_b = b", "rstd_e = e"], rev_time="PT30S",
                             fwd_time="-PT00:00:20"),
            # rolling weighted average (using "b" as the weight column)
            rolling_wavg_tick(wcol="b", cols=["rwavg_a = a", "rwavg_d = d"], rev_ticks=10),
            rolling_wavg_tick(wcol="b", cols=["rwavg_a = a", "rwavg_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_wavg_time(ts_col="Timestamp", wcol="b", cols=["rwavg_b = b", "rwavg_e = e"], rev_time="PT00:00:10"),
            rolling_wavg_time(ts_col="Timestamp", wcol="b", cols=["rwavg_b = b", "rwavg_e = e"], rev_time=10_000_000_000, fwd_time=-10_000_000_00),
            rolling_wavg_time(ts_col="Timestamp", wcol="b", cols=["rwavg_b = b", "rwavg_e = e"], rev_time="PT30S", fwd_time="-PT00:00:20"),
            # rolling formula
            rolling_formula_tick(formula="sum(x)", formula_param="x", cols=["formula_a = a", "formula_d = d"], rev_ticks=10),
            rolling_formula_tick(formula="avg(x)", formula_param="x", cols=["formula_a = a", "formula_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_formula_time(formula="sum(x)", formula_param="x", ts_col="Timestamp", cols=["formula_b = b", "formula_e = e"], rev_time="PT00:00:10"),
            rolling_formula_time(formula="avg(x)", formula_param="x", ts_col="Timestamp", cols=["formula_b = b", "formula_e = e"], rev_time=10_000_000_000, fwd_time=-10_000_000_00),
            rolling_formula_time(formula="sum(x)", formula_param="x", ts_col="Timestamp", cols=["formula_b = b", "formula_e = e"], rev_time="PT30S", fwd_time="-PT00:00:20"),
        ]

        # Rolling Operators list shared with test_rolling_ops / test_rolling_ops_proxy that produce a single output column
        cls.rolling_ops_one_output = [
            rolling_formula_tick(formula="formula_ad=sum(a) + sum(d)", rev_ticks=10),
            rolling_formula_tick(formula="formula_ad=avg(a) + avg(b)", rev_ticks=10, fwd_ticks=10),
            rolling_formula_time(formula="formula_be=sum(b) + sum(e)", ts_col="Timestamp", rev_time="PT00:00:10"),
            rolling_formula_time(formula="formula_be=avg(b) + avg(e)", ts_col="Timestamp", rev_time=10_000_000_000, fwd_time=-10_000_000_00),
            rolling_formula_time(formula="formula_be=sum(b) + sum(b)", ts_col="Timestamp", rev_time="PT30S", fwd_time="-PT00:00:20"),
            rolling_count_where_tick(col="count_1", filters="a > 50", rev_ticks=10),
            rolling_count_where_tick(col="count_2", filters=["a > 0", "a <= 50"], rev_ticks=10, fwd_ticks=10),
            rolling_count_where_time(col="count_3", filters="a > 50", ts_col="Timestamp", rev_time="PT00:00:10"),
            rolling_count_where_time(col="count_4", filters="a > 0 && a <= 50", ts_col="Timestamp", rev_time=10_000_000_000, fwd_time=-10_000_000_00),
            rolling_count_where_time(col="count_5", filters="a < 0 || a > 50", ts_col="Timestamp", rev_time="PT30S", fwd_time="-PT00:00:20"),
        ]


    @classmethod
    def tearDownClass(cls) -> None:
        del cls.em_op_ctrl
        del cls.em_ops
        del cls.simple_ops
        del cls.rolling_ops
        super().tearDownClass()

    def test_em(self):
        for op in self.em_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="b")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.definition), 1 + len(t.definition))
                    with update_graph.exclusive_lock(self.test_update_graph):
                        self.assertEqual(rt.size, t.size)

    def test_em_proxy(self):
        pt_proxies = [self.static_table.partition_by("b").proxy(),
                      self.ticking_table.partition_by("b").proxy(),
                      ]

        for op in self.em_ops:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(op, by="e")
                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        self.assertTrue(rct.is_refreshing is ct.is_refreshing)
                        self.assertEqual(len(rct.definition), 1 + len(ct.definition))
                        with update_graph.exclusive_lock(self.test_update_graph):
                            self.assertEqual(ct.size, rct.size)                        

    def test_simple_ops(self):
        for op in self.simple_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="e")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.definition), 2 + len(t.definition))
                    with update_graph.exclusive_lock(self.test_update_graph):
                        self.assertEqual(rt.size, t.size)

    def test_simple_ops_proxy(self):
        pt_proxies = [self.static_table.partition_by("c").proxy(),
                      self.ticking_table.partition_by("c").proxy(),
                      ]

        for op in self.simple_ops:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(ops=op, by="e")

                    self.assertTrue(rt_proxy.is_refreshing is pt_proxy.is_refreshing)
                    self.assertEqual(len(rt_proxy.target.constituent_table_columns),
                                     2 + len(pt_proxy.target.constituent_table_columns))

                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        with update_graph.exclusive_lock(self.test_update_graph):
                            self.assertEqual(ct.size, rct.size)

    def test_simple_ops_one_output(self):
        for op in self.simple_ops_one_output:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="e")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.definition), 1 + len(t.definition))
                    with update_graph.exclusive_lock(self.test_update_graph):
                        self.assertEqual(rt.size, t.size)

    def test_simple_ops_one_output_proxy(self):
        pt_proxies = [self.static_table.partition_by("c").proxy(),
                      self.ticking_table.partition_by("c").proxy(),
                      ]

        for op in self.simple_ops_one_output:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(ops=op, by="e")

                    self.assertTrue(rt_proxy.is_refreshing is pt_proxy.is_refreshing)
                    self.assertEqual(len(rt_proxy.target.constituent_table_columns),
                                     1 + len(pt_proxy.target.constituent_table_columns))

                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        with update_graph.exclusive_lock(self.test_update_graph):
                            self.assertEqual(ct.size, rct.size)

    def test_rolling_ops(self):
        # Test rolling operators that produce 2 output columns
        for op in self.rolling_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="c")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.definition), 2 + len(t.definition))
                    with update_graph.exclusive_lock(self.test_update_graph):
                        self.assertEqual(rt.size, t.size)
        # Test rolling operators that produce a single output column
        for op in self.rolling_ops_one_output:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="c")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.definition), 1 + len(t.definition))
                    with update_graph.exclusive_lock(self.test_update_graph):
                        self.assertEqual(rt.size, t.size)

    def test_rolling_ops_proxy(self):
        pt_proxies = [self.static_table.partition_by("b").proxy(),
                      self.ticking_table.partition_by("b").proxy(),
                      ]

        # Test rolling operators that produce 2 output columns
        for op in self.rolling_ops:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(op, by="c")
                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        self.assertTrue(rct.is_refreshing is ct.is_refreshing)
                        self.assertEqual(len(rct.definition), 2 + len(ct.definition))
                        with update_graph.exclusive_lock(self.test_update_graph):
                            self.assertEqual(ct.size, rct.size)
        # Test rolling operators that produce a single output column
        for op in self.rolling_ops_one_output:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(op, by="c")
                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        self.assertTrue(rct.is_refreshing is ct.is_refreshing)
                        self.assertEqual(len(rct.definition), 1 + len(ct.definition))
                        with update_graph.exclusive_lock(self.test_update_graph):
                            self.assertEqual(ct.size, rct.size)

    def test_multiple_ops(self):
        multiple_ops = [
            cum_sum(["sum_a=a", "sum_b=b"]),
            cum_max(["max_a=a", "max_d=d"]),
            ema_tick(10, ["ema_d=d", "ema_e=e"]),
            ema_time("Timestamp", "PT00:00:00.1", ["ema_time_d=d", "ema_time_e=e"]),
            rolling_wavg_tick(wcol="b", cols=["rwavg_a = a", "rwavg_d = d"], rev_ticks=10),
        ]
        for t in (self.static_table, self.ticking_table):
            rt = t.update_by(ops=multiple_ops, by="c")
            self.assertTrue(rt.is_refreshing is t.is_refreshing)
            self.assertEqual(len(rt.definition), 10 + len(t.definition))
            with update_graph.exclusive_lock(self.test_update_graph):
                self.assertEqual(rt.size, t.size)

    def test_cum_count_where_output(self):
        """
        Test and validation of the cum_count_where feature
        """
        test_table = empty_table(4).update(["a=ii", "b=ii%2"])
        count_aggs = [
            cum_count_where(col="count1", filters="a >= 1"),
            cum_count_where(col="count2", filters="a >= 1 && b == 0"),
        ]
        result_table = test_table.update_by(ops=count_aggs)
        self.assertEqual(result_table.size, 4)

        # get the table as a local pandas dataframe
        df = to_pandas(result_table)
        # assert the values meet expectations
        self.assertEqual(df.loc[0, "count1"], 0)
        self.assertEqual(df.loc[1, "count1"], 1)
        self.assertEqual(df.loc[2, "count1"], 2)
        self.assertEqual(df.loc[3, "count1"], 3)

        self.assertEqual(df.loc[0, "count2"], 0)
        self.assertEqual(df.loc[1, "count2"], 0)
        self.assertEqual(df.loc[2, "count2"], 1)
        self.assertEqual(df.loc[3, "count2"], 1)

    def test_rolling_count_where_output(self):
        """
        Test and validation of the rolling_count_where feature
        """
        test_table = empty_table(4).update(["a=ii", "b=ii%2"])
        count_aggs = [
            rolling_count_where_tick(col="count1", filters="a >= 1", rev_ticks=2),
            rolling_count_where_tick(col="count2", filters="a >= 1 && b == 0", rev_ticks=2),
        ]
        result_table = test_table.update_by(ops=count_aggs)
        self.assertEqual(result_table.size, 4)

        # get the table as a local pandas dataframe
        df = to_pandas(result_table)
        # assert the values meet expectations
        self.assertEqual(df.loc[0, "count1"], 0)
        self.assertEqual(df.loc[1, "count1"], 1)
        self.assertEqual(df.loc[2, "count1"], 2)
        self.assertEqual(df.loc[3, "count1"], 2)

        self.assertEqual(df.loc[0, "count2"], 0)
        self.assertEqual(df.loc[1, "count2"], 0)
        self.assertEqual(df.loc[2, "count2"], 1)
        self.assertEqual(df.loc[3, "count2"], 1)

if __name__ == '__main__':
    unittest.main()
