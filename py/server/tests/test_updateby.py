#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, time_table, ugp
from deephaven.updateby import BadDataBehavior, MathContext, OperationControl, DeltaControl, ema_tick, ema_time, \
    ems_tick, ems_time, emmin_tick, emmin_time, emmax_tick, emmax_time, emstd_tick, emstd_time,\
    cum_sum, cum_prod, cum_min, cum_max, forward_fill, delta, rolling_sum_tick, rolling_sum_time, \
    rolling_group_tick, rolling_group_time, rolling_avg_tick, rolling_avg_time, rolling_min_tick, rolling_min_time, \
    rolling_max_tick, rolling_max_time, rolling_prod_tick, rolling_prod_time, rolling_count_tick, rolling_count_time, \
    rolling_std_tick, rolling_std_time, rolling_wavg_tick, rolling_wavg_time
from tests.testbase import BaseTestCase


class UpdateByTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.static_table = read_csv("tests/data/test_table.csv").update("Timestamp=now()")
        with ugp.exclusive_lock():
            self.ticking_table = time_table("PT00:00:00.001").update(
                ["a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b"])

    def tearDown(self) -> None:
        self.static_table = None
        self.ticking_table = None
        super().tearDown()

    @classmethod
    def setUpClass(cls) -> None:
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

        cls.simple_ops = [
            cum_sum,
            cum_prod,
            cum_min,
            cum_max,
            forward_fill,
            delta
        ]

        # Rolling Operators list shared with test_rolling_ops / test_rolling_ops_proxy
        cls.rolling_ops = [
            # rolling sum
            rolling_sum_tick(cols=["rsum_a = a", "rsum_d = d"], rev_ticks=10),
            rolling_sum_tick(cols=["rsum_a = a", "rsum_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time="PT00:00:10"),
            rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time="PT00:00:30",
                             fwd_time="-PT00:00:20"),
            # rolling group
            rolling_group_tick(cols=["rgroup_a = a", "rgroup_d = d"], rev_ticks=10),
            rolling_group_tick(cols=["rgroup_a = a", "rgroup_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time="PT00:00:10"),
            rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time=10_000_000_000,
                               fwd_time=-10_000_000_00),
            rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time="PT00:00:30",
                               fwd_time="-PT00:00:20"),
            # rolling average
            rolling_avg_tick(cols=["ravg_a = a", "ravg_d = d"], rev_ticks=10),
            rolling_avg_tick(cols=["ravg_a = a", "ravg_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time="PT00:00:10"),
            rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time="PT00:00:30",
                             fwd_time="-PT00:00:20"),
            # rolling minimum
            rolling_min_tick(cols=["rmin_a = a", "rmin_d = d"], rev_ticks=10),
            rolling_min_tick(cols=["rmin_a = a", "rmin_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time="PT00:00:10"),
            rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time="PT00:00:30",
                             fwd_time="-PT00:00:20"),
            # rolling maximum
            rolling_max_tick(cols=["rmax_a = a", "rmax_d = d"], rev_ticks=10),
            rolling_max_tick(cols=["rmax_a = a", "rmax_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time="PT00:00:10"),
            rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time="PT00:00:30",
                             fwd_time="-PT00:00:20"),
            # rolling product
            rolling_prod_tick(cols=["rprod_a = a", "rprod_d = d"], rev_ticks=10),
            rolling_prod_tick(cols=["rprod_a = a", "rprod_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time="PT00:00:10"),
            rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time=10_000_000_000,
                              fwd_time=-10_000_000_00),
            rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time="PT00:00:30",
                              fwd_time="-PT00:00:20"),
            # rolling count
            rolling_count_tick(cols=["rcount_a = a", "rcount_d = d"], rev_ticks=10),
            rolling_count_tick(cols=["rcount_a = a", "rcount_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_count_time(ts_col="Timestamp", cols=["rcount_b = b", "rcount_e = e"], rev_time="PT00:00:10"),
            rolling_count_time(ts_col="Timestamp", cols=["rcount_b = b", "rcount_e = e"], rev_time=10_000_000_000,
                               fwd_time=-10_000_000_00),
            rolling_count_time(ts_col="Timestamp", cols=["rcount_b = b", "rcount_e = e"], rev_time="PT00:00:30",
                               fwd_time="-PT00:00:20"),
            # rolling standard deviation
            rolling_std_tick(cols=["rstd_a = a", "rstd_d = d"], rev_ticks=10),
            rolling_std_tick(cols=["rstd_a = a", "rstd_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_std_time(ts_col="Timestamp", cols=["rstd_b = b", "rstd_e = e"], rev_time="PT00:00:10"),
            rolling_std_time(ts_col="Timestamp", cols=["rstd_b = b", "rstd_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_std_time(ts_col="Timestamp", cols=["rstd_b = b", "rstd_e = e"], rev_time="PT00:00:30",
                             fwd_time="-PT00:00:20"),
            # rolling weighted average (using "b" as the weight column)
            rolling_wavg_tick(weight_col="b", cols=["rwavg_a = a", "rwavg_d = d"], rev_ticks=10),
            rolling_wavg_tick(weight_col="b", cols=["rwavg_a = a", "rwavg_d = d"], rev_ticks=10, fwd_ticks=10),
            rolling_wavg_time(ts_col="Timestamp", weight_col="b", cols=["rwavg_b = b", "rwavg_e = e"], rev_time="PT00:00:10"),
            rolling_wavg_time(ts_col="Timestamp", weight_col="b", cols=["rwavg_b = b", "rwavg_e = e"], rev_time=10_000_000_000, fwd_time=-10_000_000_00),
            rolling_wavg_time(ts_col="Timestamp", weight_col="b", cols=["rwavg_b = b", "rwavg_e = e"], rev_time="PT00:00:30", fwd_time="-PT00:00:20"),
        ]

    @classmethod
    def tearDownClass(cls) -> None:
        del cls.em_op_ctrl
        del cls.em_ops
        del cls.simple_ops
        del cls.rolling_ops

    def test_em(self):
        for op in self.em_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="b")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.columns), 1 + len(t.columns))
                    with ugp.exclusive_lock():
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
                        self.assertEqual(len(rct.columns), 1 + len(ct.columns))
                        with ugp.exclusive_lock():
                            self.assertEqual(ct.size, rct.size)                        

    def test_simple_ops(self):
        pairs = ["UA=a", "UB=b"]

        for op in self.simple_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op(pairs), by="e")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.columns), 2 + len(t.columns))
                    with ugp.exclusive_lock():
                        self.assertEqual(rt.size, t.size)

    def test_simple_ops_proxy(self):
        pairs = ["UA=a", "UB=b"]
        pt_proxies = [self.static_table.partition_by("c").proxy(),
                      self.ticking_table.partition_by("c").proxy(),
                      ]

        for op in self.simple_ops:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(ops=op(pairs), by="e")

                    self.assertTrue(rt_proxy.is_refreshing is pt_proxy.is_refreshing)
                    self.assertEqual(len(rt_proxy.target.constituent_table_columns),
                                     2 + len(pt_proxy.target.constituent_table_columns))

                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        with ugp.exclusive_lock():
                            self.assertEqual(ct.size, rct.size)

    def test_rolling_ops(self):
        for op in self.rolling_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="c")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.columns), 2 + len(t.columns))
                    with ugp.exclusive_lock():
                        self.assertEqual(rt.size, t.size)

    def test_rolling_ops_proxy(self):
        pt_proxies = [self.static_table.partition_by("b").proxy(),
                      self.ticking_table.partition_by("b").proxy(),
                      ]

        for op in self.rolling_ops:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(op, by="c")
                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        self.assertTrue(rct.is_refreshing is ct.is_refreshing)
                        self.assertEqual(len(rct.columns), 2 + len(ct.columns))
                        with ugp.exclusive_lock():
                            self.assertEqual(ct.size, rct.size)                       

if __name__ == '__main__':
    unittest.main()
