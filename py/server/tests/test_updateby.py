#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, time_table, ugp
from deephaven.updateby import ema_tick_decay, BadDataBehavior, MathContext, OperationControl, ema_time_decay, cum_sum, \
    cum_prod, cum_min, cum_max, forward_fill, rolling_sum_tick, rolling_sum_time, \
    rolling_group_tick, rolling_group_time, rolling_avg_tick, rolling_avg_time, rolling_min_tick, rolling_min_time, \
    rolling_max_tick, rolling_max_time, rolling_prod_tick, rolling_prod_time
from tests.testbase import BaseTestCase


class UpdateByTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.static_table = read_csv("tests/data/test_table.csv").update("Timestamp=currentTime()")
        with ugp.exclusive_lock():
            self.ticking_table = time_table("00:00:00.001").update(
                ["a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b"])

    def tearDown(self) -> None:
        self.static_table = None
        self.ticking_table = None
        super().tearDown()

    def test_ema(self):
        op_ctrl = OperationControl(on_null=BadDataBehavior.THROW,
                                   on_nan=BadDataBehavior.RESET,
                                   big_value_context=MathContext.UNLIMITED)

        ops = [ema_tick_decay(time_scale_ticks=100, cols="ema_a = a"),
               ema_tick_decay(time_scale_ticks=100, cols="ema_a = a", op_control=op_ctrl),
               ema_time_decay(ts_col="Timestamp", time_scale=10, cols="ema_a = a"),
               ema_time_decay(ts_col="Timestamp", time_scale="00:00:00.001", cols="ema_c = c",
                              op_control=op_ctrl),
               ]

        for op in ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="b")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.columns), 1 + len(t.columns))
                    with ugp.exclusive_lock():
                        self.assertEqual(rt.size, t.size)

    def test_ema_proxy(self):
        op_ctrl = OperationControl(on_null=BadDataBehavior.THROW,
                                   on_nan=BadDataBehavior.RESET,
                                   big_value_context=MathContext.UNLIMITED)

        ops = [ema_tick_decay(time_scale_ticks=100, cols="ema_a = a"),
               ema_tick_decay(time_scale_ticks=100, cols="ema_a = a", op_control=op_ctrl),
               ema_time_decay(ts_col="Timestamp", time_scale=10, cols="ema_a = a"),
               ema_time_decay(ts_col="Timestamp", time_scale=100, cols="ema_c = c",
                              op_control=op_ctrl),
               ]
        pt_proxies = [self.static_table.partition_by("b").proxy(),
                      self.ticking_table.partition_by("b").proxy(),
                      ]

        for op in ops:
            with self.subTest(op):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(op, by="e")
                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        self.assertTrue(rct.is_refreshing is ct.is_refreshing)
                        self.assertEqual(len(rct.columns), 1 + len(ct.columns))
                        with ugp.exclusive_lock():
                            self.assertEqual(ct.size, rct.size)                        

    def test_simple_ops(self):
        op_builders = [cum_sum, cum_prod, cum_min, cum_max, forward_fill]
        pairs = ["UA=a", "UB=b"]

        for op_builder in op_builders:
            with self.subTest(op_builder):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op_builder(pairs), by="e")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.columns), 2 + len(t.columns))
                    with ugp.exclusive_lock():
                        self.assertEqual(rt.size, t.size)

    def test_simple_ops_proxy(self):
        op_builders = [cum_sum, cum_prod, cum_min, cum_max, forward_fill]
        pairs = ["UA=a", "UB=b"]
        pt_proxies = [self.static_table.partition_by("c").proxy(),
                      self.ticking_table.partition_by("c").proxy(),
                      ]

        for op_builder in op_builders:
            with self.subTest(op_builder):
                for pt_proxy in pt_proxies:
                    rt_proxy = pt_proxy.update_by(ops=op_builder(pairs), by="e")

                    self.assertTrue(rt_proxy.is_refreshing is pt_proxy.is_refreshing)
                    self.assertEqual(len(rt_proxy.target.constituent_table_columns),
                                     2 + len(pt_proxy.target.constituent_table_columns))

                    for ct, rct in zip(pt_proxy.target.constituent_tables, rt_proxy.target.constituent_tables):
                        with ugp.exclusive_lock():
                            self.assertEqual(ct.size, rct.size)

    # Rolling Operators list shared with test_rolling_ops / test_rolling_ops_proxy
    rolling_ops = [
        # rolling sum
        rolling_sum_tick(cols=["rsum_a = a", "rsum_d = d"], rev_ticks=10),
        rolling_sum_tick(cols=["rsum_a = a", "rsum_d = d"], rev_ticks=10, fwd_ticks=10),
        rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time="00:00:10"),
        rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time=10_000_000_000,
                         fwd_time=-10_000_000_00),
        rolling_sum_time(ts_col="Timestamp", cols=["rsum_b = b", "rsum_e = e"], rev_time="00:00:30",
                         fwd_time="-00:00:20"),

        # rolling group
        rolling_group_tick(cols=["rgroup_a = a", "rgroup_d = d"], rev_ticks=10),
        rolling_group_tick(cols=["rgroup_a = a", "rgroup_d = d"], rev_ticks=10, fwd_ticks=10),
        rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time="00:00:10"),
        rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time=10_000_000_000,
                           fwd_time=-10_000_000_00),
        rolling_group_time(ts_col="Timestamp", cols=["rgroup_b = b", "rgroup_e = e"], rev_time="00:00:30",
                           fwd_time="-00:00:20"),

        # rolling average
        rolling_avg_tick(cols=["ravg_a = a", "ravg_d = d"], rev_ticks=10),
        rolling_avg_tick(cols=["ravg_a = a", "ravg_d = d"], rev_ticks=10, fwd_ticks=10),
        rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time="00:00:10"),
        rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time=10_000_000_000,
                         fwd_time=-10_000_000_00),
        rolling_avg_time(ts_col="Timestamp", cols=["ravg_b = b", "ravg_e = e"], rev_time="00:00:30",
                         fwd_time="-00:00:20"),

        # rolling minimum
        rolling_min_tick(cols=["rmin_a = a", "rmin_d = d"], rev_ticks=10),
        rolling_min_tick(cols=["rmin_a = a", "rmin_d = d"], rev_ticks=10, fwd_ticks=10),
        rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time="00:00:10"),
        rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time=10_000_000_000,
                         fwd_time=-10_000_000_00),
        rolling_min_time(ts_col="Timestamp", cols=["rmin_b = b", "rmin_e = e"], rev_time="00:00:30",
                         fwd_time="-00:00:20"),

        # rolling maximum
        rolling_max_tick(cols=["rmax_a = a", "rmax_d = d"], rev_ticks=10),
        rolling_max_tick(cols=["rmax_a = a", "rmax_d = d"], rev_ticks=10, fwd_ticks=10),
        rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time="00:00:10"),
        rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time=10_000_000_000,
                         fwd_time=-10_000_000_00),
        rolling_max_time(ts_col="Timestamp", cols=["rmax_b = b", "rmax_e = e"], rev_time="00:00:30",
                         fwd_time="-00:00:20"),

        # rolling product
        rolling_prod_tick(cols=["rprod_a = a", "rprod_d = d"], rev_ticks=10),
        rolling_prod_tick(cols=["rprod_a = a", "rprod_d = d"], rev_ticks=10, fwd_ticks=10),
        rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time="00:00:10"),
        rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time=10_000_000_000,
                          fwd_time=-10_000_000_00),
        rolling_prod_time(ts_col="Timestamp", cols=["rprod_b = b", "rprod_e = e"], rev_time="00:00:30",
                          fwd_time="-00:00:20"),
    ]

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
