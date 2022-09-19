#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, time_table, ugp
from deephaven.updateby import ema_tick_decay, BadDataBehavior, MathContext, OperationControl, ema_time_decay, cum_sum, \
    cum_prod, cum_min, cum_max, forward_fill
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
                                   on_null_time=BadDataBehavior.POISON,
                                   on_nan=BadDataBehavior.RESET,
                                   on_zero_deltatime=BadDataBehavior.SKIP,
                                   on_negative_deltatime=BadDataBehavior.SKIP,
                                   big_value_context=MathContext.UNLIMITED)

        ema_ops = [ema_tick_decay(time_scale_ticks=100, cols="ema_a = a"),
                   ema_tick_decay(time_scale_ticks=100, cols="ema_a = a", op_control=op_ctrl),
                   ema_time_decay(ts_col="Timestamp", time_scale=10, cols="ema_a = a"),
                   ema_time_decay(ts_col="Timestamp", time_scale="00:00:00.001", cols="ema_c = c",
                                  op_control=op_ctrl),
                   ]

        for ema_op in ema_ops:
            with self.subTest(ema_op):
                for t in (self.static_table, self.ticking_table):
                    ema_table = t.update_by(ops=ema_op, by="b")
                    self.assertTrue(ema_table.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(ema_table.columns), 1 + len(t.columns))
                    with ugp.exclusive_lock():
                        self.assertEqual(ema_table.size, t.size)

    def test_simple_ops(self):
        op_builders = [cum_sum, cum_prod, cum_min, cum_max, forward_fill]
        pairs = ["UA=a", "UB=b"]

        for op_builder in op_builders:
            with self.subTest(op_builder):
                for t in (self.static_table, self.ticking_table):
                    updateby_table = t.update_by(ops=op_builder(pairs), by="e")
                    self.assertTrue(updateby_table.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(updateby_table.columns), 2 + len(t.columns))
                    with ugp.exclusive_lock():
                        self.assertEqual(updateby_table.size, t.size)

    def test_simple_ops_proxy(self):
        op_builders = [cum_sum, cum_prod, cum_min, cum_max, forward_fill]
        pairs = ["UA=a", "UB=b"]
        pt_proxies = [self.static_table.partition_by("c").proxy(),
                      self.ticking_table.partition_by("c").proxy(),
                      ]

        for op_builder in op_builders:
            with self.subTest(op_builder):
                for pt_proxy in pt_proxies:
                    if pt_proxy.is_refreshing:
                        ugp.auto_locking = True
                    updateby_proxy = pt_proxy.update_by(ops=op_builder(pairs), by="e")

                    self.assertTrue(updateby_proxy.is_refreshing is pt_proxy.is_refreshing)
                    self.assertEqual(len(updateby_proxy.target.constituent_table_columns),
                                     2 + len(pt_proxy.target.constituent_table_columns))

                    for ct, ub_ct in zip(pt_proxy.target.constituent_tables, updateby_proxy.target.constituent_tables):
                        with ugp.exclusive_lock():
                            self.assertEqual(ct.size, ub_ct.size)

                    if pt_proxy.is_refreshing:
                        ugp.auto_locking = False

    def test_ema_proxy(self):
        op_ctrl = OperationControl(on_null=BadDataBehavior.THROW,
                                   on_null_time=BadDataBehavior.POISON,
                                   on_nan=BadDataBehavior.RESET,
                                   on_zero_deltatime=BadDataBehavior.SKIP,
                                   on_negative_deltatime=BadDataBehavior.SKIP,
                                   big_value_context=MathContext.UNLIMITED)

        ema_ops = [ema_tick_decay(time_scale_ticks=100, cols="ema_a = a"),
                          ema_tick_decay(time_scale_ticks=100, cols="ema_a = a", op_control=op_ctrl),
                          ema_time_decay(ts_col="Timestamp", time_scale=10, cols="ema_a = a"),
                          ema_time_decay(ts_col="Timestamp", time_scale=100, cols="ema_c = c",
                                         op_control=op_ctrl),
                          ]
        pt_proxies = [self.static_table.partition_by("b").proxy(),
                      self.ticking_table.partition_by("b").proxy(),
                      ]

        for ema_op in ema_ops:
            with self.subTest(ema_op):
                for pt_proxy in pt_proxies:
                    if pt_proxy.is_refreshing:
                        ugp.auto_locking = True

                    ema_proxy = pt_proxy.update_by(ema_op, by="e")
                    for ct, ema_ct in zip(pt_proxy.target.constituent_tables, ema_proxy.target.constituent_tables):
                        self.assertTrue(ema_ct.is_refreshing is ct.is_refreshing)
                        self.assertEqual(len(ema_ct.columns), 1 + len(ct.columns))
                        with ugp.exclusive_lock():
                            self.assertEqual(ct.size, ema_ct.size)

                    if pt_proxy.is_refreshing:
                        ugp.auto_locking = True


if __name__ == '__main__':
    unittest.main()
