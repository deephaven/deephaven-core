#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from pyarrow import csv

from pydeephaven.updateby import cum_sum, cum_prod, cum_min, cum_max, forward_fill, OperationControl, BadDataBehavior, \
    MathContext, ema_tick_decay, ema_time_decay
from tests.testbase import BaseTestCase


class UpdateByTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        pa_table = csv.read_csv(self.csv_file)
        self.static_table = self.session.import_table(pa_table).update(["Timestamp=now()"])
        self.ticking_table = self.session.time_table(1000000).update(
                ["a = i", "b = i*i % 13", "c = i * 13 % 23", "d = a + b", "e = a - b"])

    def tearDown(self) -> None:
        super().tearDown()

    def test_simple_ops(self):
        for op_func in [cum_sum, cum_prod, cum_min, cum_max, forward_fill]:
            with self.subTest(op_func):
                ub_op = op_func(cols=["cc = c", "cb = b"])
                result_table = self.static_table.update_by(ops=[ub_op], by=["e"])
                self.assertEqual(len(result_table.schema), 2 + len(self.static_table.schema))
                self.assertGreaterEqual(self.static_table.size, result_table.size)

    def test_ema(self):
        op_ctrl = OperationControl(on_null=BadDataBehavior.THROW,
                                   on_nan=BadDataBehavior.RESET,
                                   big_value_context=MathContext.UNLIMITED)

        ops = [ema_tick_decay(decay_ticks=100, cols=["ema_a = a"]),
               ema_tick_decay(decay_ticks=100, cols=["ema_a = a"], op_control=op_ctrl),
               ema_time_decay(ts_col="Timestamp", decay_time=10, cols=["ema_a = a"]),
               ema_time_decay(ts_col="Timestamp", decay_time=1000000, cols=["ema_c = c"],
                              op_control=op_ctrl),
               ]

        for op in ops:
            for t in (self.static_table, self.ticking_table):
                with self.subTest(f"{t} {op}"):
                    rt = t.update_by(ops=[op], by=["b"])
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.schema), 1 + len(t.schema))
                    if not rt.is_refreshing:
                        self.assertEqual(rt.size, t.size)


if __name__ == '__main__':
    unittest.main()
