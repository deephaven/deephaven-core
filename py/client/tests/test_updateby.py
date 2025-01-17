#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from pyarrow import csv

from pydeephaven.updateby import BadDataBehavior, MathContext, OperationControl, DeltaControl, ema_tick, ema_time, \
    ems_tick, ems_time, emmin_tick, emmin_time, emmax_tick, emmax_time, emstd_tick, emstd_time, \
    cum_sum, cum_prod, cum_min, cum_max, forward_fill, delta, rolling_sum_tick, rolling_sum_time, \
    rolling_group_tick, rolling_group_time, rolling_avg_tick, rolling_avg_time, rolling_min_tick, rolling_min_time, \
    rolling_max_tick, rolling_max_time, rolling_prod_tick, rolling_prod_time, rolling_count_tick, rolling_count_time, \
    rolling_std_tick, rolling_std_time, rolling_wavg_tick, rolling_wavg_time, rolling_formula_tick, rolling_formula_time, \
    cum_count_where, rolling_count_where_tick, rolling_count_where_time
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
            ema_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c"),
            ema_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c", op_control=cls.em_op_ctrl),
            # exponential moving sum
            ems_tick(decay_ticks=100, cols="ems_a = a"),
            ems_tick(decay_ticks=100, cols="ems_a = a", op_control=cls.em_op_ctrl),
            ems_time(ts_col="Timestamp", decay_time=10, cols="ems_a = a"),
            ems_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="ems_c = c",
                     op_control=cls.em_op_ctrl),
            ems_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c"),
            ems_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c", op_control=cls.em_op_ctrl),
            # exponential moving minimum
            emmin_tick(decay_ticks=100, cols="emmin_a = a"),
            emmin_tick(decay_ticks=100, cols="emmin_a = a", op_control=cls.em_op_ctrl),
            emmin_time(ts_col="Timestamp", decay_time=10, cols="emmin_a = a"),
            emmin_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="emmin_c = c",
                       op_control=cls.em_op_ctrl),
            emmin_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c"),
            emmin_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c", op_control=cls.em_op_ctrl),
            # exponential moving maximum
            emmax_tick(decay_ticks=100, cols="emmax_a = a"),
            emmax_tick(decay_ticks=100, cols="emmax_a = a", op_control=cls.em_op_ctrl),
            emmax_time(ts_col="Timestamp", decay_time=10, cols="emmax_a = a"),
            emmax_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="emmax_c = c",
                       op_control=cls.em_op_ctrl),
            emmax_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c"),
            emmax_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c", op_control=cls.em_op_ctrl),
            # exponential moving standard deviation
            emstd_tick(decay_ticks=100, cols="emstd_a = a"),
            emstd_tick(decay_ticks=100, cols="emstd_a = a", op_control=cls.em_op_ctrl),
            emstd_time(ts_col="Timestamp", decay_time=10, cols="emstd_a = a"),
            emstd_time(ts_col="Timestamp", decay_time="PT00:00:00.001", cols="emtd_c = c",
                       op_control=cls.em_op_ctrl),
            emstd_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c"),
            emstd_time(ts_col="Timestamp", decay_time="PT1M", cols="ema_c = c", op_control=cls.em_op_ctrl),
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
            rolling_formula_time(formula="avg(x)", formula_param="x", ts_col="Timestamp", cols=["formula_b = b", "formula_e = e"], rev_time=10_000_000_000,
                             fwd_time=-10_000_000_00),
            rolling_formula_time(formula="sum(x)", formula_param="x", ts_col="Timestamp", cols=["formula_b = b", "formula_e = e"], rev_time="PT30S",
                             fwd_time="-PT00:00:20"),

        ]


    def test_simple_ops(self):
        for op in self.simple_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="e")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.schema), 2 + len(t.schema))
                    self.assertGreaterEqual(rt.size, t.size)

    def test_simple_ops_one_output(self):
        for op in self.simple_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="e")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.schema), 2 + len(t.schema))
                    self.assertGreaterEqual(rt.size, t.size)

    def test_em(self):
        for op in self.em_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="b")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.schema), 1 + len(t.schema))
                    if not rt.is_refreshing:
                        self.assertEqual(rt.size, t.size)

    def test_rolling_ops(self):
        for op in self.rolling_ops:
            with self.subTest(op):
                for t in (self.static_table, self.ticking_table):
                    rt = t.update_by(ops=op, by="c")
                    self.assertTrue(rt.is_refreshing is t.is_refreshing)
                    self.assertEqual(len(rt.schema), 2 + len(t.schema))
                    self.assertGreaterEqual(rt.size, t.size)

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
            self.assertEqual(len(rt.schema), 10 + len(t.schema))
            if not rt.is_refreshing:
                self.assertEqual(rt.size, t.size)

    def test_cum_count_where_output(self):
        """
        Test and validation of the cum_count_where feature
        """
        test_table = self.session.empty_table(4).update(["a=ii", "b=ii%2"])
        count_aggs = [
            cum_count_where(col="count1", filters="a >= 1"),
            cum_count_where(col="count2", filters="a >= 1 && b == 0"),
        ]
        result_table = test_table.update_by(ops=count_aggs)
        self.assertEqual(result_table.size, 4)

        # get the table as a local pandas dataframe
        df = result_table.to_arrow().to_pandas()
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
        test_table = self.session.empty_table(4).update(["a=ii", "b=ii%2"])
        count_aggs = [
            rolling_count_where_tick(col="count1", filters="a >= 1", rev_ticks=2),
            rolling_count_where_tick(col="count2", filters="a >= 1 && b == 0", rev_ticks=2),
        ]
        result_table = test_table.update_by(ops=count_aggs)
        self.assertEqual(result_table.size, 4)

        # get the table as a local pandas dataframe
        df = result_table.to_arrow().to_pandas()
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
