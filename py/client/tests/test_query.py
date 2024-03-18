#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import time
import unittest

from pyarrow import csv

from pydeephaven import DHError, agg
from pydeephaven.updateby import ema_tick, cum_prod
from tests.testbase import BaseTestCase


class QueryTestCase(BaseTestCase):
    def test_tail_update_static(self):
        table = self.session.empty_table(10)
        query = self.session.query(table).update(formulas=["Col1 = i + 1"]) \
            .tail(5).update(formulas=["Col2=i*i"])
        result_table = query.exec()
        self.assertEqual(5, result_table.size)
        time.sleep(1)
        result_table2 = query.exec()
        self.assertEqual(result_table.size, result_table2.size)

    def test_tail_update_join_fail(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        right_table = self.session.empty_table(1000).update(["a = i"])
        query = self.session.query(test_table)
        (query.drop_columns(cols=['c'])
         .where(["a > 10"])
         .tail(10)
         .join(right_table, on=["a"]))

        with self.assertRaises(DHError):
            query.exec()

    def test_tail_update_join(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        right_table = self.session.empty_table(1000).update(["a = ii"])
        query = self.session.query(test_table)
        (query.drop_columns(cols=['c'])
         .where(["a > 10"]).tail(10)
         .join(right_table, on=["a"]))

        result_table = query.exec()
        self.assertTrue(result_table.size > 0)

    def test_update_by(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        ub_ops = [ema_tick(decay_ticks=100, cols=["ema_a = a"]),
                  cum_prod(cols=["cc = c", "cb = b"]),
                  ]

        query = self.session.query(test_table)
        (query.drop_columns(cols=['e'])
         .where(["a > 10"])
         .update_by(ops=ub_ops, by=["b"])
         .tail(10))

        result_table = query.exec()
        self.assertTrue(result_table.size == 10)

    def test_snapshot(self):
        test_table = self.session.time_table(period=1000000)
        while test_table.snapshot().size < 100:
            time.sleep(0.001)
        query = self.session.query(test_table)
        (query.update(formulas=["Col1 = i", "Col2 = i * 2"])
         .where(["Col1 > 10"])
         .snapshot()
         .head(10))
        result_table = query.exec()

        self.assertEqual(result_table.to_arrow().num_rows, 10)

    def test_snapshot_when(self):
        source_table = (self.session.time_table(period=10_000_000)
                        .update(formulas=["Col1= i", "Col2 = i * 2"]).drop_columns(["Timestamp"]))
        trigger_table = self.session.time_table(period=1_000_000_000)
        query = self.session.query(source_table).snapshot_when(trigger_table=trigger_table, stamp_cols=["Timestamp"],
                                                               initial=True, incremental=True, history=False)
        result_table = query.exec()
        self.assertEqual(len(result_table.schema), len(source_table.schema) + 1)

    def test_agg_by(self):
        pa_table = csv.read_csv(self.csv_file)
        table = self.session.import_table(pa_table)
        query = self.session.query(table).agg_by(aggs=[agg.avg(cols=["a"])], by=["b"]) \
            .update(formulas=["Col1 = i + 1"]) \
            .tail(5).update(formulas=["Col2=i*i"])
        result_table = query.exec()
        self.assertEqual(5, result_table.size)

    def test_agg_all_by(self):
        pa_table = csv.read_csv(self.csv_file)
        table = self.session.import_table(pa_table)
        query = self.session.query(table).agg_all_by(agg=agg.avg(), by=["b"]) \
            .update(formulas=["Col1 = i + 1"]) \
            .tail(5).update(formulas=["Col2=i*i"])
        result_table = query.exec()
        self.assertEqual(5, result_table.size)

    def test_where_in(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)

        unique_table = test_table.head(num_rows=50).select_distinct(
            cols=["a", "c"]
        )

        with self.subTest("where_in"):
            query = self.session.query(test_table)
            (query.drop_columns(cols=['b'])
             .update(["f = a * 10"])
             .where_in(unique_table, cols=["c"])
             .where(["f == a * 10"]))
            result_table = query.exec()
            self.assertLessEqual(unique_table.size, result_table.size)

        with self.subTest("where_not_in"):
            query = self.session.query(test_table)
            (query.drop_columns(cols=['b'])
             .update(["f = a * 10"])
             .where_not_in(unique_table, cols=["c"])
             .where(["f == a * 10"]))
            result_table = query.exec()
            self.assertGreaterEqual(test_table.size - unique_table.size, result_table.size)


if __name__ == '__main__':
    unittest.main()
