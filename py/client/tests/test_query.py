#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import time
import unittest

from pyarrow import csv

from pydeephaven import DHError
from pydeephaven.updateby import ema_tick_decay, cum_prod
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
        ub_ops = [ema_tick_decay(time_scale_ticks=100, cols=["ema_a = a"]),
                  cum_prod(cols=["cc = c", "cb = b"]),
                  ]

        query = self.session.query(test_table)
        (query.drop_columns(cols=['e'])
         .where(["a > 10"])
         .update_by(ops=ub_ops, by=["b"])
         .tail(10))

        result_table = query.exec()
        self.assertTrue(result_table.size == 10)


if __name__ == '__main__':
    unittest.main()
