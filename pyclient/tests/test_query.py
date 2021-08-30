#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import time

from pyarrow import csv

from pydeephaven import DHError
from tests.testbase import BaseTestCase


class QueryTestCase(BaseTestCase):
    def test_tail_update_static(self):
        table = self.session.empty_table(10)
        query = self.session.query(table).update(column_specs=["Col1 = i + 1"]) \
            .tail(5).update(column_specs=["Col2=i*i"])
        result_table = query.exec()
        self.assertEqual(5, result_table.size)
        time.sleep(1)
        result_table2 = query.exec()
        self.assertEqual(result_table.size, result_table2.size)

    def test_tail_update_ticking_fail(self):
        table = self.session.time_table(period=100000)
        query = self.session.query(table).update(column_specs=["Col1 = i + 1"]) \
            .tail(5).update(column_specs=["Col2 = i * i"])

        with self.assertRaises(DHError):
            result_table = query.exec()
            self.assertFalse(result_table.is_static)

    def test_tail_update_join_fail(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        right_table = self.session.empty_table(1000).update(["a = i"])
        query = self.session.query(test_table)
        (query.drop_columns(column_names=['c'])
            .where(["a > 10"])
            .tail(10)
            .join(right_table, keys=["a"]))

        with self.assertRaises(DHError):
            query.exec()

    def test_tail_update_join(self):
        pa_table = csv.read_csv(self.csv_file)
        test_table = self.session.import_table(pa_table)
        right_table = self.session.empty_table(1000).update(["a = ii"])
        query = self.session.query(test_table)
        (query.drop_columns(column_names=['c'])
         .where(["a > 10"]).tail(10)
         .join(right_table, keys=["a"]))

        result_table = query.exec()
        self.assertTrue(result_table.size > 0)
