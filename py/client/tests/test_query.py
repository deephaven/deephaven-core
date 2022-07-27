#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import time

from pyarrow import csv

from pydeephaven import DHError
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
