#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, DHError
from deephaven.plot.selectable_dataset import one_click, one_click_partitioned_table
from tests.testbase import BaseTestCase


class SelectableDatasetTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None
        super().tearDown()

    def test_one_click(self):
        sds = one_click(self.test_table, by=['a', 'b'])
        self.assertIsNotNone(sds)
        sds = one_click(self.test_table, by=['a', 'b'], require_all_filters=True)
        self.assertIsNotNone(sds)

        with self.assertRaises(DHError):
            sds = one_click(self.test_table)

        with self.assertRaises(DHError):
            sds = one_click(self.test_table)

    def test_one_click_tm(self):
        pt = self.test_table.partition_by(["c", "e"])
        sds = one_click_partitioned_table(pt)
        self.assertIsNotNone(sds)
        sds = one_click_partitioned_table(pt, require_all_filters=True)
        self.assertIsNotNone(sds)


if __name__ == '__main__':
    unittest.main()
