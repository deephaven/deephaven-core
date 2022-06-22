#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import read_csv, DHError
from deephaven.filters import RegexFilter, Filter, and_, or_, not_
from tests.testbase import BaseTestCase


class FilterTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_regex_filter(self):
        new_test_table = self.test_table.update("X = String.valueOf(d)")
        regex_filter = RegexFilter("X", "...")
        with self.assertRaises(DHError):
            filtered_table = self.test_table.where(filters=regex_filter)

        filtered_table = new_test_table.where(filters=regex_filter)
        self.assertLessEqual(filtered_table.size, new_test_table.size)

        with self.assertRaises(DHError):
            filtered_table = new_test_table.where(filters=[regex_filter, "b < 100"])

        new_test_table = new_test_table.update("Y = String.valueOf(e)")
        regex_filter1 = RegexFilter("Y", ".0.")
        filtered_table = new_test_table.where(filters=[regex_filter, regex_filter1])
        self.assertLessEqual(filtered_table.size, new_test_table.size)

    def test_filter(self):
        conditions = ["a > 100", "b < 1000", "c < 0"]
        filters = Filter.from_(conditions)
        filtered_table = self.test_table.where(filters)
        filter_and = and_(filters)
        filtered_table_and = self.test_table.where(filter_and)
        self.assert_table_equals(filtered_table, filtered_table_and)

        filter_or = or_(filters)
        filtered_table_or = self.test_table.where(filter_or)
        self.assertGreater(filtered_table_or.size, filtered_table_and.size)

        filter_not = not_(filter_or)
        filtered_table_not = self.test_table.where(filter_not)
        self.assertEqual(filtered_table_or.size + filtered_table_not.size, self.test_table.size)


if __name__ == '__main__':
    unittest.main()
