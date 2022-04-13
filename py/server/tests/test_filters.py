#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest

from deephaven import read_csv, DHError
from deephaven.filters import RegexFilter
from tests.testbase import BaseTestCase


class FilterTestCase(BaseTestCase):
    def setUp(self):
        self.test_table = read_csv("tests/data/test_table.csv")

    def tearDown(self) -> None:
        self.test_table = None

    def test_something(self):
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


if __name__ == '__main__':
    unittest.main()
