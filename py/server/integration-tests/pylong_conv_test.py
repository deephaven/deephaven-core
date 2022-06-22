#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from test_helper import start_jvm
start_jvm()

from deephaven import empty_table

long_value = 2 ** 32 + 5


class TestClass(unittest.TestCase):

    def test_long_number_conversion(self):
        t = empty_table(1)
        result = t.update("X = long_value").to_string(1)
        self.assertEqual(long_value, int(result.split()[2]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
