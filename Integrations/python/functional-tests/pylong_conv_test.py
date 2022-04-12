#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import unittest
from deephaven import TableTools
import bootstrap

long_value = 2 ** 32 + 5


class TestClass(unittest.TestCase):

    def test_long_number_conversion(self):
        t = TableTools.emptyTable(1)
        result = TableTools.string(t.update("X = long_value"), 1)
        self.assertEqual(long_value, int(result.split()[2]))


if __name__ == "__main__":
    bootstrap.build_py_session()

    unittest.main(verbosity=2)
