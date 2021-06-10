#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest
from numba import vectorize, int64, int32, int16, boolean, short
from deephaven import TableTools
import bootstrap


@vectorize([boolean(int64, int64)])
def vectorized_func(x, y):
    return x % 2 > y % 5


@vectorize([short(int64, int64)])
def vectorized_func_wrong_return_type(x, y):
    return x % 2 > y % 5


class TestClass(unittest.TestCase):

    def test_wrong_return_type(self):
        with self.assertRaises(Exception):
            t = TableTools.emptyTable(10).view("I=ii", "J=(ii * 2)")\
                .where("vectorized_func_wrong_return_type(I, J)")

    def test_filter(self):
        t = TableTools.emptyTable(10).view("I=ii", "J=(ii * 2)").where("vectorized_func(I, J)")
        html_output = TableTools.html(t)
        self.assertIn("<td>5</td><td>10</td>", html_output)


if __name__ == "__main__":
    bootstrap.build_py_session()

    unittest.main(verbosity=2)
