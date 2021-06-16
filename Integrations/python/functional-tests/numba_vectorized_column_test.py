#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import unittest
from numba import vectorize, int64
from deephaven import TableTools
import bootstrap


@vectorize([int64(int64, int64)])
def vectorized_func(x, y):
    return x % 3 + y


class TestClass(unittest.TestCase):

    def test_part_of_expr(self):
        with self.assertRaises(Exception):
            t = TableTools.emptyTable(10).view("I=ii", "J=(ii * 2)").update("K = 2 * vectorized_func(I, J)")

    def test_cast(self):
        t = TableTools.emptyTable(10).view("I=ii", "J=(ii * 2)").update("K = (float)vectorized_func(I, J)")
        html_output = TableTools.html(t)
        self.assertIn("<td>9</td>", html_output)

    def test_column(self):
        t = TableTools.emptyTable(10).view("I=ii", "J=(ii * 2)").update("K = vectorized_func(I, J)")
        html_output = TableTools.html(t)
        self.assertIn("<td>9</td>", html_output)


if __name__ == "__main__":
    bootstrap.build_py_session()

    unittest.main(verbosity=2)
