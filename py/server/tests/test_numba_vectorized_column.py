#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from numba import vectorize, int64

from deephaven import empty_table, DHError
from deephaven.html import to_html
from tests.testbase import BaseTestCase


@vectorize([int64(int64, int64)])
def vectorized_func(x, y):
    return x % 3 + y


class TestNumbaVectorizedColumnClass(BaseTestCase):

    def test_part_of_expr(self):
        with self.assertRaises(Exception):
            t = empty_table(10).view(formulas=["I=ii", "J=(ii * 2)"]).update("K = 2 * vectorized_func(I, J)")

    def test_cast(self):
        with self.assertRaises(DHError) as cm:
            t = empty_table(10).view(formulas=["I=ii", "J=(ii * 2)"]).update("K = (float)vectorized_func(I, J)")
        self.assertIn("can't be cast", str(cm.exception))

    def test_column(self):
        t = empty_table(10).view(formulas=["I=ii", "J=(ii * 2)"]).update("K = vectorized_func(I, J)")
        html_output = to_html(t)
        self.assertIn("<td>9</td>", html_output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
