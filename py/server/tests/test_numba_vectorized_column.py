#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest
import warnings

from numba import vectorize, int64
import math
from deephaven import empty_table, DHError
from deephaven.html import to_html

from tests.testbase import BaseTestCase


@vectorize([int64(int64, int64)])
def vectorized_func(x, y):
    return x % 3 + y

class NumbaVectorizedColumnTestCase(BaseTestCase):

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

    def test_boxed_type_arg(self):
        @vectorize(['float64(float64)'])
        def norm_cdf(x):
            """ Cumulative distribution function for the standard normal distribution """
            return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0

        # make sure we don't get a warning about numpy scalar used in annotation
        warnings.filterwarnings("error", category=UserWarning)
        with self.subTest("Boxed type 2 primitives"):
            dv = 0.05
            t = empty_table(10).update("X = norm_cdf(dv)")

        with self.subTest("Boxed type 2 primitives - 2"):
            dv = 0.05
            t = empty_table(10).update(["Y = dv*1.0", "X = norm_cdf(Y)"])
        warnings.filterwarnings("default", category=UserWarning)

if __name__ == "__main__":
    unittest.main(verbosity=2)
