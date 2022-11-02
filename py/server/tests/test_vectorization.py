#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import random
import time
import unittest
from types import SimpleNamespace
from typing import List, Any

import deephaven

from deephaven import DHError, read_csv, empty_table, SortDirection, AsOfMatchRule, time_table, ugp
from deephaven.agg import sum_, weighted_avg, avg, pct, group, count_, first, last, max_, median, min_, std, abs_sum, \
    var, formula, partition
from deephaven.execution_context import make_user_exec_ctx
from deephaven.html import to_html
from deephaven.pandas import to_pandas
from deephaven.table import Table, DhVectorize
from tests.testbase import BaseTestCase


class VectorizationTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        deephaven.table._test_vectorization = True
        deephaven.table._vectorized_count = 0

    def tearDown(self) -> None:
        deephaven.table._test_vectorization = False
        deephaven.table._vectorized_count = 0
        super().tearDown()

    def test_vectorization_exceptions(self):
        t = empty_table(1)

        @DhVectorize
        def vectorized_func(p1, p2):
            return p1 + p2

        def auto_func(p1, p2):
            return p1 + p2

        @DhVectorize
        def no_param_func():
            return random.randint(0, 100)

        with self.subTest("parameter number mismatch"):
            with self.assertRaises(DHError) as cm:
                t1 = t.update("X = vectorized_func(i)")
            self.assertRegex(str(cm.exception), r".*count.*mismatch", )

            with self.assertRaises(DHError) as cm:
                t1 = t.update("X = auto_func(i)")
            self.assertRegex(str(cm.exception), r".*count.*mismatch", )

        with self.subTest("can't cast return value"):
            with self.assertRaises(DHError) as cm:
                t1 = t.update("X = (float)vectorized_func(i, ii)")
            self.assertIn("can't be cast", str(cm.exception))

        with self.subTest("not be part of another expression"):
            with self.assertRaises(DHError) as cm:
                t1 = t.update("X = k + vectorized_func(i, ii)")
            self.assertIn("in another expression", str(cm.exception))

    def test_column_used_twice(self):
        def py_plus(p1, p2) -> int:
            return p1 + p2

        t = empty_table(1).update("X = py_plus(ii, ii)")

        self.assertEqual(deephaven.table._vectorized_count, 1)

    def test_vectorized_no_arg(self):
        def py_random() -> int:
            return random.randint(0, 100)

        t = empty_table(1).update("X = py_random()")

        self.assertEqual(deephaven.table._vectorized_count, 1)

    def test_vectorized_const_arg(self):
        def py_const(seed) -> int:
            random.seed(seed)
            return random.randint(0, 100)

        t = empty_table(1).update("X = py_const(3)")
        self.assertEqual(deephaven.table._vectorized_count, 1)

        seed = 10
        t = empty_table(1).update("X = py_const(seed)")
        self.assertEqual(deephaven.table._vectorized_count, 2)

    def test_multiple_formulas(self):
        def pyfunc(p1, p2, p3) -> int:
            return p1 + p2 + p3

        t = empty_table(1).update("X = i").update(["Y = pyfunc(X, i, 33)", "Z = pyfunc(X, ii, 66)"])
        self.assertEqual(deephaven.table._vectorized_count, 2)
        self.assertIn("33", t.to_string(cols=["Y"]))
        self.assertIn("66", t.to_string(cols=["Z"]))

    def test_multiple_formulas_vectorized(self):
        @DhVectorize
        def pyfunc(p1, p2, p3) -> int:
            return p1 + p2 + p3

        t = empty_table(1).update("X = i").update(["Y = pyfunc(X, i, 33)", "Z = pyfunc(X, ii, 66)"])
        self.assertEqual(deephaven.table._vectorized_count, 3)
        self.assertIn("33", t.to_string(cols=["Y"]))
        self.assertIn("66", t.to_string(cols=["Z"]))

    def test_multiple_filters(self):
        ...

    def test_multiple_filters_vectorized(self):
        ...


if __name__ == "__main__":
    unittest.main()
