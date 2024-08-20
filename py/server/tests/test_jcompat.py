#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import dtypes
from deephaven.jcompat import j_function, j_lambda, AutoCloseable
from tests.testbase import BaseTestCase

import jpy

_JSharedContext = jpy.get_type("io.deephaven.engine.table.SharedContext")

class JCompatTestCase(BaseTestCase):
    def test_j_function(self):
        def int_to_str(v: int) -> str:
            return str(v)

        j_func = j_function(int_to_str, dtypes.string)

        r = j_func.apply(10)
        self.assertEqual(r, "10")

    def test_j_lambda(self):
        def int_to_str(v: int) -> str:
            return str(v)
        j_func = j_lambda(int_to_str, jpy.get_type('java.util.function.Function'), dtypes.string)

        r = j_func.apply(10)
        self.assertEqual(r, "10")

    def test_auto_closeable(self):
        auto_closeable = AutoCloseable(_JSharedContext.makeSharedContext())
        with auto_closeable:
            self.assertEqual(auto_closeable.closed, False)
        self.assertEqual(auto_closeable.closed, True)


if __name__ == "__main__":
    unittest.main()
