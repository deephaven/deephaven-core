#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import dtypes
from deephaven.jcompat import j_function, j_lambda, AutoCloseable, j_array_list, j_collection_to_list, j_hashset
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

    def test_j_collection_to_list(self):
        lst = [2, 1, 3]
        j_list = j_array_list(lst)
        self.assertEqual(lst, j_collection_to_list(j_list))

        s = set(lst)
        j_set = j_hashset(s)
        self.assertEqual(s, set(j_collection_to_list(j_set)))


if __name__ == "__main__":
    unittest.main()
