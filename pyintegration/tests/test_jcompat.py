#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from tests.testbase import BaseTestCase
from deephaven import dtypes
from deephaven._jcompat import j_function


class JCompatTestCase(BaseTestCase):
    def test_j_function(self):
        def int_to_str(v: int) -> str:
            return str(v)

        j_func = j_function(int_to_str, dtypes.string)

        r = j_func.apply(10)
        self.assertEqual(r, "10")


if __name__ == "__main__":
    unittest.main()
