#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven import garbage_collect
from tests.testbase import BaseTestCase


class GcTestCase(BaseTestCase):
    def test_garbage_collect(self):
        self.assertIsNone(garbage_collect())


if __name__ == "__main__":
    unittest.main()
