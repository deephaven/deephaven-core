#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven2 import get_workspace_root
from tests.testbase import BaseTestCase


class UtilsTestCase(BaseTestCase):
    def test_get_workspace_root(self):
        root_path = get_workspace_root()
        self.assertNotEqual("", root_path)


if __name__ == '__main__':
    unittest.main()
