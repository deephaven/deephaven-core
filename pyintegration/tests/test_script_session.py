import unittest

import jpy

from tests.utils.bootstrap import build_py_session
from testbase import BaseTestCase


class BootstrapTestCase(BaseTestCase):
    def test_start_jvm(self):
        build_py_session()
        self.assertTrue(jpy.has_jvm())


if __name__ == '__main__':
    unittest.main()
