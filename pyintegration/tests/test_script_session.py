import unittest
from testbase import BaseTestCase
import jpy, jpyutil
from bootstrap import build_py_session


class BootstrapTestCase(BaseTestCase):
    def test_start_jvm(self):
        build_py_session()
        self.assertTrue(jpy.has_jvm())


if __name__ == '__main__':
    unittest.main()
