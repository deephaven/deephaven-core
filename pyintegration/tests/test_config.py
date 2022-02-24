#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import os.path
import unittest

from deephaven2.config import get_server_timezone, get_log_dir
from deephaven2.time import TimeZone
from tests.testbase import BaseTestCase


class ConfigTestCase(BaseTestCase):

    def test_get_log_dir(self):
        log_dir = get_log_dir()
        self.assertTrue(os.path.exists(log_dir))

    def test_get_server_timezone(self):
        tz = get_server_timezone()
        self.assertIn(tz, [tz for tz in TimeZone])


if __name__ == '__main__':
    unittest.main()
