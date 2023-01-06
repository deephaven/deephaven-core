#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import os.path
import unittest

from deephaven.config import get_server_timezone
from deephaven.time import TimeZone
from tests.testbase import BaseTestCase


class ConfigTestCase(BaseTestCase):

    def test_get_server_timezone(self):
        tz = get_server_timezone()
        self.assertIn(tz, [tz for tz in TimeZone])


if __name__ == '__main__':
    unittest.main()
