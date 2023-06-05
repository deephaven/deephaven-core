#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import os.path
import unittest

from deephaven.config import get_server_timezone
from deephaven.time import time_zone
from tests.testbase import BaseTestCase


class ConfigTestCase(BaseTestCase):

    def test_get_server_timezone(self):
        tz = get_server_timezone()
        self.assertEqual(tz, time_zone(None))


if __name__ == '__main__':
    unittest.main()
