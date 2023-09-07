#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest
import jpy

from deephaven.config import get_server_timezone
from tests.testbase import BaseTestCase

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

class ConfigTestCase(BaseTestCase):

    def test_get_server_timezone(self):
        tz = get_server_timezone()
        self.assertEqual(tz, _JDateTimeUtils.timeZone())


if __name__ == '__main__':
    unittest.main()
