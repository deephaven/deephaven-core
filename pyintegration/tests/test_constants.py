#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from deephaven2.constants import *
from tests.testbase import BaseTestCase


class ConstantsTestCase(BaseTestCase):

    def test_constants_values(self):
        self.assertEqual(MAX_BYTE, 127)
        self.assertEqual(MAX_CHAR, 65534)
        self.assertEqual(MAX_DOUBLE, float('inf'))
        self.assertEqual(MAX_FINITE_DOUBLE, 1.7976931348623157e+308)
        self.assertEqual(MAX_FINITE_FLOAT, 3.4028234663852886e+38)
        self.assertEqual(MAX_FLOAT, float('inf'))
        self.assertEqual(MAX_INT, 2147483647)
        self.assertEqual(MAX_LONG, 9223372036854775807)
        self.assertEqual(MAX_SHORT, 32767)
        self.assertEqual(MIN_BYTE, -127)
        self.assertEqual(MIN_CHAR, 0)
        self.assertEqual(MIN_DOUBLE, -float('inf'))
        self.assertEqual(MIN_FINITE_DOUBLE, -1.7976931348623155e+308)
        self.assertEqual(MIN_FINITE_FLOAT, -3.4028232635611926e+38)
        self.assertEqual(MIN_FLOAT, -float('inf'))
        self.assertEqual(MIN_INT, -2147483647)
        self.assertEqual(MIN_LONG, -9223372036854775807)
        self.assertEqual(MIN_POS_DOUBLE, 5e-324)
        self.assertEqual(MIN_POS_FLOAT, 1.401298464324817e-45)
        self.assertEqual(MIN_SHORT, -32767)
        self.assertNotEqual(NAN_DOUBLE, NAN_DOUBLE)
        self.assertNotEqual(NAN_FLOAT, NULL_FLOAT)
        self.assertEqual(NEG_INFINITY_DOUBLE, -float('inf'))
        self.assertEqual(NEG_INFINITY_FLOAT, -float('inf'))
        self.assertEqual(NULL_BOOLEAN, None)
        self.assertEqual(NULL_BYTE, -128)
        self.assertEqual(NULL_CHAR, 65535)
        self.assertEqual(NULL_DOUBLE, -1.7976931348623157e+308)
        self.assertEqual(NULL_FLOAT, -3.4028234663852886e+38)
        self.assertEqual(NULL_INT, -2147483648)
        self.assertEqual(NULL_LONG, -9223372036854775808)
        self.assertEqual(NULL_SHORT, -32768)
        self.assertEqual(POS_INFINITY_DOUBLE, float('inf'))
        self.assertEqual(POS_INFINITY_FLOAT, float('inf'))
