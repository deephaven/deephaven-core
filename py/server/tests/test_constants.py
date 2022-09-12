#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from deephaven import empty_table
from deephaven.constants import *
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

    def test_return_null_long(self):
        null_byte = NULL_BYTE
        null_short = NULL_SHORT
        null_int = NULL_INT
        null_long = NULL_LONG

        def return_null_long():
            return NULL_LONG

        t = empty_table(9).update(
            ["X = null_byte", "Y = null_short", "YY = null_int", "Z = null_long", "ZZ = (long)return_null_long()"])
        self.assertEqual(t.to_string().count("null"), 45)
