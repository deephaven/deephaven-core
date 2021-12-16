#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from deephaven2.constants import *
from tests.testbase import BaseTestCase


class ConstantsTestCase(BaseTestCase):

    def test_null_values(self):
        self.assertEqual(NULL_CHAR, 65535)
        self.assertEqual(NULL_FLOAT, float.fromhex('-0x1.fffffep127'))
        self.assertEqual(NULL_DOUBLE, float.fromhex('-0x1.fffffffffffffP+1023'))
        self.assertEqual(NULL_SHORT, -32768)
        self.assertEqual(NULL_INT, -0x80000000)
        self.assertEqual(NULL_LONG, -0x8000000000000000)
        self.assertEqual(NULL_BYTE, -128)
