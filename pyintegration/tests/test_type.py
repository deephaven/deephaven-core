#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy
import numpy

from deephaven2 import dtypes
from deephaven2.dtypes import DType
from tests.testbase import BaseTestCase


class TypeTestCase(BaseTestCase):
    def test_type_alias(self):
        self.assertEqual(DType.short, DType.int16)

    def test_type_value(self):
        self.assertEqual(DType.double.value, DType.double.qst_type)

    def test_jpy_type(self):
        self.assertEqual(DType.short.j_type, jpy.get_type("short"))

    def test_qst_type(self):
        self.assertIn(".qst.type.", str(DType.short.qst_type))

    def test_custom_type(self):
        self.assertIn("CustomType", str(DType.StringSet.qst_type))

    def test_period(self):
        hour_period = DType.Period.j_type("T1H")
        self.assertTrue(isinstance(hour_period, DType.Period.j_type))

    def test_callable(self):
        big_decimal = dtypes.BigDecimal(12.88)
        self.assertIn("12.88", str(big_decimal))

        big_decimal1 = dtypes.BigDecimal("12.88")
        self.assertTrue(big_decimal.compareTo(big_decimal1))

    def test_array(self):
        j_array = dtypes.int_.array(5)
        for i in range(5):
            j_array[i] = i
        np_array = numpy.frombuffer(j_array, numpy.int32)
        self.assertTrue((np_array == numpy.array([0, 1, 2, 3, 4], dtype=numpy.int32)).all())

    def test_array_from(self):
        j_array = dtypes.int_.array_from(range(5))
        np_array = numpy.frombuffer(j_array, numpy.int32)
        self.assertTrue((np_array == numpy.array([0, 1, 2, 3, 4], dtype=numpy.int32)).all())


if __name__ == '__main__':
    unittest.main()
