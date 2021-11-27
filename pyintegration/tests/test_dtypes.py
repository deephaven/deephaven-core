#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy
import numpy
import numpy as np
import pandas as pd

from deephaven2 import dtypes
from deephaven2.dtypes import DTypes
from tests.testbase import BaseTestCase


class DTypesTestCase(BaseTestCase):
    def test_type_alias(self):
        self.assertEqual(DTypes.short, DTypes.int16)

    def test_jdb_type(self):
        self.assertEqual(dtypes.short.j_type, jpy.get_type("short"))

    def test_qst_type(self):
        self.assertIn(".qst.type.", str(dtypes.short.qst_type))

    def test_custom_type(self):
        self.assertIn("CustomType", str(dtypes.StringSet.qst_type))

    def test_period(self):
        hour_period = dtypes.Period.j_type("T1H")
        self.assertTrue(isinstance(hour_period, dtypes.Period.j_type))

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

    def test_array_of(self):
        j_array = dtypes.int_.array_of(range(5))
        np_array = numpy.frombuffer(j_array, numpy.int32)
        self.assertTrue((np_array == numpy.array([0, 1, 2, 3, 4], dtype=numpy.int32)).all())

        j_array = dtypes.int_.array_of([0, 1, 2, 3, 4])
        np_array = numpy.frombuffer(j_array, numpy.int32)
        self.assertTrue((np_array == numpy.array([0, 1, 2, 3, 4], dtype=numpy.int32)).all())

    def test_integer_array_of(self):
        np_array = np.array([float('nan'), 1.123, float('inf')])

        expected = [dtypes.NULL_LONG, 1, dtypes.NULL_LONG]
        j_array = dtypes.int64.array_of(np_array)
        py_array = [x for x in j_array]
        self.assertEqual(expected, py_array)

        pd_series = pd.Series(np_array)
        j_array = dtypes.int64.array_of(pd_series)
        py_array = [x for x in j_array]
        self.assertEqual(expected, py_array)

        expected = [dtypes.NULL_SHORT, 1, 0]
        j_array = dtypes.short.array_of(np_array)
        py_array = [x for x in j_array]
        self.assertEqual(expected, py_array)

        expected = [1, 2, 3]
        j_array = dtypes.int64.array_of([1.1, 2.2, 3.3])
        py_array = [x for x in j_array]
        self.assertEqual(expected, py_array)

    def test_floating_array_of(self):
        np_array = np.array([float('nan'), 1.1, float('inf')])

        expected = [dtypes.NULL_DOUBLE, 1.1, float('inf')]
        j_array = dtypes.float64.array_of(np_array)
        py_array = [x for x in j_array]
        self.assertEqual(expected, py_array)

        pd_series = pd.Series(np_array)
        j_array = dtypes.double.array_of(pd_series)
        py_array = [x for x in j_array]
        self.assertEqual(expected, py_array)

        expected = [dtypes.NULL_FLOAT, 1.1, float('inf')]
        j_array = dtypes.float32.array_of(np_array)
        py_array = [x for x in j_array]
        for i in range(3):
            self.assertAlmostEqual(expected[i], py_array[i])

        expected = [1, 2, 3]
        np_array = np.array(expected)
        j_array = dtypes.float64.array_of(np_array)
        py_array = [x for x in j_array]
        for i in range(3):
            self.assertAlmostEqual(expected[i], py_array[i])

        j_array = dtypes.float64.array_of(expected)
        py_array = [x for x in j_array]
        for i in range(3):
            self.assertAlmostEqual(expected[i], py_array[i])


if __name__ == '__main__':
    unittest.main()
