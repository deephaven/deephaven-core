#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import math
import time
import unittest

import jpy
import numpy
import numpy as np
import pandas as pd

from deephaven2 import dtypes
from deephaven2.dtypes import DTypes
from deephaven2.constants import *
from tests.testbase import BaseTestCase


class DTypesTestCase(BaseTestCase):
    def test_type_alias(self):
        self.assertEqual(DTypes.byte, DTypes.int8)
        self.assertEqual(DTypes.short, DTypes.int16)
        self.assertEqual(DTypes.int_, DTypes.int32)
        self.assertEqual(DTypes.long, DTypes.int64)
        self.assertEqual(DTypes.float_, DTypes.single)
        self.assertEqual(DTypes.float_, DTypes.float32)
        self.assertEqual(DTypes.double, DTypes.float64)

    def test_j_type(self):
        self.assertEqual(dtypes.bool_.j_type, jpy.get_type("java.lang.Boolean"))
        self.assertEqual(dtypes.byte.j_type, jpy.get_type("byte"))
        self.assertEqual(dtypes.short.j_type, jpy.get_type("short"))
        self.assertEqual(dtypes.char.j_type, jpy.get_type("char"))
        self.assertEqual(dtypes.int_.j_type, jpy.get_type("int"))
        self.assertEqual(dtypes.long.j_type, jpy.get_type("long"))
        self.assertEqual(dtypes.float_.j_type, jpy.get_type("float"))
        self.assertEqual(dtypes.double.j_type, jpy.get_type("double"))
        self.assertEqual(dtypes.string.j_type, jpy.get_type("java.lang.String"))
        self.assertEqual(dtypes.BigDecimal.j_type, jpy.get_type("java.math.BigDecimal"))

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

        big_decimal2 = dtypes.BigDecimal("12.88")
        self.assertIn("12.88", str(big_decimal2))

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
        np_array = np.array([float('nan'), NULL_DOUBLE, 1.123, np.inf], dtype=np.float64)

        with self.subTest("long array from numpy array/pd series with nan_to_value"):
            expected = [0, NULL_LONG, 1, NULL_LONG]
            j_array = dtypes.int64.array_of(np_array, nan_to_value=0)
            self.assertIn("[J", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

            pd_series = pd.Series(np_array)
            j_array = dtypes.int64.array_of(pd_series)
            self.assertIn("[J", str(type(j_array)))
            expected = [NULL_LONG, NULL_LONG, 1, NULL_LONG]
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("int array from numpy array"):
            expected = [NULL_INT, NULL_INT, 1, NULL_INT]
            j_array = dtypes.int32.array_of(np_array)
            self.assertIn("[I", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("short array from numpy array"):
            # not sure if this is a numpy bug
            expected = [0, 0, 1, 0]
            j_array = dtypes.short.array_of(np_array)
            self.assertIn("[S", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("byte array from numpy array"):
            # same as short
            expected = [0, 0, 1, 0]
            j_array = dtypes.byte.array_of(np_array)
            self.assertIn("[B", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("int array from Python list"):
            expected = [1, 2, 3]
            j_array = dtypes.int32.array_of([1.1, 2.2, 3.3])
            self.assertIn("[I", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("short array from Python list with null_to_value"):
            py_list = [NULL_SHORT, 1000, 2000, 3000]
            expected = [0, 1000, 2000, 3000]
            j_array = dtypes.short.array_of(py_list, null_to_value=0)
            self.assertIn("[S", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("byte array from Python list, down cast"):
            expected = [1000, 2000, 3000]
            j_array = dtypes.byte.array_of(expected)
            self.assertIn("[B", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertNotEqual(expected, py_array)

    def test_floating_array_of(self):
        with self.subTest("double array from numpy array with nan2value null2value"):
            np_array = np.array([float('nan'), NULL_DOUBLE, 1.1, float('inf')], dtype=np.float64)
            expected = [NULL_DOUBLE, 0, 1.1, float('inf')]
            j_array = dtypes.float64.array_of(np_array, nan_to_value=NULL_DOUBLE, null_to_value=0.0)
            self.assertIn("[D", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("double array from numpy array"):
            np_array = np.array([float('nan'), NULL_DOUBLE, 1.1, float('inf')], dtype=np.float64)
            pd_series = pd.Series(np_array)
            j_array = dtypes.double.array_of(pd_series)
            py_array = [x for x in j_array]
            expected = [float('nan'), NULL_DOUBLE, 1.1, float('inf')]
            self.assertTrue(math.isnan(py_array[0]))
            self.assertEqual(expected[1:], py_array[1:])

        with self.subTest("float array from numpy array, down cast from double to float"):
            np_array = np.array([float('nan'), 1.1, float('inf')], dtype=np.float64)
            expected = [NULL_FLOAT, 1.1, float('inf')]
            j_array = dtypes.float32.array_of(np_array, nan_to_value=NULL_FLOAT)
            self.assertIn("[F", str(type(j_array)))
            py_array = [x for x in j_array]
            for i in range(3):
                self.assertAlmostEqual(expected[i], py_array[i])

        with self.subTest("double array from numpy long array"):
            expected = [NULL_LONG, 1, 2, 3]
            np_array = np.array(expected, dtype=np.int64)
            j_array = dtypes.float64.array_of(np_array)
            self.assertIn("[D", str(type(j_array)))
            py_array = [x for x in j_array]
            for i in range(4):
                self.assertAlmostEqual(expected[i], py_array[i])

        with self.subTest("double array from Python list of integer"):
            expected = [NULL_LONG, 1, 2, 3]
            j_array = dtypes.float64.array_of(expected)
            py_array = [x for x in j_array]
            for i in range(3):
                self.assertAlmostEqual(expected[i], py_array[i])

    def test_char_array_of(self):
        test_str = "abcdefg0123456"
        j_array = dtypes.char.array_of(test_str)
        self.assertIn("[C", str(type(j_array)))
        py_array = [chr(x) for x in j_array]
        self.assertEqual(test_str, "".join(py_array))

    def test_datetime(self):
        values = [dtypes.DateTime(round(time.time())), dtypes.DateTime.j_type.now()]
        j_array = dtypes.DateTime.array_of(values)
        self.assertTrue(2, len(j_array))
        # TODO add content comparison


if __name__ == '__main__':
    unittest.main()
