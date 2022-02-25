#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import functools
import math
import time
import unittest

import numpy as np
import pandas as pd

from deephaven2 import dtypes
from deephaven2.constants import *
from deephaven2.dtypes import DateTime
from deephaven2.time import now
from tests.testbase import BaseTestCase


def remap_double(v, null_value):
    if v != v or v == NULL_DOUBLE or v == float('inf'):
        return null_value
    return v


class DTypesTestCase(BaseTestCase):
    def test_type_alias(self):
        self.assertEqual(dtypes.byte, dtypes.int8)
        self.assertEqual(dtypes.short, dtypes.int16)
        self.assertEqual(dtypes.int_, dtypes.int64)
        self.assertEqual(dtypes.long, dtypes.int64)
        self.assertEqual(dtypes.float_, dtypes.double)
        self.assertEqual(dtypes.float_, dtypes.float64)
        self.assertEqual(dtypes.double, dtypes.float64)

    def test_j_type(self):
        self.assertEqual(dtypes.bool_.j_type, jpy.get_type("java.lang.Boolean"))
        self.assertEqual(dtypes.byte.j_type, jpy.get_type("byte"))
        self.assertEqual(dtypes.short.j_type, jpy.get_type("short"))
        self.assertEqual(dtypes.char.j_type, jpy.get_type("char"))
        self.assertEqual(dtypes.int_.j_type, jpy.get_type("long"))
        self.assertEqual(dtypes.long.j_type, jpy.get_type("long"))
        self.assertEqual(dtypes.float_.j_type, jpy.get_type("double"))
        self.assertEqual(dtypes.double.j_type, jpy.get_type("double"))
        self.assertEqual(dtypes.string.j_type, jpy.get_type("java.lang.String"))
        self.assertEqual(dtypes.BigDecimal.j_type, jpy.get_type("java.math.BigDecimal"))
        self.assertEqual(dtypes.StringSet.j_type, jpy.get_type("io.deephaven.stringset.StringSet"))
        self.assertEqual(dtypes.DateTime.j_type, jpy.get_type("io.deephaven.time.DateTime"))
        self.assertEqual(dtypes.Period.j_type, jpy.get_type("io.deephaven.time.Period"))
        self.assertEqual(dtypes.PyObject.j_type, jpy.get_type("org.jpy.PyObject"))
        self.assertEqual(dtypes.JObject.j_type, jpy.get_type("java.lang.Object"))

    def test_np_type(self):
        self.assertEqual(dtypes.bool_.np_type, np.bool_)
        self.assertEqual(dtypes.byte.np_type, np.int8)
        self.assertEqual(dtypes.short.np_type, np.int16)
        self.assertEqual(dtypes.char.np_type, np.dtype('uint16'))
        self.assertEqual(dtypes.int_.np_type, np.int64)
        self.assertEqual(dtypes.long.np_type, np.int64)
        self.assertEqual(dtypes.float_.np_type, np.float64)
        self.assertEqual(dtypes.double.np_type, np.float64)
        self.assertEqual(dtypes.string.np_type, np.object_)
        self.assertEqual(dtypes.BigDecimal.np_type, np.object_)
        self.assertEqual(dtypes.StringSet.np_type, np.object_)
        self.assertEqual(dtypes.DateTime.np_type, np.dtype("datetime64[ns]"))
        self.assertEqual(dtypes.Period.np_type, np.object_)
        self.assertEqual(dtypes.PyObject.np_type, np.object_)
        self.assertEqual(dtypes.JObject.np_type, np.object_)

    def test_period(self):
        hour_period = dtypes.Period.j_type("T1H")
        self.assertTrue(isinstance(hour_period, dtypes.Period.j_type))

    def test_callable(self):
        big_decimal = dtypes.BigDecimal(12.88)
        self.assertIn("12.88", str(big_decimal))

        big_decimal2 = dtypes.BigDecimal("12.88")
        self.assertIn("12.88", str(big_decimal2))

        j_string = dtypes.string("abc")
        self.assertEqual(j_string.toString(), "abc")

    def test_array(self):
        j_array = dtypes.array(dtypes.int_, range(5))
        np_array = np.frombuffer(j_array, np.int64)
        expected = np.array([0, 1, 2, 3, 4], dtype=np.int64)
        self.assertTrue(np.array_equal(np_array, expected))

        j_array = dtypes.array(dtypes.int64, [0, 1, 2, 3, 4])
        np_array = np.frombuffer(j_array, dtype=np.int64)
        self.assertTrue(np.array_equal(np_array, expected))

    def test_integer_array(self):
        np_array = np.array([float('nan'), NULL_DOUBLE, 1.123, np.inf], dtype=np.float64)

        nulls = {dtypes.int64: NULL_LONG, dtypes.int32: NULL_INT, dtypes.short: NULL_SHORT, dtypes.byte: NULL_BYTE}
        for dt, nv in nulls.items():
            map_fn = functools.partial(remap_double, null_value=nv)
            with self.subTest(f"numpy double array to {dt}"):
                expected = [nv, nv, 1, nv]
                j_array = dtypes.array(dt, np_array, remap=map_fn)
                py_array = [x for x in j_array]
                self.assertEqual(expected, py_array)

        with self.subTest("int array from Python list"):
            expected = [1, 2, 3]
            j_array = dtypes.array(dtypes.int32, [1.1, 2.2, 3.3])
            self.assertIn("[I", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertEqual(expected, py_array)

        with self.subTest("byte array from Python list, down cast"):
            expected = [1000, 2000, 3000]
            j_array = dtypes.array(dtypes.byte, expected)
            self.assertIn("[B", str(type(j_array)))
            py_array = [x for x in j_array]
            self.assertNotEqual(expected, py_array)

    def test_floating_array(self):

        nulls = {dtypes.float_: NULL_FLOAT, dtypes.double: NULL_DOUBLE}

        np_array = np.array([float('nan'), 1.7976931348623157e+300, NULL_DOUBLE, 1.1, float('inf')], dtype=np.float64)
        for dt, nv in nulls.items():
            map_fn = functools.partial(remap_double, null_value=nv)
            with self.subTest(f"numpy double array to {dt} with mapping"):
                expected = [nv, 1.7976931348623157e+300, nv, 1.1, nv]
                j_array = dtypes.array(dt, np_array, remap=map_fn)
                py_array = [x for x in j_array]
                for i in range(4):
                    # downcast from double to float results in inf when the value is outside of float range
                    self.assertTrue(math.isclose(expected[i], py_array[i], rel_tol=1e-7) or py_array[i] == float('inf'))

        with self.subTest("double array from numpy array"):
            np_array = np.array([float('nan'), NULL_DOUBLE, 1.1, float('inf')], dtype=np.float64)
            pd_series = pd.Series(np_array)
            j_array = dtypes.array(dtypes.double, pd_series)
            py_array = [x for x in j_array]
            expected = [float('nan'), NULL_DOUBLE, 1.1, float('inf')]
            self.assertTrue(math.isnan(py_array[0]))
            self.assertEqual(expected[1:], py_array[1:])

        with self.subTest("double array from numpy long array"):
            expected = [NULL_LONG, 1, 2, 3]
            np_array = np.array(expected, dtype=np.int64)
            j_array = dtypes.array(dtypes.float64, np_array)
            self.assertIn("[D", str(type(j_array)))
            py_array = [x for x in j_array]
            for i in range(4):
                self.assertAlmostEqual(expected[i], py_array[i])

        with self.subTest("double array from Python list of integer"):
            expected = [NULL_LONG, 1, 2, 3]
            j_array = dtypes.array(dtypes.float64, expected)
            py_array = [x for x in j_array]
            for i in range(3):
                self.assertAlmostEqual(expected[i], py_array[i])

    def test_char_array(self):
        def remap_char(v):
            if v is None:
                return NULL_CHAR
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                if len(v) < 1:
                    return NULL_CHAR
                return ord(v[0])
            try:
                return int(v)
            except:
                return NULL_CHAR

        test_str = "abcdefg0123456"
        j_array = dtypes.array(dtypes.char, test_str)
        self.assertIn("[C", str(type(j_array)))
        py_array = [chr(x) for x in j_array]
        self.assertEqual(test_str, "".join(py_array))

        test_list = [None, "abc", {}, 69]
        expected = [NULL_CHAR, ord("a"), NULL_CHAR, ord("E")]
        j_array = dtypes.array(dtypes.char, test_list, remap=remap_char)
        py_array = [x for x in j_array]
        self.assertIn("[C", str(type(j_array)))
        self.assertEqual(expected, py_array)

    def test_datetime(self):
        dt1 = DateTime(round(time.time()))
        dt2 = now()
        values = [dt1, dt2, None]
        j_array = dtypes.array(DateTime, values)
        self.assertTrue(all(x == y for x, y in zip(j_array, values)))


if __name__ == '__main__':
    unittest.main()
