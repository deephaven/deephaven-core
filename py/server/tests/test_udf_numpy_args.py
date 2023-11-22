#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import typing
from typing import Optional, Union, Any
import unittest


import numpy as np
import numpy.typing as npt

from deephaven import empty_table, DHError, dtypes
from deephaven.dtypes import double_array, int32_array, long_array, int16_array, char_array, int8_array, \
    float32_array
from tests.testbase import BaseTestCase

_J_TYPE_NULL_MAP = {
    "double": "NULL_DOUBLE",
    "float": "NULL_FLOAT",
    "int": "NULL_INT",
    "long": "NULL_LONG",
    "short": "NULL_SHORT",
    "byte": "NULL_BYTE",
}

_J_TYPE_NP_DTYPE_MAP = {
    "double": "np.float64",
    "float": "np.float32",
    "int": "np.int32",
    "long": "np.int64",
    "short": "np.int16",
    "byte": "np.int8",
}

_J_TYPE_J_ARRAY_TYPE_MAP = {
    "double": double_array,
    "float": float32_array,
    "int": int32_array,
    "long": long_array,
    "short": int16_array,
    "byte": int8_array,
}


class UDFNumpyTest(BaseTestCase):
    def test_j_to_py_no_annotation_no_null(self):
        col1_formula = "Col1 = i % 10"
        for j_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
            col2_formula = f"Col2 = ({j_dtype})i"
            with self.subTest(j_dtype):
                tbl = empty_table(100).update([col1_formula, col2_formula]).group_by("Col1")

                func_str = f"""
def test_udf(col1, col2) -> bool:
    j_array_type = _J_TYPE_J_ARRAY_TYPE_MAP[{j_dtype!r}].j_type
    return (isinstance(col1, int) or isinstance(col1, float)) and isinstance(col2, j_array_type)
                        """
                exec(func_str, globals())
                res = tbl.update("Col3 = test_udf(Col1, Col2)")
                self.assertEqual(10, res.to_string().count("true"))

    def test_j_to_py_no_annotation_null(self):
        col1_formula = "Col1 = i % 10"
        for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
            col2_formula = f"Col2 = i % 3 == 0? {null_name} : ({j_dtype})i"
            with self.subTest(j_dtype):
                tbl = empty_table(100).update([col1_formula, col2_formula]).group_by("Col1")

                func_str = f"""
def test_udf(col1, col2) -> bool:
    j_array_type = _J_TYPE_J_ARRAY_TYPE_MAP[{j_dtype!r}].j_type
    return ((isinstance(col1, int) or isinstance(col1, float)) and isinstance(col2, j_array_type)
        and np.any(np.array(col2) == {null_name}))
                        """
                exec(f"from deephaven.constants import {null_name}", globals())
                exec(func_str, globals())
                res = tbl.update("Col3 = test_udf(Col1, Col2)")
                self.assertEqual(10, res.to_string().count("true"))
                exec(f"del {null_name}", globals())

    def test_jarray_to_np_array_no_null(self):
        col1_formula = "Col1 = i % 10"
        for j_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
            col2_formula = f"Col2 = ({j_dtype})i"
            with self.subTest(j_dtype):
                tbl = empty_table(100).update([col1_formula, col2_formula]).group_by("Col1")

                func_str = f"""
def test_udf(col1, col2: np.ndarray[{np_dtype}]) -> bool:
    return np.nanmean(col2) == np.mean(col2)
                """
                exec(func_str, globals())
                res = tbl.update("Col3 = test_udf(Col1, Col2)")
                self.assertEqual(10, res.to_string().count("true"))

    def test_jarray_to_np_array_null(self):
        col1_formula = "Col1 = i % 10"
        for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
            col2_formula = f"Col2 = i % 3 == 0? {null_name} : ({j_dtype})i"
            with self.subTest(j_dtype):
                tbl = empty_table(100).update([col1_formula, col2_formula]).group_by("Col1")

                func_str = f"""
def test_udf(col1, col2: np.ndarray[{_J_TYPE_NP_DTYPE_MAP[j_dtype]}]) -> bool:
    return np.nanmean(col2) != np.mean(col2)
                """
                exec(func_str, globals())
                if j_dtype in ("float", "double"):
                    res = tbl.update("Col3 = test_udf(Col1, Col2)")
                    self.assertEqual(10, res.to_string().count("true"))
                else:
                    with self.assertRaises(DHError) as cm:
                        tbl.update("Col3 = test_udf(Col1, Col2)")

    def test_j_scalar_to_py_no_null(self):
        col1_formula = "Col1 = i % 10"
        for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
            col2_formula = f"Col2 = ({j_dtype})i"
            with self.subTest(j_dtype):
                np_type = _J_TYPE_NP_DTYPE_MAP[j_dtype]
                func = f"""
def test_udf(col: {np_type}) -> bool:
    if np.isnan(col):
        return True
    else:
        return False
        """
                exec(func, globals())
                with self.subTest(j_dtype):
                    tbl = empty_table(100).update([col1_formula, col2_formula])
                    res = tbl.update("Col3 = test_udf(Col2)")
                    self.assertEqual(0, res.to_string().count("true"))

                func = f"""
def test_udf(col: Optional[{np_type}]) -> bool:
    if col is None:
        return True
    else:
        return False
        """
                exec(func, globals())
                with self.subTest(j_dtype):
                    tbl = empty_table(100).update([col1_formula, col2_formula])
                    res = tbl.update("Col3 = test_udf(Col2)")
                    self.assertEqual(0, res.to_string().count("true"))

    def test_j_scalar_to_py_null(self):
        col1_formula = "Col1 = i % 10"
        for data_type, null_name in _J_TYPE_NULL_MAP.items():
            col2_formula = f"Col2 = i % 3 == 0? {null_name} : ({data_type})i"
            with self.subTest(data_type):
                np_type = _J_TYPE_NP_DTYPE_MAP[data_type]
                func = f"""
def test_udf(col: {np_type}) -> bool:
    if np.isnan(col):
        return True
    else:
        return False
"""
                exec(func, globals())
                with self.subTest(data_type):
                    tbl = empty_table(100).update([col1_formula, col2_formula])
                    if data_type in ("float", "double"):
                        res = tbl.update("Col3 = test_udf(Col2)")
                        self.assertEqual(4, res.to_string().count("true"))
                    else:
                        with self.assertRaises(DHError) as cm:
                            res = tbl.update("Col3 = test_udf(Col2)")

                func = f"""
def test_udf(col: Optional[{np_type}]) -> bool:
    if col is None:
        return True
    else:
        return False
"""
                exec(func, globals())
                with self.subTest(data_type):
                    tbl = empty_table(100).update([col1_formula, col2_formula])
                    res = tbl.update("Col3 = test_udf(Col2)")
                    self.assertEqual(4, res.to_string().count("true"))

    def test_weird_cases(self):
        def f(p1: Union[np.ndarray[typing.Any], None]) -> bool:
            return bool(p1)
        with self.assertRaises(DHError) as cm:
            t = empty_table(10).update(["X1 = f(i)"])

        def f1(p1: Union[np.int16, np.int32]) -> bool:
            return bool(p1)
        with self.assertRaises(DHError) as cm:
            t = empty_table(10).update(["X1 = f1(i)"])

        def f11(p1: Union[float, np.float32]) -> bool:
            return bool(p1)
        with self.assertRaises(DHError) as cm:
            t = empty_table(10).update(["X1 = f11(i)"])

        def f2(p1: Union[np.int16, np.float64]) -> Union[Optional[bool]]:
            return bool(p1)
        t = empty_table(10).update(["X1 = f2(i)"])
        self.assertEqual(t.columns[0].data_type, dtypes.bool_)
        self.assertEqual(9, t.to_string().count("true"))

        def f21(p1: Union[np.int16, np.float64]) -> Union[Optional[bool], int]:
            return bool(p1)
        with self.assertRaises(DHError) as cm:
            t = empty_table(10).update(["X1 = f21(i)"])

        def f3(p1: Union[np.int16, np.float64], p2=None) -> bool:
            return bool(p1)
        t = empty_table(10).update(["X1 = f3(i)"])
        self.assertEqual(t.columns[0].data_type, dtypes.bool_)

        def f4(p1: Union[np.int16, np.float64], p2=None) -> bool:
            return bool(p1)
        t = empty_table(10).update(["X1 = f4((double)i)"])
        self.assertEqual(t.columns[0].data_type, dtypes.bool_)
        with self.assertRaises(DHError) as cm:
            t = empty_table(10).update(["X1 = f4(now())"])

        def f41(p1: Union[np.int16, np.float64, Union[Any]], p2=None) -> bool:
            return bool(p1)
        t = empty_table(10).update(["X1 = f41(now())"])
        self.assertEqual(t.columns[0].data_type, dtypes.bool_)

        def f42(p1: Union[np.int16, np.float64, np.datetime64], p2=None) -> bool:
            return p1.dtype.char == "M"
        t = empty_table(10).update(["X1 = f42(now())"])
        self.assertEqual(t.columns[0].data_type, dtypes.bool_)
        self.assertEqual(10, t.to_string().count("true"))

        def f5(col1, col2: np.ndarray[np.int32]) -> bool:
            return np.nanmean(col2) == np.mean(col2)
        t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X")
        t = t.update(["X1 = f5(X, Y)"])
        with self.assertRaises(DHError) as cm:
            t = t.update(["X1 = f5(X, null)"])

        def f51(col1, col2: Optional[np.ndarray[np.int32]]) -> bool:
            return np.nanmean(col2) == np.mean(col2)
        t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X")
        t = t.update(["X1 = f51(X, Y)"])
        with self.assertRaises(DHError) as cm:
            t = t.update(["X1 = f51(X, null)"])


if __name__ == "__main__":
    unittest.main()
