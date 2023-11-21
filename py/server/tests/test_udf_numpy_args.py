#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

import unittest

from typing import Optional

import numpy as np

from deephaven import empty_table, DHError
from deephaven.dtypes import double_array, int32_array, long_array, int16_array, char_array, int8_array, \
    float32_array
from deephaven.table import dh_vectorize
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
        ...

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


if __name__ == "__main__":
    unittest.main()
