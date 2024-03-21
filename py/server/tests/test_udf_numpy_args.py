#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
    "byte": "NULL_BYTE",
    "short": "NULL_SHORT",
    "char": "NULL_CHAR",
    "int": "NULL_INT",
    "long": "NULL_LONG",
    "float": "NULL_FLOAT",
    "double": "NULL_DOUBLE",
}

_J_TYPE_NP_DTYPE_MAP = {
    "byte": "np.int8",
    "short": "np.int16",
    "char": "np.uint16",
    "int": "np.int32",
    "long": "np.int64",
    "float": "np.float32",
    "double": "np.float64",
}

_J_TYPE_J_ARRAY_TYPE_MAP = {
    "byte": int8_array,
    "short": int16_array,
    "char": char_array,
    "int": int32_array,
    "long": long_array,
    "float": float32_array,
    "double": double_array,
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
    return isinstance(col1, int) and isinstance(col2, j_array_type)
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
    return (isinstance(col1, int) and isinstance(col2, j_array_type) and np.any(np.array(col2) == {null_name}))
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
    return (isinstance(col1, int) and isinstance(col2, np.ndarray) and col2.dtype.type == {np_dtype} and np.nanmean(
    col2) == np.mean( col2))
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
    return (isinstance(col1, int) and isinstance(col2, np.ndarray) and col2.dtype.type == 
    {_J_TYPE_NP_DTYPE_MAP[j_dtype]} and np.nanmean(col2) != np.mean( col2))
                """
                exec(func_str, globals())

                # for floating point types, DH nulls are auto converted to np.nan
                # for integer types, DH nulls in the array raise exceptions
                if j_dtype in ("float", "double"):
                    res = tbl.update("Col3 = test_udf(Col1, Col2)")
                    self.assertEqual(10, res.to_string().count("true"))
                else:
                    with self.assertRaises(DHError) as cm:
                        tbl.update("Col3 = test_udf(Col1, Col2)")
                    self.assertRegex(str(cm.exception), "Java .* array contains Deephaven null values, but numpy .* "
                                                        "array does not support ")

    def test_j_scalar_to_py_no_null(self):
        col1_formula = "Col1 = i % 10"
        for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
            col2_formula = f"Col2 = ({j_dtype})i"
            with self.subTest(j_dtype):
                np_type = _J_TYPE_NP_DTYPE_MAP[j_dtype]
                func = f"""
def test_udf(col: {np_type}) -> bool:
    if not isinstance(col, {np_type}):
        return False
    if np.isnan(col):
        return False
    else:
        return True
        """
                exec(func, globals())
                with self.subTest(j_dtype):
                    tbl = empty_table(100).update([col1_formula, col2_formula])
                    res = tbl.update("Col3 = test_udf(Col2)")
                    self.assertEqual(10, res.to_string().count("true"))

                func = f"""
def test_udf(col: Optional[{np_type}]) -> bool:
    if not isinstance(col, {np_type}):
        return False
    if col is None:
        return False
    else:
        return True
        """
                exec(func, globals())
                with self.subTest(j_dtype):
                    tbl = empty_table(100).update([col1_formula, col2_formula])
                    res = tbl.update("Col3 = test_udf(Col2)")
                    self.assertEqual(10, res.to_string().count("true"))

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
        if not isinstance(col, {np_type}):
            return True
        return False
"""
                exec(func, globals())
                with self.subTest(data_type):
                    tbl = empty_table(100).update([col1_formula, col2_formula])
                    # for floating point types, DH nulls are auto converted to np.nan
                    # for integer types, DH nulls in the array raise exceptions
                    if data_type in ("float", "double"):
                        res = tbl.update("Col3 = test_udf(Col2)")
                        self.assertEqual(4, res.to_string().count("true"))
                    else:
                        with self.assertRaises(DHError) as cm:
                            res = tbl.update("Col3 = test_udf(Col2)")
                        self.assertRegex(str(cm.exception), "Argument .* is not compatible with annotation*")

                func = f"""
def test_udf(col: Optional[{np_type}]) -> bool:
    if col is None:
        return True
    else:
        if not isinstance(col, {np_type}):
            return True
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
        self.assertRegex(str(cm.exception), "Argument .* is not compatible with annotation*")

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
        self.assertRegex(str(cm.exception), "Argument .* is not compatible with annotation*")

        def f51(col1, col2: Optional[np.ndarray[np.int32]]) -> bool:
            return np.nanmean(col2) == np.mean(col2)

        t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X")
        t = t.update(["X1 = f51(X, Y)"])
        with self.assertRaises(DHError) as cm:
            t = t.update(["X1 = f51(X, null)"])
        self.assertRegex(str(cm.exception), "unsupported operand type.*NoneType")

        t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X")

        def f6(*args: np.int32, col2: np.ndarray[np.int32]) -> bool:
            return np.nanmean(col2) == np.mean(col2)
        with self.assertRaises(DHError) as cm:
            t1 = t.update(["X1 = f6(X, Y)"])
        self.assertIn("missing 1 required keyword-only argument", str(cm.exception))

        with self.assertRaises(DHError) as cm:
            t1 = t.update(["X1 = f6(X, Y=null)"])
        self.assertIn("not compatible with annotation", str(cm.exception))

    def test_str_bool_datetime_array(self):
        with self.subTest("str"):
            def f1(p1: np.ndarray[str], p2=None) -> bool:
                return bool(len(p1))

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? `deephaven`: null"]).group_by("X")
            t1 = t.update(["X1 = f1(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            with self.assertRaises(DHError) as cm:
                t2 = t.update(["X1 = f1(null, Y )"])
            self.assertRegex(str(cm.exception), "Argument .* is not compatible with annotation*")

            def f11(p1: Union[np.ndarray[str], None], p2=None) -> bool:
                return bool(len(p1)) if p1 is not None else False
            t2 = t.update(["X1 = f11(null, Y)"])
            self.assertEqual(3, t2.to_string().count("false"))

        with self.subTest("datetime"):
            def f2(p1: np.ndarray[np.datetime64], p2=None) -> bool:
                return bool(len(p1))

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? now() : null"]).group_by("X")
            t1 = t.update(["X1 = f2(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            with self.assertRaises(DHError) as cm:
                t2 = t.update(["X1 = f2(null, Y )"])
            self.assertRegex(str(cm.exception), "Argument .* is not compatible with annotation*")

            def f21(p1: Union[np.ndarray[np.datetime64], None], p2=None) -> bool:
                return bool(len(p1)) if p1 is not None else False
            t2 = t.update(["X1 = f21(null, Y)"])
            self.assertEqual(3, t2.to_string().count("false"))

        with self.subTest("boolean"):
            def f3(p1: np.ndarray[np.bool_], p2=None) -> bool:
                return bool(len(p1))

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : null"]).group_by("X")
            with self.assertRaises(DHError) as cm:
                t1 = t.update(["X1 = f3(Y)"])
            self.assertRegex(str(cm.exception), "Java .* array contains Deephaven null values, but numpy .* "
                                                "array does not support ")

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : false"]).group_by("X")
            t1 = t.update(["X1 = f3(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            with self.assertRaises(DHError) as cm:
                t2 = t.update(["X1 = f3(null, Y )"])
            self.assertRegex(str(cm.exception), "Argument 'p1': None is not compatible with annotation")

            def f31(p1: Optional[np.ndarray[bool]], p2=None) -> bool:
                return bool(len(p1)) if p1 is not None else False
            t2 = t.update(["X1 = f31(null, Y)"])
            self.assertEqual(3, t2.to_string("X1").count("false"))

    def test_str_bool_datetime_scalar(self):
        with self.subTest("str"):
            def f1(p1: str, p2=None) -> bool:
                return p1 is None

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? `deephaven`: null"])
            with self.assertRaises(DHError) as cm:
                t1 = t.update(["X1 = f1(Y)"])
            self.assertRegex(str(cm.exception), "Argument 'p1': None is not compatible with annotation")

            def f11(p1: Union[str, None], p2=None) -> bool:
                return p1 is None
            t2 = t.update(["X1 = f11(Y)"])
            self.assertEqual(5, t2.to_string().count("false"))

        with self.subTest("datetime"):
            def f2(p1: np.datetime64, p2=None) -> bool:
                return p1 is None

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? now() : null"])
            with self.assertRaises(DHError) as cm:
                t1 = t.update(["X1 = f2(Y)"])
            self.assertRegex(str(cm.exception), "Argument 'p1': None is not compatible with annotation")

            def f21(p1: Union[np.datetime64, None], p2=None) -> bool:
                return p1 is None
            t2 = t.update(["X1 = f21(Y)"])
            self.assertEqual(5, t2.to_string().count("false"))

        with self.subTest("boolean"):
            def f3(p1: np.bool_, p2=None) -> bool:
                return p1 is None

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : null"])
            with self.assertRaises(DHError) as cm:
                t1 = t.update(["X1 = f3(Y)"])
            self.assertRegex(str(cm.exception), "Argument 'p1': None is not compatible with annotation")

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : false"])
            t1 = t.update(["X1 = f3(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            self.assertEqual(0, t1.to_string("X1").count("true"))

            def f31(p1: Optional[np.bool_], p2=None) -> bool:
                return p1 is None
            t2 = t.update(["X1 = f31(null, Y)"])
            self.assertEqual(10, t2.to_string("X1").count("true"))

    def test_non_np_typehints(self):
        py_types = {"int", "float"}

        for p_type in py_types:
            with self.subTest(p_type):
                func_str = f"""
def f(x: {p_type}) -> bool:  # note typing
    return type(x) == {p_type}
"""
                exec(func_str, globals())
                t = empty_table(1).update(["X = i", f"Y = f(({p_type})X)"])
                self.assertEqual(1, t.to_string(cols="Y").count("true"))


        np_int_types = {"np.int8", "np.int16", "np.int32", "np.int64"}
        for p_type in np_int_types:
            with self.subTest(p_type):
                func_str = f"""
def f(x: {p_type}) -> bool:  # note typing
    return type(x) == {p_type}
"""
                exec(func_str, globals())
                t = empty_table(1).update(["X = i", f"Y = f(X)"])
                self.assertEqual(1, t.to_string(cols="Y").count("true"))

        np_floating_types = {"np.float32", "np.float64"}
        for p_type in np_floating_types:
            with self.subTest(p_type):
                func_str = f"""
def f(x: {p_type}) -> bool:  # note typing
    return type(x) == {p_type}
"""
                exec(func_str, globals())
                t = empty_table(1).update(["X = i", f"Y = f((float)X)"])
                self.assertEqual(1, t.to_string(cols="Y").count("true"))

if __name__ == "__main__":
    unittest.main()
