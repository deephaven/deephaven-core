#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import typing
import warnings
from datetime import datetime
from typing import Optional, Union, Any, Sequence
import unittest

import numpy as np
import pandas as pd
import jpy

from deephaven import empty_table, DHError, dtypes, new_table
from deephaven.column import int_col
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

_J_TYPE_PY_TYPE_MAP: typing.Dict[str, type] = {
    "byte": int,
    "short": int,
    "char": int,
    "int": int,
    "long": int,
    "float": float,
    "double": float,
}


class UdfScalarArgsTest(BaseTestCase):
    def test_no_or_object_typehints(self):
        type_hints = {
            "",
            ": object",
            ": Any",
            ": Optional[Any]",
        }
        x_formula = "X = i % 10"
        for th in type_hints:
            with self.subTest("no null"):
                for j_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
                    y_formula = f"Y = ({j_dtype})i"
                    with self.subTest(j_dtype):
                        tbl = empty_table(10).update([x_formula, y_formula])

                        func_str = f"""
def test_udf(x {th}, y {th}) -> bool:
    return isinstance(x, int) and isinstance(y, _J_TYPE_PY_TYPE_MAP['{j_dtype}'])
                        """
                        exec(func_str, globals())
                        res = tbl.update("Z = test_udf(X, Y)")
                        self.assertEqual(10, res.to_string().count("true"))

            with self.subTest("with null"):
                for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
                    y_formula = f"Y = i % 3 == 0? {null_name} : ({j_dtype})i"
                    with self.subTest(j_dtype):
                        tbl = empty_table(10).update([x_formula, y_formula])

                        func_str = f"""
def test_udf(x {th}, y {th}) -> bool: # no auto DH null conversion
    return isinstance(x, int) and isinstance(y, _J_TYPE_PY_TYPE_MAP['{j_dtype}']) and y == {null_name}
                        """
                        exec(f"from deephaven.constants import {null_name}", globals())
                        exec(func_str, globals())
                        res = tbl.update("Z = test_udf(X, Y)")
                        self.assertEqual(4, res.to_string().count("true"))
                        exec(f"del {null_name}", globals())

    def test_np_primitive_typehints(self):
        x_formula = "X = i % 10"
        with self.subTest("no null"):
            for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
                y_formula = f"Y = ({j_dtype})i"
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
                        tbl = empty_table(10).update([x_formula, y_formula])
                        res = tbl.update("Z = test_udf(Y)")
                        self.assertEqual(10, res.to_string().count("true"))

        with self.subTest("with null"):
            for data_type, null_name in _J_TYPE_NULL_MAP.items():
                y_formula = f"Y = i % 2 == 0? {null_name} : ({data_type})i"
                with self.subTest(data_type):
                    np_type = _J_TYPE_NP_DTYPE_MAP[data_type]
                    func = f"""
def test_udf(col: {np_type}) -> bool: # no auto DH null conversion
    if col is None:
        return True
    return False
"""
                    exec(func, globals())
                    with self.subTest(data_type):
                        tbl = empty_table(10).update([x_formula, y_formula])
                        res = tbl.update("Z = test_udf(Y)")
                        self.assertEqual(0, res.to_string(num_rows=10, cols="Z").count("true"))

                    func = f"""
def test_udf(col: Optional[{np_type}]) -> bool: # Optional enables auto DH null conversion to None
    if col is None:
        return True
    return False
"""
                    exec(func, globals())
                    with self.subTest(data_type):
                        tbl = empty_table(10).update([x_formula, y_formula])
                        res = tbl.update("Z = test_udf(Y)")
                        self.assertEqual(5, res.to_string(num_rows=10, cols="Z").count("true"))

    def test_python_typehints(self):
        x_formula = "X = i % 10"
        with self.subTest("no null"):
            for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
                y_formula = f"Y = ({j_dtype})i"
                with self.subTest(j_dtype):
                    py_type = _J_TYPE_PY_TYPE_MAP[j_dtype].__name__
                    func = f"""
def test_udf(col: {py_type}) -> bool:
    if not isinstance(col, {py_type}):
        return False
    if np.isnan(col):
        return False
    else:
        return True
                    """
                    exec(func, globals())
                    with self.subTest(j_dtype):
                        tbl = empty_table(10).update([x_formula, y_formula])
                        res = tbl.update("Z = test_udf(Y)")
                        self.assertEqual(10, res.to_string().count("true"))

        with self.subTest("with null"):
            for data_type, null_name in _J_TYPE_NULL_MAP.items():
                y_formula = f"Y = i % 2 == 0? {null_name} : ({data_type})i"
                with self.subTest(data_type):
                    py_type = _J_TYPE_PY_TYPE_MAP[data_type].__name__
                    func = f"""
def test_udf(col: {py_type}) -> bool: # no auto DH null conversion
    if col is None:
        return True
    return False
                    """
                    exec(func, globals())
                    with self.subTest(data_type):
                        tbl = empty_table(10).update([x_formula, y_formula])
                        res = tbl.update("Z = test_udf(Y)")
                        self.assertEqual(0, res.to_string(num_rows=10, cols="Z").count("true"))

                    func = f"""
def test_udf(col: Optional[{py_type}]) -> bool: # Optional enables auto DH null conversion to None
    if col is None:
        return True
    return False
                    """
                    exec(func, globals())
                    with self.subTest(data_type):
                        tbl = empty_table(10).update([x_formula, y_formula])
                        res = tbl.update("Z = test_udf(Y)")
                        self.assertEqual(5, res.to_string(num_rows=10, cols="Z").count("true"))

    def test_str_bool_datetime(self):
        with self.subTest("str"):
            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? `deephaven`: null"])

            def test_udf(p1: str, p2=None) -> bool:  # str is nullable
                return p1 is None

            t1 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            self.assertEqual(5, t1.to_string(cols="Z").count("true"))

            def test_udf(p1: Union[str, None],
                         p2=None) -> bool:  # str is nullable regardless of Optional/Union[None, str]
                return p1 is None

            t2 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(5, t2.to_string(cols="Z").count("true"))

        with self.subTest("boolean"):
            def test_udf(p1: np.bool_, p2=None) -> np.bool_:
                return p1 == True

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : false"])  # no null
            t1 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            self.assertEqual(5, t1.to_string(cols="Z").count("true"))

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : null"])  # null is mapped to None
            t1 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            self.assertEqual(5, t1.to_string(cols="Z").count("true"))

            def test_udf(p1: Optional[np.bool_], p2=None) -> bool:  # Optional enables auto DH null conversion to None
                return p1 is None

            t2 = t.update(["Z = test_udf(null, Y)"])
            self.assertEqual(10, t2.to_string(cols="Z").count("true"))

        with self.subTest("datetime.datetime"):
            def test_udf(p1: datetime, p2=None) -> bool:
                return p1 is None

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? now() : null"])  # datetime.datetime is nullable
            t1 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            self.assertEqual(5, t1.to_string(cols="Z").count("false"))

        with self.subTest("np.datetime64"):
            def test_udf(p1: np.datetime64, p2=None) -> np.bool_:  # numpy supports NaT, NaT for null
                return p1.dtype.type == np.datetime64 and np.isnat(p1)

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? now() : null"])
            t1 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            self.assertEqual(5, t1.to_string(cols="Z").count("true"))

            def test_udf(p1: Union[np.datetime64, None],
                         p2=None) -> bool:  # Optional enables auto DH null conversion to None
                return p1 is None

            t2 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(5, t2.to_string(cols="Z").count("true"))

        with self.subTest("pd.Timestamp"):
            def test_udf(p1: pd.Timestamp, p2=None) -> bool:  # pandas supports NaT, NaT for null
                return pd.isna(p1)

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? now() : null"])
            t1 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            self.assertEqual(5, t1.to_string(cols="Z").count("false"))

            def test_udf(p1: Optional[pd.Timestamp],
                         p2=None) -> bool:  # Optional enables auto DH null conversion to None
                return p1 is None

            t2 = t.update(["Z = test_udf(Y)"])
            self.assertEqual(5, t2.to_string(cols="Z").count("false"))

    def test_widening_narrowing(self):
        widening_map = {
            "byte": {"short", "int", "long"},
            "short": {"int", "long"},
            "char": {"int", "long"},
            "int": {"long"},
        }

        narrowing_map = {
            "long": {"int", "short", "char", "byte"},
            "int": {"short", "char", "byte"},
            "short": {"char", "byte"},
            "char": {"short", "byte"},
            "double": {"float"},
        }

        int_to_floating_map = {
            "byte": {"float", "double"},
            "short": {"float", "double"},
            "char": {"float", "double"},
            "int": {"float", "double"},
            "long": {"float", "double"},
        }

        floating_to_int_map = {
            "float": {"byte", "short", "char", "int", "long"},
            "double": {"byte", "short", "char", "int", "long"},
        }

        with self.subTest("widening"):
            for p_type, widening_types in widening_map.items():
                with self.subTest(p_type):
                    for j_type in widening_types:
                        np_type = _J_TYPE_NP_DTYPE_MAP[j_type]
                        func_str = f"""
def test_udf(x: {np_type}) -> bool:  
    return type(x) == {np_type}
                        """
                        exec(func_str, globals())
                        t = empty_table(1).update(["X = i", f"Y = test_udf(({p_type})X)"])
                        self.assertEqual(1, t.to_string(cols="Y").count("true"))

        with self.subTest("narrowing"):
            for p_type, narrowing_types in narrowing_map.items():
                with self.subTest(p_type):
                    for j_type in narrowing_types:
                        np_type = _J_TYPE_NP_DTYPE_MAP[j_type]
                        func_str = f"""
def test_udf(x: {np_type}) -> bool:  
    return type(x) == {np_type}
                        """
                        exec(func_str, globals())
                        with self.assertRaises(DHError) as cm:
                            t = empty_table(1).update(["X = i", f"Y = test_udf(({p_type})X)"])
                        self.assertRegex(str(cm.exception), "f: Expect")

        with self.subTest("int to floating types"):
            for p_type, f_types in int_to_floating_map.items():
                with self.subTest(p_type):
                    for j_type in f_types:
                        np_type = _J_TYPE_NP_DTYPE_MAP[j_type]
                        func_str = f"""
def test_udf(x: {np_type}) -> bool:  
    return type(x) == {np_type}
                        """
                        exec(func_str, globals())
                        with self.assertRaises(DHError) as cm:
                            t = empty_table(1).update(["X = i", f"Y = test_udf(({p_type})X)"])
                        self.assertRegex(str(cm.exception), "f: Expect")

            with self.subTest("floating to int types"):
                for p_type, f_types in floating_to_int_map.items():
                    with self.subTest(p_type):
                        for j_type in f_types:
                            np_type = _J_TYPE_NP_DTYPE_MAP[j_type]
                            func_str = f"""
def test_udf(x: {np_type}) -> bool:  
    return type(x) == {np_type}
                            """
                            exec(func_str, globals())
                            with self.assertRaises(DHError) as cm:
                                t = empty_table(1).update(["X = i", f"Y = test_udf(({p_type})X)"])
                            self.assertRegex(str(cm.exception), "f: Expect")

    def test_select_most_performant(self):
        with self.subTest("np.int64 vs. int -> int"):
            def test_udf(x: Union[np.int64, float, int]) -> bool:
                return type(x) == int

            t = empty_table(10).update("X = test_udf(ii)")
            self.assertEqual(10, t.to_string().count("true"))

        with self.subTest("np.int32 vs. int -> int"):
            def test_udf(x: Union[np.int32, int]) -> bool:
                return type(x) == int

            t = empty_table(10).update("X = test_udf(i)")
            self.assertEqual(10, t.to_string().count("true"))

        with self.subTest("np.float64 vs. float -> float"):
            def test_udf(x: Union[np.float64, float, int]) -> bool:
                return type(x) == float

            t = empty_table(10).update("X = test_udf((double)i)")
            self.assertEqual(10, t.to_string().count("true"))

        with self.subTest("np.float32 vs. float -> float"):
            def test_udf(x: Union[np.float32, float, int]) -> bool:
                return type(x) == float

            t = empty_table(10).update("X = test_udf((float)i)")
            self.assertEqual(10, t.to_string().count("true"))

    def test_positional_keyword_parameters(self):
        with self.subTest("positional only params"):
            def test_udf(p1: int, p2: float, kw1: str, /) -> bool:
                return p1 == 1 and p2 == 1.0 and kw1 == "1"

            t = empty_table(1).update("X = test_udf(1, 1.0, `1`)")
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)

        with self.subTest("no keyword only params"):
            def test_udf(p1: int, p2: float, kw1: str) -> bool:
                return p1 == 1 and p2 == 1.0 and kw1 == "1"

            t = empty_table(1).update("X = test_udf(1, 1.0, `1`)")
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)

            with self.assertRaises(DHError) as cm:
                t = empty_table(1).update("X = `1`").update("Y = test_udf(1, 1.0, X = `1`)")
            self.assertRegex(str(cm.exception), "test_udf: Expected argument .* got class java.lang.Boolean")

        with self.subTest("with keyword only params"):
            def test_udf(p1: int, p2: float, *, kw1: str) -> bool:
                return p1 == 1 and p2 == 1.0 and kw1 == "1"

            with self.assertRaises(DHError) as cm:
                t = empty_table(1).update("X = test_udf(1, 1.0, `1`)")
            self.assertRegex(str(cm.exception), "takes 2 positional arguments")

    def test_varargs(self):
        cols = ["A", "B", "C", "D"]
        t = new_table([int_col(c, [0, 1, 2, 3, 4, 5, 6]) for c in cols])

        with self.subTest("valid varargs typehint"):
            def test_udf(p1: np.int32, *args: np.int64) -> np.int64:
                return sum(args)

            result = t.update(f"X = test_udf({','.join(cols)})")
            self.assertEqual(result.columns[4].data_type, dtypes.int64)

        with self.subTest("invalid varargs typehint"):
            def test_udf(p1: np.int32, *args: np.int16) -> np.int64:
                return sum(args)

            with self.assertRaises(DHError) as cm:
                t.update(f"X = test_udf({','.join(cols)})")
            self.assertRegex(str(cm.exception), "test_udf: Expected argument .* got int")

    def test_uncommon_cases(self):
        with self.subTest("f"):
            def f(p1: Union[np.ndarray[typing.Any], None]) -> bool:
                return bool(p1)

            with self.assertRaises(DHError) as cm:
                t = empty_table(10).update(["X1 = f(i)"])

        with self.subTest("f1"):
            def f1(p1: Union[np.int16, np.int32]) -> bool:
                return bool(p1)

            t = empty_table(10).update(["X1 = f1(i)"])
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)

        with self.subTest("f11"):
            def f11(p1: Union[float, np.float32]) -> bool:
                return bool(p1)

            with self.assertRaises(DHError) as cm:
                t = empty_table(10).update(["X1 = f11(i)"])

        with self.subTest("f2"):
            def f2(p1: Union[np.int32, np.float64]) -> Union[Optional[bool]]:
                return bool(p1)

            t = empty_table(10).update(["X1 = f2(i)"])
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)
            self.assertEqual(9, t.to_string().count("true"))

        with self.subTest("f21"):
            def f21(p1: Union[np.int16, np.float64]) -> Union[Optional[bool], int]:
                return bool(p1)

            with self.assertRaises(DHError) as cm:
                t = empty_table(10).update(["X1 = f21(i)"])

        with self.subTest("f3"):
            def f3(p1: Union[np.int32, np.float64], p2=None) -> bool:
                return bool(p1)

            t = empty_table(10).update(["X1 = f3(i)"])
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)

        with self.subTest("f4"):
            def f4(p1: Union[np.int16, np.float64], p2=None) -> bool:
                return bool(p1)

            t = empty_table(10).update(["X1 = f4((double)i)"])
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)
            with self.assertRaises(DHError) as cm:
                t = empty_table(10).update(["X1 = f4(now())"])
            self.assertRegex(str(cm.exception), "f4: Expected .* got .*Instant")

        with self.subTest("f41"):
            def f41(p1: Union[np.int16, np.float64, Union[Any]], p2=None) -> bool:
                return bool(p1)

            t = empty_table(10).update(["X1 = f41(now())"])
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)

        with self.subTest("f42"):
            def f42(p1: Union[np.int16, np.float64, np.datetime64], p2=None) -> bool:
                return p1.dtype.char == "M"

            t = empty_table(10).update(["X1 = f42(now())"])
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)
            self.assertEqual(10, t.to_string().count("true"))

        with self.subTest("f5"):
            def f5(col1, col2: np.ndarray[np.int32]) -> np.bool_:
                return np.nanmean(col2) == np.mean(col2)

            t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X")
            t = t.update(["X1 = f5(X, Y)"])
            with self.assertRaises(DHError) as cm:
                t = t.update(["X1 = f5(X, null)"])
            self.assertRegex(str(cm.exception), "f5: Expected .* got null")

        with self.subTest("f51"):
            def f51(col1, col2: Optional[np.ndarray[np.int32]]) -> np.bool_:
                return np.nanmean(col2) == np.mean(col2)

            t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X")
            t = t.update(["X1 = f51(X, Y)"])
            with self.assertRaises(DHError) as cm:
                t = t.update(["X1 = f51(X, null)"])
            self.assertRegex(str(cm.exception), "unsupported operand type.*NoneType")

        t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X")

        with self.subTest("f6"):
            def f6(*args: np.int32, col2: np.ndarray[np.int32]) -> bool:
                return np.nanmean(col2) == np.mean(col2)

            with self.assertRaises(DHError) as cm:
                t1 = t.update(["X1 = f6(X, Y)"])
            self.assertIn("missing 1 required keyword-only argument", str(cm.exception))

            with self.assertRaises(DHError) as cm:
                t1 = t.update(["X1 = f6(X, Y=null)"])
            self.assertRegex(str(cm.exception), "f6: Expected argument \(col2\) to be either .* got class java.lang.Boolean")

        with self.subTest("f7"):
            def f1(x: int) -> Optional[float]:
                ...

            def f2(x: float) -> Optional[int]:
                ...

            t = empty_table(1).update("X = f2(f1(ii))")
            self.assertEqual(t.columns[0].data_type, dtypes.int64)

        with self.subTest("jpy.JType"):
            def f1(x: jpy.JType) -> bool:
                return isinstance(x, jpy.JType)

            t = empty_table(1).update("X = f1(now())")
            self.assertEqual(t.columns[0].data_type, dtypes.bool_)
            self.assertEqual(1, t.to_string().count("true"))

    def test_unsupported_typehints_with_PyObject(self):
        with self.subTest("Unsupported custom class paired with org.jpy.PyObject arg"):
            class C:
                def __init__(self):
                    pass

                def value(self) -> int:
                    return 3

            def make_c() -> C:
                return C()

            def use_c(c: C) -> int:
                return c.value()

            t = empty_table(3).update(["C = make_c()", "V = use_c(C)"])
            self.assertIsNotNone(t)

            def misuse_c(c: int) -> bool:
                return isinstance(c, C)
            t1 = t.update("V = misuse_c(C)")
            self.assertEqual(t1.columns[1].data_type, dtypes.bool_)
            self.assertEqual(3, t1.to_string().count("true"))


        with self.subTest("Unsupported Python built-in type paired with org.jpy.PyObject arg"):
            from typing import Sequence

            def make_c():
                return [1, 2, 3]

            def use_c(c: Sequence) -> int:
                return c[1]

            t = empty_table(3).update(["C = make_c()", "V = use_c(C)"])
            self.assertIsNotNone(t)
            self.assertEqual(t.columns[1].data_type, dtypes.int64)

            def misuse_c(c: int) -> int:
                return c
            with self.assertRaises(DHError) as cm:
                t.update("V = misuse_c(C)")

    def test_boxed_type_arg(self):
        def f(p1: float, p2: np.float64) -> bool:
            return p1 == 0.05

        dv = 0.05
        with warnings.catch_warnings(record=True) as w:
            t = empty_table(10).update("X = f(dv, dv)")
        self.assertEqual(w[-1].category, UserWarning)
        self.assertRegex(str(w[-1].message), "numpy scalar type.*is used")
        self.assertEqual(10, t.to_string().count("true"))

    def test_no_signature(self):
        builtin_max = max
        t = empty_table(10).update("X = (int) builtin_max(1, 2, 3)")
        self.assertEqual(t.columns[0].data_type, dtypes.int32)
        self.assertEqual(10, t.to_string().count("3"))

    def test_no_name(self):
        from functools import partial
        def fn(i: int, z: int) -> int:
            return i * 5 - z
        local_fn = partial(fn, z=5)

        t = empty_table(5).update("col=i*2")
        t = t.update('col2=local_fn(col)')
        self.assertEqual(t.columns[1].data_type, dtypes.int64)
        self.assertEqual(5, t.size)


if __name__ == "__main__":
    unittest.main()
