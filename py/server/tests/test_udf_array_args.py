#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import typing
from typing import Optional, Union, Any, Sequence, List, Tuple, Set, Dict, Literal
import unittest

import numpy as np

from deephaven import empty_table, DHError, dtypes
from tests.testbase import BaseTestCase
from .test_udf_scalar_args import _J_TYPE_NP_DTYPE_MAP, _J_TYPE_NULL_MAP, _J_TYPE_J_ARRAY_TYPE_MAP


class UdfArrayArgsTest(BaseTestCase):
    def test_no_or_object_typehints(self):
        type_hints = {
            "",
            ": object",
            ": Any",
            ": Optional[Any]",
        }
        x_formula = "X = i % 10"
        for th in type_hints:
            with self.subTest("no null cells"):
                for j_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
                    y_formula = f"Y = ({j_dtype})i"
                    with self.subTest(j_dtype):
                        tbl = empty_table(100).update([x_formula, y_formula]).group_by("X")

                        func_str = f"""
def test_udf(x {th}, y {th}) -> bool:
    j_array_type = _J_TYPE_J_ARRAY_TYPE_MAP[{j_dtype!r}].j_type
    return isinstance(x, int) and isinstance(y, j_array_type)
                            """
                        exec(func_str, globals())
                        res = tbl.update("Z = test_udf(X, Y)")
                        self.assertEqual(10, res.to_string().count("true"))

            with self.subTest("with null cells"):  # no auto DH null to np.nan conversion
                for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
                    y_formula = f"Y = i % 3 == 0? {null_name} : ({j_dtype})i"
                    with self.subTest(j_dtype):
                        tbl = empty_table(100).update([x_formula, y_formula]).group_by("X")

                        func_str = f"""
def test_udf(x {th}, y {th}) -> bool:
    j_array_type = _J_TYPE_J_ARRAY_TYPE_MAP[{j_dtype!r}].j_type
    return (isinstance(x, int) and isinstance(y, j_array_type) and np.any(np.array(y) == {null_name}))
                            """
                        exec(f"from deephaven.constants import {null_name}", globals())
                        exec(func_str, globals())
                        res = tbl.update("Z = test_udf(X, Y)")
                        self.assertEqual(10, res.to_string().count("true"))
                        exec(f"del {null_name}", globals())

            with self.subTest("null arrays"):  # arrays are considered nullable
                for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
                    y_formula = f"Y = i % 3 == 0? {null_name} : ({j_dtype})i"
                    with self.subTest(j_dtype):
                        tbl = empty_table(100).update([x_formula, y_formula]).group_by("X").update(
                            "Y = X % 2 == 0? null : Y")

                        func_str = f"""
def test_udf(x {th}, y {th}) -> bool:
    j_array_type = _J_TYPE_J_ARRAY_TYPE_MAP[{j_dtype!r}].j_type
    return y is None
                            """
                        exec(func_str, globals())
                        res = tbl.update("Z = test_udf(X, Y)")
                        self.assertEqual(5, res.to_string().count("true"))

    def test_np_primitive_array(self):
        x_formula = "X = i % 10"
        with self.subTest("no null elements"):
            for j_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
                y_formula = f"Y = ({j_dtype})i"
                with self.subTest(j_dtype):
                    tbl = empty_table(100).update([x_formula, y_formula]).group_by("X")

                    func_str = f"""
def test_udf(x, y: np.ndarray[{np_dtype}]) -> bool:
    return (isinstance(x, int) and isinstance(y, np.ndarray) and y.dtype.type == {np_dtype} and np.nanmean(
    y) == np.mean( y))
                            """
                    exec(func_str, globals())
                    res = tbl.update("Z = test_udf(X, Y)")
                    self.assertEqual(10, res.to_string().count("true"))

        with self.subTest("with null elements"):
            for j_dtype, null_name in _J_TYPE_NULL_MAP.items():
                y_formula = f"Y = i % 3 == 0? {null_name} : ({j_dtype})i"
                with self.subTest(j_dtype):
                    tbl = empty_table(100).update([x_formula, y_formula]).group_by("X")

                    func_str = f"""
def test_udf(x, y: np.ndarray[{_J_TYPE_NP_DTYPE_MAP[j_dtype]}]) -> bool:
    return (isinstance(x, int) and isinstance(y, np.ndarray) and y.dtype.type == 
    {_J_TYPE_NP_DTYPE_MAP[j_dtype]} and np.nanmean(y) == np.mean( y))
                            """
                    exec(func_str, globals())

                    # for floating point types, DH nulls are auto converted to np.nan
                    # for integer types, DH nulls in the array raise exceptions
                    if j_dtype in ("float", "double"):
                        res = tbl.update("Z = test_udf(X, Y)")
                        self.assertEqual(10, res.to_string().count("true"))
                    else:
                        res = tbl.update("Z = test_udf(X, Y)")
                        self.assertEqual(10, res.to_string().count("true"))

                        # TODO need to wait for https://github.com/deephaven/deephaven-core/issues/5213 to be resolved
                        # with self.assertRaises(DHError) as cm:
                        #     tbl.update("Z = test_udf(X, Y)")
                        # self.assertRegex(str(cm.exception), "Java .* array contains Deephaven null values,
                        # but numpy .* "
                        #                                     "array does not support ")

    def test_np_object_array(self):
        with self.subTest("PyObject"):
            class C:
                pass

            t = empty_table(10).update(["X = i % 3", "Y = C()"])
            self.assertEqual(t.columns[1].data_type, dtypes.PyObject)
            t = t.group_by("X")

            def test_udf(x, y: np.ndarray[C]) -> bool:  # not conversion supported typehint
                import jpy
                return isinstance(y, jpy.JType)

            t1 = t.update("Z = test_udf(X, Y)")
            self.assertEqual(3, t1.to_string().count("true"))

        with self.subTest("JObject"):
            def fn(col) -> List:
                return [col]

            t = empty_table(10).update("X = i % 3").update(f"Y= fn(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)
            t = t.group_by("X")

            def test_udf(x, y: np.ndarray[List]) -> bool:  # not conversion supported typehint
                import jpy
                return isinstance(y, jpy.JType)

            t1 = t.update("Z = test_udf(X, Y)")
            self.assertEqual(3, t1.to_string().count("true"))

    def test_str_bool_datetime_array(self):
        with self.subTest("str"):
            def test_udf(p1: np.ndarray[str], *, p2=None) -> bool:
                return bool(len(p1))

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? `deephaven`: null"]).group_by("X")
            t1 = t.update(["X1 = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            with self.assertRaises(DHError) as cm:
                t2 = t.update(["X1 = test_udf(null, Y )"])
            self.assertRegex(str(cm.exception), "test_udf: Expected .* got null")

            def test_udf(p1: Union[np.ndarray[str], None], p2=None) -> bool:
                return bool(len(p1)) if p1 is not None else False

            t2 = t.update(["X1 = test_udf(null, Y)"])
            self.assertEqual(3, t2.to_string().count("false"))

        with self.subTest("datetime"):
            def test_udf(p1: np.ndarray[np.datetime64], p2=None) -> bool:
                return bool(len(p1))

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? now() : null"]).group_by("X")
            t1 = t.update(["X1 = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            with self.assertRaises(DHError) as cm:
                t2 = t.update(["X1 = test_udf(null, Y )"])
            self.assertRegex(str(cm.exception), "test_udf: Expected .* got null")

            def test_udf(p1: Union[np.ndarray[np.datetime64], None], p2=None) -> bool:
                return bool(len(p1)) if p1 is not None else False

            t2 = t.update(["X1 = test_udf(null, Y)"])
            self.assertEqual(3, t2.to_string().count("false"))

        with self.subTest("boolean"):
            def test_udf(p1: np.ndarray[np.bool_], p2=None) -> bool:
                return bool(len(p1))

            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : null"]).group_by("X")
            t1 = t.update(["X1 = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            # TODO need to wait for https://github.com/deephaven/deephaven-core/issues/5213 to be resolved
            # with self.assertRaises(DHError) as cm:
            #     t1 = t.update(["X1 = test_udf(Y)"])
            # self.assertRegex(str(cm.exception), "Java .* array contains Deephaven null values, but numpy .* "
            #                                     "array does not support ")
            t = empty_table(10).update(["X = i % 3", "Y = i % 2 == 0? true : false"]).group_by("X")
            t1 = t.update(["X1 = test_udf(Y)"])
            self.assertEqual(t1.columns[2].data_type, dtypes.bool_)
            with self.assertRaises(DHError) as cm:
                t2 = t.update(["X1 = test_udf(null, Y )"])
            self.assertRegex(str(cm.exception), "test_udf: Expected .* got null")

            def test_udf(p1: Optional[np.ndarray[bool]], p2=None) -> bool:
                return bool(len(p1)) if p1 is not None else False

            t2 = t.update(["X1 = test_udf(null, Y)"])
            self.assertEqual(3, t2.to_string(cols="X1").count("false"))

    def test_unsupported(self):
        unsupported = {"Sequence", "Sequence[Any]", "List[int]", "Tuple[int]", "Set[int]", "Dict[int, int]",
                       "bytes", "bytearray", "Literal['A', 'B']", "np.ndarray", "np.ndarray[Any]",
                       }
        for th in unsupported:
            with self.subTest(th):
                func_str = f"""
def test_udf(x, y: {th}) -> bool:
    return True
                """
                exec(func_str, globals())

                with self.assertRaises(DHError) as cm:
                    t = empty_table(1).update(["X = i", "Y = ii"]).group_by("X").update(
                        ["Z = test_udf(X, Y.toArray())"])
                self.assertRegex(str(cm.exception), "test_udf: Unsupported type hint")

        with self.subTest("Union with supported type hint"):
            for th in unsupported:
                with self.subTest(th):
                    func_str = f"""
def test_udf(x, y: Union[{th}, np.ndarray[np.int64]]) -> bool:
    return True
                    """
                    exec(func_str, globals())

                    t = empty_table(1).update(["X = i", "Y = ii"]).group_by("X").update(
                        ["Z = test_udf(X, Y.toArray())"])
                    self.assertEqual(t.columns[2].data_type, dtypes.bool_)


if __name__ == "__main__":
    unittest.main()
