#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
import datetime
import typing
import unittest
from typing import List, Union, Tuple, Sequence, Optional

import numba as nb
import numpy as np
import numpy.typing as npt
import pandas as pd

from deephaven import empty_table, dtypes, DHError
from tests.testbase import BaseTestCase

_J_TYPE_NP_DTYPE_MAP = {
    dtypes.double: "np.float64",
    dtypes.float32: "np.float32",
    dtypes.int32: "np.int32",
    dtypes.long: "np.int64",
    dtypes.short: "np.int16",
    dtypes.byte: "np.int8",
    dtypes.bool_: "np.bool_",
    dtypes.string: "np.str_",
    dtypes.char: "np.uint16",
}


class PyFuncReturnJavaTestCase(BaseTestCase):
    def test_scalar_return(self):
        for dh_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
            with self.subTest(dh_dtype=dh_dtype, np_dtype=np_dtype):
                func_str = f"""
def fn(col) -> {np_dtype}:
    return {np_dtype}(col)
"""
                exec(func_str, globals())

                t = empty_table(10).update("X = i").update(f"Y= fn(X + 1)")
                self.assertEqual(t.columns[1].data_type, dh_dtype)

    def test_array_return(self):
        component_types = {
            "int": dtypes.long_array,
            "float": dtypes.double_array,
            "np.int8": dtypes.byte_array,
            "np.int16": dtypes.short_array,
            "np.int32": dtypes.int32_array,
            "np.int64": dtypes.long_array,
            "np.float32": dtypes.float32_array,
            "np.float64": dtypes.double_array,
            "bool": dtypes.boolean_array,
            "np.str_": dtypes.string_array,
            "np.uint16": dtypes.char_array,
        }
        container_types = ["List", "Tuple", "list", "tuple", "Sequence", "np.ndarray"]
        for component_type, dh_dtype in component_types.items():
            for container_type in container_types:
                with self.subTest(component_type=component_type, container_type=container_type):
                    func_decl_str = f"""def fn(col) -> {container_type}[{component_type}]:"""
                    if container_type == "np.ndarray":
                        func_body_str = f"""    return np.array([{component_type}(c) for c in col])"""
                    else:
                        func_body_str = f"""    return [{component_type}(c) for c in col]"""
                    exec("\n".join([func_decl_str, func_body_str]), globals())
                    t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X").update(f"Z= fn(Y + 1)")
                    self.assertEqual(t.columns[2].data_type, dh_dtype)

        container_types = ["bytes", "bytearray"]
        for container_type in container_types:
            with self.subTest(container_type=container_type):
                func_decl_str = f"""def fn(col) -> {container_type}:"""
                func_body_str = f"""    return {container_type}(col)"""
                exec("\n".join([func_decl_str, func_body_str]), globals())
                t = empty_table(10).update(["X = i % 3", "Y = i"]).group_by("X").update(f"Z= fn(Y + 1)")
                self.assertEqual(t.columns[2].data_type, dtypes.byte_array)

    def test_scalar_return_class_method_not_supported(self):
        for dh_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
            with self.subTest(dh_dtype=dh_dtype, np_dtype=np_dtype):
                func_str = f"""
class Foo:
    def fn(self, col) -> {np_dtype}:
        return {np_dtype}(col)
foo = Foo()
"""
                exec(func_str, globals())

                t = empty_table(10).update("X = i").update(f"Y= foo.fn(X + 1)")
                self.assertNotEqual(t.columns[1].data_type, dh_dtype)

    def test_datetime_scalar_return(self):
        dt_dtypes = [
            "np.dtype('datetime64[ns]')",
            "np.dtype('datetime64[ms]')",
            "datetime.datetime",
            "pd.Timestamp"
        ]

        for np_dtype in dt_dtypes:
            with self.subTest(np_dtype=np_dtype):
                func_decl_str = f"""def fn(col) -> {np_dtype}:"""
                if np_dtype == "np.dtype('datetime64[ns]')":
                    func_body_str = f"""    return pd.Timestamp(col).to_numpy()"""
                elif np_dtype == "datetime.datetime":
                    func_body_str = f"""    return pd.Timestamp(col).to_pydatetime()"""
                elif np_dtype == "pd.Timestamp":
                    func_body_str = f"""    return pd.Timestamp(col)"""

                exec("\n".join([func_decl_str, func_body_str]), globals())

                t = empty_table(10).update("X = i").update(f"Y= fn(X + 1)")
                self.assertEqual(t.columns[1].data_type, dtypes.Instant)
                # vectorized
                t = empty_table(10).update("X = i").update(f"Y= fn(X)")
                self.assertEqual(t.columns[1].data_type, dtypes.Instant)

    def test_datetime_array_return(self):

        dt = datetime.datetime.now()
        ts = pd.Timestamp(dt)
        np_dt = np.datetime64(dt)
        dt_list = [ts, np_dt, dt]

        # test if we can convert to numpy datetime64 array
        np_array = np.array([pd.Timestamp(dt).to_numpy() for dt in dt_list], dtype=np.datetime64)

        dt_dtypes = [
            "np.ndarray[np.dtype('datetime64[ns]')]",
            "List[datetime.datetime]",
            "Tuple[pd.Timestamp]"
        ]

        dt_data = [
            "dt_list",
            "np_array",
        ]

        # we are capable of mapping all datetime arrays (Sequence, np.ndarray) to instant arrays, so even if the actual
        # return value of the function doesn't match its type hint (which will be caught by a static type checker),
        # as long as it is a valid datetime collection, we can still convert it to instant array
        for np_dtype in dt_dtypes:
            for data in dt_data:
                with self.subTest(np_dtype=np_dtype, data=data):
                    func_decl_str = f"""def fn(col) -> {np_dtype}:"""
                    func_body_str = f"""    return {data}"""
                    exec("\n".join([func_decl_str, func_body_str]), globals().update(
                        {"dt_list": dt_list, "np_array": np_array}))

                    t = empty_table(10).update("X = i").update(f"Y= fn(X + 1)")
                    self.assertEqual(t.columns[1].data_type, dtypes.instant_array)

    def test_return_value_errors(self):
        def fn(col) -> List[object]:
            return [col]

        def fn1(col) -> List:
            return [col]

        def fn2(col):
            return col

        def fn3(col) -> List[Union[datetime.datetime, int]]:
            return [col]

        with self.subTest(fn):
            t = empty_table(1).update("X = i").update(f"Y= fn(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)

        with self.subTest(fn1):
            t = empty_table(1).update("X = i").update(f"Y= fn1(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)

        with self.subTest(fn2):
            t = empty_table(1).update("X = i").update(f"Y= fn2(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)

        with self.subTest(fn3):
            t = empty_table(1).update("X = i").update(f"Y= fn3(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)

        with self.subTest("wrong typehint bool vs. np.bool_"):
            def fn4(col: int) -> bool:
                return np.bool_(col)

            with self.assertRaises(DHError) as cm:
                 t = empty_table(2).update("X = i").update(f"Y= fn4(X + 1)")
            self.assertIn("class org.jpy.PyObject cannot be cast to class java.lang.Boolean", str(cm.exception))

    def test_vectorization_off_on_return_type(self):
        def f1(x) -> List[str]:
            return ["a"]

        t = empty_table(10).update("X = f1(3 + i)")
        self.assertEqual(t.columns[0].data_type, dtypes.string_array)

        t = empty_table(10).update("X = f1(i)")
        self.assertEqual(t.columns[0].data_type, dtypes.string_array)

        t = empty_table(10).update(["A=i%2", "B=i"]).group_by("A")

        # Testing https://github.com/deephaven/deephaven-core/issues/4557
        def f4557_1(x, y) -> np.ndarray[np.int64]:
            # np.array is still needed as of v0.29
            return np.array(x) + y

        # Testing https://github.com/deephaven/deephaven-core/issues/4562
        @nb.guvectorize([(nb.int32[:], nb.int32, nb.int32[:])], "(m),()->(m)", nopython=True)
        def f4562_1(x, y, res):
            res[:] = x + y

        t2 = t.update([
            "X = f4557_1(B,3)",
            "Y = f4562_1(B,3)"
        ])
        self.assertEqual(t2.columns[2].data_type, dtypes.long_array)
        self.assertEqual(t2.columns[3].data_type, dtypes.int32_array)

        t3 = t2.ungroup()
        self.assertEqual(t3.columns[2].data_type, dtypes.int64)
        self.assertEqual(t3.columns[3].data_type, dtypes.int32)

    def test_ndim_nparray_return_type(self):
        def f() -> np.ndarray[np.int64]:
            return np.ndarray([1, 2], dtype=np.int64)

        with self.assertRaises(DHError) as cm:
            t = empty_table(10).update(["X1 = f()"])
        self.assertIn("not support multi-dimensional arrays", str(cm.exception))

    def test_npt_NDArray_return_type(self):
        def f() -> npt.NDArray[np.int64]:
            return np.array([1, 2], dtype=np.int64)

        t = empty_table(10).update(["X1 = f()"])
        self.assertEqual(t.columns[0].data_type, dtypes.long_array)

    def test_ndarray_weird_cases(self):
        def f() -> np.ndarray[typing.Any]:
            return np.array([1, 2], dtype=np.int64)
        t = empty_table(10).update(["X1 = f()"])
        self.assertEqual(t.columns[0].data_type, dtypes.PyObject)

        def f1() -> npt.NDArray[typing.Any]:
            return np.array([1, 2], dtype=np.int64)
        t = empty_table(10).update(["X1 = f1()"])
        self.assertEqual(t.columns[0].data_type, dtypes.PyObject)

        def f2() -> np.ndarray[typing.Any, np.int64]:
            return np.array([1, 2], dtype=np.int64)
        t = empty_table(10).update(["X1 = f2()"])
        self.assertEqual(t.columns[0].data_type, dtypes.PyObject)

        def f3() -> Union[None, None]:
            return np.array([1, 2], dtype=np.int64)
        t = empty_table(10).update(["X1 = f3()"])
        self.assertEqual(t.columns[0].data_type, dtypes.PyObject)

        def f4() -> None:
            return np.array([1, 2], dtype=np.int64)
        t = empty_table(10).update(["X1 = f4()"])
        self.assertEqual(t.columns[0].data_type, dtypes.PyObject)

    def test_optional_scalar_return(self):
        for dh_dtype, np_dtype in _J_TYPE_NP_DTYPE_MAP.items():
            with self.subTest(dh_dtype=dh_dtype, np_dtype=np_dtype):
                func_str = f"""
def fn(col) -> Optional[{np_dtype}]:
    return None if col % 2 == 0 else {np_dtype}(col)
"""
                exec(func_str, globals())

                t = empty_table(10).update("X = i").update(f"Y= fn(X + 1)")
                self.assertEqual(t.columns[1].data_type, dh_dtype)
                self.assertEqual(t.to_string().count("null"), 5)

    def test_optional_array_return(self):
        def f() -> Optional[np.ndarray[np.int64]]:
            return np.array([1, 2], dtype=np.int64)

        t = empty_table(10).update(["X1 = f()"])
        self.assertEqual(t.columns[0].data_type, dtypes.long_array)

        def f1(col) -> Optional[List[int]]:
            return None if col % 2 == 0 else [col]

        t = empty_table(10).update(["X1 = f1(i)"])
        self.assertEqual(t.columns[0].data_type, dtypes.long_array)
        self.assertEqual(t.to_string().count("null"), 5)

    def test_np_ufunc(self):
        # no vectorization and no type inference
        npsin = np.sin
        t = empty_table(10).update(["X1 = npsin(i)"])
        self.assertEqual(t.columns[0].data_type, dtypes.PyObject)
        t2 = t.update("X2 = X1.getDoubleValue()")
        self.assertEqual(t2.columns[1].data_type, dtypes.double)

        import numba

        # numba vectorize decorator doesn't support numpy ufunc
        with self.assertRaises(TypeError):
            nbsin = numba.vectorize([numba.float64(numba.float64)])(np.sin)

        # this is the workaround that utilizes vectorization and type inference
        @numba.vectorize([numba.float64(numba.int64)], nopython=True)
        def nbsin(x):
            return np.sin(x)
        t3 = empty_table(10).update(["X3 = nbsin(i)"])
        self.assertEqual(t3.columns[0].data_type, dtypes.double)

    def test_java_instant_return(self):
        from deephaven.time import to_j_instant

        t = empty_table(10).update(["X1 = to_j_instant(`2021-01-01T00:00:00Z`)"])
        self.assertEqual(t.columns[0].data_type, dtypes.Instant)

        def udf() -> List[dtypes.Instant]:
            return [to_j_instant("2021-01-01T00:00:00Z")]
        t = empty_table(10).update(["X1 = udf()"])
        self.assertEqual(t.columns[0].data_type, dtypes.instant_array)


if __name__ == '__main__':
    unittest.main()
