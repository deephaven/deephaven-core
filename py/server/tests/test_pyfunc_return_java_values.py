#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
import time
import unittest
from typing import List, Tuple, Sequence

import numpy as np
import pandas as pd
import datetime
from deephaven import empty_table, dtypes
from deephaven import time as dhtime
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
    # dtypes.char: "np.uint16",
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
            # "np.uint16": dtypes.char_array,
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

        for np_dtype in dt_dtypes:
            for data in dt_data:
                with self.subTest(np_dtype=np_dtype, data=data):
                    func_decl_str = f"""def fn(col) -> {np_dtype}:"""
                    # func_body_str_1 = f"""    nonlocal {data}"""
                    func_body_str_1 = """"""
                    func_body_str_2 = f"""    return {data}"""
                    exec("\n".join([func_decl_str, func_body_str_1, func_body_str_2]), globals().update(
                        {"dt_list": dt_list, "np_array": np_array}));

                    t = empty_table(10).update("X = i").update(f"Y= fn(X + 1)")
                    self.assertEqual(t.columns[1].data_type, dtypes.instant_array)

    def test_return_value_errors(self):
        def fn(col) -> List[object]:
            return [col]

        def fn1(col) -> List:
            return [col]

        def fn2(col):
            return col

        with self.subTest(fn):
            t = empty_table(1).update("X = i").update(f"Y= fn(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)

        with self.subTest(fn1):
            t = empty_table(1).update("X = i").update(f"Y= fn1(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)

        with self.subTest(fn2):
            t = empty_table(1).update("X = i").update(f"Y= fn2(X + 1)")
            self.assertEqual(t.columns[1].data_type, dtypes.JObject)


if __name__ == '__main__':
    unittest.main()
