#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""Tests for PEP 695 ``TypeAliasType`` (the ``type X = ...`` statement, Python 3.12+) handling in Python UDFs.

These exercise ``deephaven._udf._unwrap_type_alias`` / ``_resolve_origin`` and their use in the parameter and
return-annotation parsing, including chained aliases and pathological self-referential/mutually-recursive aliases.
The ``type`` statement is a 3.12+ parser feature, so the alias definitions live in ``exec``'d strings and the whole
suite is skipped on older interpreters.
"""

import sys
import unittest

from deephaven import DHError, dtypes, empty_table
from tests.testbase import BaseTestCase


@unittest.skipIf(
    sys.version_info < (3, 12), "PEP 695 type alias statement requires Python 3.12+"
)
class UdfTypeAliasTest(BaseTestCase):
    def test_scalar_alias_param(self):
        # A bare alias of a scalar type should be parsed exactly like the underlying type.
        exec("type MyInt = int", globals())
        func_str = """
def test_udf(x: MyInt) -> bool:
    return isinstance(x, int)
"""
        exec(func_str, globals())
        t = empty_table(10).update("X = i").update("Y = test_udf(X)")
        self.assertEqual(10, t.to_string().count("true"))

    def test_scalar_alias_return(self):
        # A bare alias used as a return annotation should resolve to the underlying DH dtype.
        exec("type MyFloat = np.float64", globals())
        func_str = """
def fn(col) -> MyFloat:
    return np.float64(col)
"""
        exec(func_str, globals())
        t = empty_table(10).update("X = i").update("Y = fn(X + 1)")
        self.assertEqual(t.columns[1].data_type, dtypes.double)

    def test_array_alias_param(self):
        # An alias whose origin unwraps to np.ndarray exercises _resolve_origin / component-type detection.
        exec("type MyArr = np.ndarray[np.int64]", globals())
        func_str = """
def test_udf(x, y: MyArr) -> bool:
    return isinstance(y, np.ndarray) and y.dtype.type == np.int64
"""
        exec(func_str, globals())
        t = (
            empty_table(100)
            .update(["X = i % 10", "Y = (long)i"])
            .group_by("X")
            .update("Z = test_udf(X, Y)")
        )
        self.assertEqual(10, t.to_string().count("true"))

    def test_array_alias_return(self):
        # An alias of a Sequence type as the return annotation should yield the right DH array dtype.
        exec("type MyList = typing.List[np.int64]", globals())
        func_str = """
def fn(col) -> MyList:
    return [np.int64(c) for c in col]
"""
        exec(func_str, globals())
        t = (
            empty_table(10)
            .update(["X = i % 3", "Y = i"])
            .group_by("X")
            .update("Z = fn(Y + 1)")
        )
        self.assertEqual(t.columns[2].data_type, dtypes.long_array)

    def test_chained_alias(self):
        # Chained aliases (A -> B -> C) must be fully unwrapped, behaving identically to the root type.
        exec("type A = np.int64\ntype B = A\ntype C = B", globals())
        func_str = """
def test_udf(x: C) -> C:
    return np.int64(x)
"""
        exec(func_str, globals())
        t = empty_table(10).update("X = i").update("Y = test_udf(X + 1)")
        self.assertEqual(t.columns[1].data_type, dtypes.long)

    def test_alias_in_optional_union(self):
        # An alias nested inside Optional/Union must be unwrapped to the non-None type on both the return and
        # parameter paths.
        exec("type MyInt = np.int64", globals())
        exec("type MyArr = np.ndarray[np.int64]", globals())

        # return path: Optional[MyInt] -> non-None branch resolves to np.int64 -> long column.
        ret_func = """
def fn(col) -> typing.Optional[MyInt]:
    return np.int64(col)
"""
        exec(ret_func, globals())
        t = empty_table(10).update("X = i").update("Y = fn(X + 1)")
        self.assertEqual(t.columns[1].data_type, dtypes.long)

        # parameter path: the array alias inside the Union must be unwrapped so the arg is delivered as an
        # np.ndarray (a failed unwrap would fall back to object and pass the raw Java array instead).
        param_func = """
def test_udf(x, y: typing.Optional[MyArr]) -> bool:
    return isinstance(y, np.ndarray) and y.dtype == np.int64
"""
        exec(param_func, globals())
        t = (
            empty_table(100)
            .update(["X = i % 10", "Y = (long)i"])
            .group_by("X")
            .update("Z = test_udf(X, Y)")
        )
        self.assertEqual(10, t.to_string().count("true"))

    def test_pathological_self_referential_alias(self):
        # A self-referential alias (type X = X) would loop forever without the cycle guard. Reaching the assertion at
        # all proves termination; as a return annotation the alias falls back to an unsupported type -> PyObject.
        exec("type X = X", globals())
        func_str = """
def fn(col) -> X:
    return col
"""
        exec(func_str, globals())
        t = empty_table(10).update("X = i").update("Y = fn(X)")
        self.assertEqual(t.columns[1].data_type, dtypes.PyObject)

        # parameter path: a cyclic alias must also terminate and be reported as unsupported rather than hang.
        param_func = """
def fn2(col: X) -> bool:
    return True
"""
        exec(param_func, globals())
        with self.assertRaises(DHError) as cm:
            empty_table(10).update("X = i").update("Y = fn2(X)")
        self.assertIn("Unsupported type hint", str(cm.exception))

    def test_pathological_mutually_recursive_aliases(self):
        # Mutually recursive aliases (A -> B -> A) must also terminate via the cycle guard.
        exec("type A = B\ntype B = A", globals())
        func_str = """
def fn(col) -> A:
    return col
"""
        exec(func_str, globals())
        t = empty_table(10).update("X = i").update("Y = fn(X)")
        self.assertEqual(t.columns[1].data_type, dtypes.PyObject)


if __name__ == "__main__":
    unittest.main()
