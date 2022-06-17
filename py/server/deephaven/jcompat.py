#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides Java compatibility support including convenience functions to create some widely used Java
data structures from corresponding Python ones in order to be able to call Java methods. """

from typing import Any, Iterable, Dict, Set, TypeVar, Callable, Union, Sequence

import jpy

from deephaven._wrapper import unwrap
from deephaven.dtypes import DType


def is_java_type(obj: Any) -> bool:
    """Returns True if the object is originated in Java."""
    return isinstance(obj, jpy.JType)


def j_array_list(values: Iterable = None) -> jpy.JType:
    """Creates a Java ArrayList instance from an iterable."""
    if values is None:
        return None
    r = jpy.get_type("java.util.ArrayList")(len(list(values)))
    for v in values:
        r.add(v)
    return r


def j_hashmap(d: Dict = None) -> jpy.JType:
    """Creates a Java HashMap from a dict."""
    if d is None:
        return None

    r = jpy.get_type("java.util.HashMap")()
    for key, value in d.items():
        if value is None:
            value = ""
        r.put(key, value)
    return r


def j_hashset(s: Set = None) -> jpy.JType:
    """Creates a Java HashSet from a set."""
    if s is None:
        return None

    r = jpy.get_type("java.util.HashSet")()
    for v in s:
        r.add(v)
    return r


def j_properties(d: Dict = None) -> jpy.JType:
    """Creates a Java Properties from a dict."""
    if d is None:
        return None
    r = jpy.get_type("java.util.Properties")()
    for key, value in d.items():
        if value is None:
            value = ""
        r.setProperty(key, value)
    return r


def j_map_to_dict(m):
    """Converts a java map to a python dictionary."""
    if not m:
        return None

    r = {}
    for e in m.entrySet().toArray():
        k = e.getKey()
        v = e.getValue()
        r[k] = v

    return r


T = TypeVar("T")
R = TypeVar("R")


def j_function(func: Callable[[T], R], dtype: DType) -> jpy.JType:
    """Constructs a Java 'Function<PyObject, Object>' implementation from a Python callable or an object with an
     'apply' method that accepts a single argument.

    Args:
        func (Callable): a Python callable or an object with an 'apply' method that accepts a single argument
        dtype (DType): the return type of 'func'

    Returns:
        io.deephaven.integrations.python.PythonFunction instance
    """
    return jpy.get_type("io.deephaven.integrations.python.PythonFunction")(
        func, dtype.qst_type.clazz()
    )


def j_unary_operator(func: Callable[[T], T], dtype: DType) -> jpy.JType:
    """Constructs a Java 'Function<PyObject, Object>' implementation from a Python callable or an object with an
     'apply' method that accepts a single argument.

    Args:
        func (Callable): a Python callable or an object with an 'apply' method that accepts a single argument
        dtype (DType): the return type of 'func'

    Returns:
        io.deephaven.integrations.python.PythonFunction instance
    """
    return jpy.get_type("io.deephaven.integrations.python.PythonFunction$PythonUnaryOperator")(
        func, dtype.qst_type.clazz()
    )


def j_binary_operator(func: Callable[[T, T], T], dtype: DType) -> jpy.JType:
    """Constructs a Java 'Function<PyObject, PyObject, Object>' implementation from a Python callable or an object with an
     'apply' method that accepts a single argument.

    Args:
        func (Callable): a Python callable or an object with an 'apply' method that accepts two arguments
        dtype (DType): the return type of 'func'

    Returns:
        io.deephaven.integrations.python.PythonFunction instance
    """
    return jpy.get_type("io.deephaven.integrations.python.PythonBiFunction$PythonBinaryOperator")(
        func, dtype.qst_type.clazz()
    )


def to_sequence(v: Union[T, Sequence[T]] = None) -> Sequence[Union[T, jpy.JType]]:
    """A convenience function to create a sequence of unwrapped object from either one or a sequence of input values to
    help JPY find the matching Java overloaded method to call.

    This also enables a function to provide parameters that can accept both singular and plural values of the same type
    for the convenience of the users, e.g. both x= "abc" and x = ["abc"] are valid arguments.
    """
    if not v:
        return ()
    if not isinstance(v, Sequence) or isinstance(v, str):
        return (unwrap(v),)
    else:
        return tuple((unwrap(o) for o in v))
