#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides Java compatibility support including convenience functions to create some widely used Java
data structures from corresponding Python ones in order to be able to call Java methods. """

from typing import Any, Callable, Dict, Iterable, List, Sequence, Set, TypeVar, Union

import jpy

from deephaven._wrapper import unwrap, wrap_j_object
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
        r.add(unwrap(v))
    return r


def j_hashmap(d: Dict = None) -> jpy.JType:
    """Creates a Java HashMap from a dict."""
    if d is None:
        return None

    r = jpy.get_type("java.util.HashMap")(len(d))
    for k, v in d.items():
        k = unwrap(k)
        v = unwrap(v)
        r.put(k, v)
    return r


def j_hashset(s: Set = None) -> jpy.JType:
    """Creates a Java HashSet from a set."""
    if s is None:
        return None

    r = jpy.get_type("java.util.HashSet")(len(s))
    for v in s:
        r.add(unwrap(v))
    return r


def j_properties(d: Dict = None) -> jpy.JType:
    """Creates a Java Properties from a dict."""
    if d is None:
        return None
    r = jpy.get_type("java.util.Properties")()
    for k, v in d.items():
        k = unwrap(k)
        v = unwrap(v)
        r.setProperty(k, v)
    return r


def j_map_to_dict(m) -> Dict[Any, Any]:
    """Converts a java map to a python dictionary."""
    if not m:
        return {}

    return {e.getKey(): wrap_j_object(e.getValue()) for e in m.entrySet().toArray()}


def j_list_to_list(jlist) -> List[Any]:
    """Converts a java list to a python list."""
    if not jlist:
        return []

    return [wrap_j_object(jlist.get(i)) for i in range(jlist.size())]


T = TypeVar("T")
R = TypeVar("R")


def j_runnable(callable: Callable[[], None]) -> jpy.JType:
    """Constructs a Java 'Runnable' implementation from a Python callable that doesn't take any arguments and returns
    None.

    Args:
        callable (Callable[[], None]): a Python callable that doesn't take any arguments and returns None

    Returns:
        io.deephaven.integrations.python.PythonRunnable instance
    """
    return jpy.get_type("io.deephaven.integrations.python.PythonRunnable")(callable)


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


def j_lambda(func: Callable, lambda_jtype: jpy.JType, return_dtype: DType = None):
    """Wraps a Python Callable as a Java "lambda" type.  
    
    Java lambda types must contain a single abstract method.
    
    Args:
        func (Callable): Any Python Callable or object with an 'apply' method that accepts the same arguments 
            (number and type) the target Java lambda type
        lambda_jtype (jpy.JType): The Java lambda interface to wrap the provided callable in
        return_dtype (DType): The expected return type if conversion should be applied.  None (the default) does not
            attempt to convert the return value and returns a Java Object.
    """
    coerce_to_type = return_dtype.qst_type.clazz() if return_dtype is not None else None
    return jpy.get_type('io.deephaven.integrations.python.JavaLambdaFactory').create(lambda_jtype.jclass, func,
                                                                                     coerce_to_type)


def to_sequence(v: Union[T, Sequence[T]] = None, wrapped: bool = False) -> Sequence[Union[T, jpy.JType]]:
    """A convenience function to create a sequence of wrapped or unwrapped object from either one or a sequence of
    input values to help JPY find the matching Java overloaded method to call.

    This also enables a function to provide parameters that can accept both singular and plural values of the same type
    for the convenience of the users, e.g. both x= "abc" and x = ["abc"] are valid arguments.

    Args:
        v (Union[T, Sequence[T]], optional): the input value(s) to be converted to a sequence
        wrapped (bool, optional): if True, the input value(s) will remain wrapped in a JPy object; otherwise, the input
            value(s) will be unwrapped. Defaults to False.

    Returns:
        Sequence[Union[T, jpy.JType]]: a sequence of wrapped or unwrapped objects
    """
    if v is None or isinstance(v, Sequence) and not v:
        return ()
    if wrapped:
        if not isinstance(v, Sequence) or isinstance(v, str):
            return (v, )
        else:
            return tuple(v)

    if not isinstance(v, Sequence) or isinstance(v, str):
        return (unwrap(v), )
    else:
        return tuple((unwrap(o) for o in v))
