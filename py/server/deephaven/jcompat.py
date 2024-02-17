#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides Java compatibility support including convenience functions to create some widely used Java
data structures from corresponding Python ones in order to be able to call Java methods. """

from typing import Any, Callable, Dict, Iterable, List, Sequence, Set, TypeVar, Union, Tuple, Literal

import jpy
import numpy as np
import pandas as pd

from deephaven import dtypes, DHError
from deephaven._wrapper import unwrap, wrap_j_object
from deephaven.dtypes import DType, _PRIMITIVE_DTYPE_NULL_MAP, _J_ARRAY_NP_TYPE_MAP

_NULL_BOOLEAN_AS_BYTE = jpy.get_type("io.deephaven.util.BooleanUtils").NULL_BOOLEAN_AS_BYTE
_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")

_DH_PANDAS_NULLABLE_TYPE_MAP: Dict[DType, pd.api.extensions.ExtensionDtype] = {
    dtypes.bool_: pd.BooleanDtype,
    dtypes.byte: pd.Int8Dtype,
    dtypes.short: pd.Int16Dtype,
    dtypes.char: pd.UInt16Dtype,
    dtypes.int32: pd.Int32Dtype,
    dtypes.int64: pd.Int64Dtype,
    dtypes.float32: pd.Float32Dtype,
    dtypes.float64: pd.Float64Dtype,
}


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
            return (v,)
        else:
            return tuple(v)

    if not isinstance(v, Sequence) or isinstance(v, str):
        return (unwrap(v),)
    else:
        return tuple((unwrap(o) for o in v))


def _j_array_to_numpy_array(dtype: DType, j_array: jpy.JType, conv_null: bool, type_promotion: bool = False) -> \
        np.ndarray:
    """ Produces a numpy array from the DType and given Java array.

    Args:
        dtype (DType): The dtype of the Java array
        j_array (jpy.JType): The Java array to convert
        conv_null (bool): If True, convert nulls to the null value for the dtype
        type_promotion (bool): Ignored when conv_null is False.  When type_promotion is False, (1) input Java integer,
            boolean, or character arrays containing Deephaven nulls yield an exception, (2) input Java float or double
            arrays containing Deephaven nulls have null values converted to np.nan, and (3) input Java arrays without
            Deephaven nulls are converted to the target type.  When type_promotion is True, (1) input Java integer,
            boolean, or character arrays containing Deephaven nulls are converted to np.float64 arrays and Deephaven
            null values are converted to np.nan, (2) input Java float or double arrays containing Deephaven nulls have
            null values converted to np.nan, and (3) input Java arrays without Deephaven nulls are converted to the
            target type.  Defaults to False.

    Returns:
        np.ndarray: The numpy array

    Raises:
        DHError
    """
    if dtype.is_primitive:
        np_array = np.frombuffer(j_array, dtype.np_type)
    elif dtype == dtypes.Instant:
        longs = _JPrimitiveArrayConversionUtility.translateArrayInstantToLong(j_array)
        np_long_array = np.frombuffer(longs, np.int64)
        np_array = np_long_array.view(dtype.np_type)
    elif dtype == dtypes.bool_:
        # dh nulls will be preserved and show up as True b/c the underlying byte array isn't modified
        bytes_ = _JPrimitiveArrayConversionUtility.translateArrayBooleanToByte(j_array)
        np_array = np.frombuffer(bytes_, dtype.np_type)
    elif dtype == dtypes.string:
        np_array = np.array(j_array, dtypes.string.np_type)
    elif dtype.np_type is not np.object_:
        try:
            np_array = np.frombuffer(j_array, dtype.np_type)
        except:
            np_array = np.array(j_array, np.object_)
    else:
        np_array = np.array(j_array, np.object_)

    if conv_null:
        if dh_null := _PRIMITIVE_DTYPE_NULL_MAP.get(dtype):
            if dtype in (dtypes.float32, dtypes.float64):
                np_array = np.copy(np_array)
                np_array[np_array == dh_null] = np.nan
            else:
                if dtype is dtypes.bool_:  # needs to change its type to byte for dh null detection
                    np_array = np.frombuffer(np_array, np.byte)

                if any(np_array[np_array == dh_null]):
                    if not type_promotion:
                        raise DHError(f"Problem creating numpy array.  Java {dtype} array contains Deephaven null values, but numpy {np_array.dtype} array does not support null values")
                    np_array = np_array.astype(np.float64)
                    np_array[np_array == dh_null] = np.nan
                else:
                    if dtype is dtypes.bool_:  # needs to change its type back to bool
                        np_array = np.frombuffer(np_array, np.bool_)
                    return np_array

    return np_array


def _j_array_to_series(dtype: DType, j_array: jpy.JType, conv_null: bool) -> pd.Series:
    """Produce a copy of the specified Java array as a pandas.Series object.

    Args:
        dtype (DType): the dtype of the Java array
        j_array (jpy.JType): the Java array
        conv_null (bool): whether to check for Deephaven nulls in the data and automatically replace them with
            pd.NA.

    Returns:
        a pandas Series

    Raises:
        DHError
    """
    if conv_null and dtype == dtypes.bool_:
        j_array = _JPrimitiveArrayConversionUtility.translateArrayBooleanToByte(j_array)
        np_array = np.frombuffer(j_array, dtype=np.byte)
        s = pd.Series(data=np_array, dtype=pd.Int8Dtype(), copy=False)
        s.mask(s == _NULL_BOOLEAN_AS_BYTE, inplace=True)
        return s.astype(pd.BooleanDtype(), copy=False)

    np_array = _j_array_to_numpy_array(dtype, j_array, conv_null=False)
    if conv_null and (nv := _PRIMITIVE_DTYPE_NULL_MAP.get(dtype)) is not None:
        pd_ex_dtype = _DH_PANDAS_NULLABLE_TYPE_MAP.get(dtype)
        s = pd.Series(data=np_array, dtype=pd_ex_dtype(), copy=False)
        s.mask(s == nv, inplace=True)
    else:
        s = pd.Series(data=np_array, copy=False)

    return s
