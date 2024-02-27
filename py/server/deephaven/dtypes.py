#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines the data types supported by the Deephaven engine.

Each data type is represented by a DType class which supports creating arrays of the same type and more.
"""
from __future__ import annotations

import datetime
import sys
import typing
from typing import Any, Sequence, Callable, Dict, Type, Union, _GenericAlias, Optional

import jpy
import numpy as np
import numpy._typing as npt
import pandas as pd

from deephaven import DHError
from deephaven.constants import NULL_BYTE, NULL_SHORT, NULL_INT, NULL_LONG, NULL_FLOAT, NULL_DOUBLE, NULL_CHAR

_JQstType = jpy.get_type("io.deephaven.qst.type.Type")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")
_JPrimitiveArrayConversionUtility = jpy.get_type("io.deephaven.integrations.common.PrimitiveArrayConversionUtility")

_j_name_type_map: Dict[str, DType] = {}


def _qst_custom_type(cls_name: str):
    try:
        return _JQstType.find(_JTableTools.typeFromName(cls_name))
    except:
        return None


class DType:
    """ A class representing a data type in Deephaven."""

    def __init__(self, j_name: str, j_type: Type = None, qst_type: jpy.JType = None, is_primitive: bool = False,
                 np_type: Any = np.object_):
        """
        Args:
             j_name (str): the full qualified name of the Java class
             j_type (Type): the mapped Python class created by JPY
             qst_type (JType): the JPY wrapped object for a instance of QST Type
             is_primitive (bool): whether this instance represents a primitive Java type
             np_type (Any): an instance of numpy dtype (dtype("int64") or numpy class (e.g. np.int16), default is
                np.object_
        """
        self.j_name = j_name
        self.j_type = j_type if j_type else jpy.get_type(j_name)
        self.qst_type = qst_type if qst_type else _qst_custom_type(j_name)
        self.is_primitive = is_primitive
        self.np_type = np_type

        _j_name_type_map[j_name] = self

    def __repr__(self):
        return self.j_name

    def __call__(self, *args, **kwargs):
        if self.is_primitive:
            raise DHError(message=f"primitive type {self.j_name} is not callable.")

        try:
            return self.j_type(*args, **kwargs)
        except Exception as e:
            raise DHError(e, f"failed to create an instance of {self.j_name}") from e


bool_ = DType(j_name="java.lang.Boolean", qst_type=_JQstType.booleanType(), np_type=np.bool_)
"""Boolean type"""
byte = DType(j_name="byte", qst_type=_JQstType.byteType(), is_primitive=True, np_type=np.int8)
"""Signed byte integer type"""
int8 = byte
"""Signed byte integer type"""
short = DType(j_name="short", qst_type=_JQstType.shortType(), is_primitive=True, np_type=np.int16)
"""Signed short integer type"""
int16 = short
"""Signed short integer type"""
char = DType(j_name="char", qst_type=_JQstType.charType(), is_primitive=True, np_type=np.dtype('uint16'))
"""Character type"""
int32 = DType(j_name="int", qst_type=_JQstType.intType(), is_primitive=True, np_type=np.int32)
"""Signed 32bit integer type"""
long = DType(j_name="long", qst_type=_JQstType.longType(), is_primitive=True, np_type=np.int64)
"""Signed 64bit integer type"""
int64 = long
"""Signed 64bit integer type"""
int_ = long
"""Signed 64bit integer type"""
float32 = DType(j_name="float", qst_type=_JQstType.floatType(), is_primitive=True, np_type=np.float32)
"""Single-precision floating-point number type"""
single = float32
"""Single-precision floating-point number type"""
float64 = DType(j_name="double", qst_type=_JQstType.doubleType(), is_primitive=True, np_type=np.float64)
"""Double-precision floating-point number type"""
double = float64
"""Double-precision floating-point number type"""
float_ = float64
"""Double-precision floating-point number type"""
string = DType(j_name="java.lang.String", qst_type=_JQstType.stringType(), np_type=np.str_)
"""String type"""
Character = DType(j_name="java.lang.Character")
"""Character type"""
BigDecimal = DType(j_name="java.math.BigDecimal")
"""Java BigDecimal type"""
StringSet = DType(j_name="io.deephaven.stringset.StringSet")
"""Deephaven StringSet type"""
Instant = DType(j_name="java.time.Instant", np_type=np.dtype("datetime64[ns]"))
"""Instant date time type"""
LocalDate = DType(j_name="java.time.LocalDate")
"""Local date type"""
LocalTime = DType(j_name="java.time.LocalTime")
"""Local time type"""
ZonedDateTime = DType(j_name="java.time.ZonedDateTime")
"""Zoned date time type"""
Duration = DType(j_name="java.time.Duration")
"""Time period type, which is a unit of time in terms of clock time (24-hour days, hours, minutes, seconds, and nanoseconds)."""
Period = DType(j_name="java.time.Period")
"""Time period type, which is a unit of time in terms of calendar time (days, weeks, months, years, etc.)."""
TimeZone = DType(j_name="java.time.ZoneId")
"""Time zone type."""
BusinessCalendar = DType(j_name='io.deephaven.time.calendar.BusinessCalendar')
"""Business calendar type"""
PyObject = DType(j_name="org.jpy.PyObject")
"""Python object type"""
JObject = DType(j_name="java.lang.Object")
"""Java Object type"""
bool_array = DType(j_name='[Z')
"""boolean array type"""
byte_array = DType(j_name='[B')
"""Byte array type"""
int8_array = byte_array
"""Byte array type"""
short_array = DType(j_name='[S')
"""Short array type"""
int16_array = short_array
"""Short array type"""
char_array = DType(j_name='[C')
"""char array type"""
int32_array = DType(j_name='[I')
"""32bit integer array type"""
long_array = DType(j_name='[J')
"""64bit integer array type"""
int64_array = long_array
"""64bit integer array type"""
int_array = long_array
"""64bit integer array type"""
single_array = DType(j_name='[F')
"""Single-precision floating-point array type"""
float32_array = single_array
"""Single-precision floating-point array type"""
double_array = DType(j_name='[D')
"""Double-precision floating-point array type"""
float64_array = double_array
"""Double-precision floating-point array type"""
float_array = double_array
"""Double-precision floating-point array type"""
string_array = DType(j_name='[Ljava.lang.String;')
"""Java String array type"""
boolean_array = DType(j_name='[Ljava.lang.Boolean;')
"""Java Boolean array type"""
instant_array = DType(j_name='[Ljava.time.Instant;')
"""Java Instant array type"""
zdt_array = DType(j_name='[Ljava.time.ZonedDateTime;')
"""Zoned date time array type"""

_PRIMITIVE_DTYPE_NULL_MAP = {
    bool_: NULL_BYTE,
    byte: NULL_BYTE,
    char: NULL_CHAR,
    int16: NULL_SHORT,
    int32: NULL_INT,
    int64: NULL_LONG,
    float32: NULL_FLOAT,
    float64: NULL_DOUBLE,
}

_BUILDABLE_ARRAY_DTYPE_MAP = {
    bool_: bool_array,
    byte: int8_array,
    char: char_array,
    int16: int16_array,
    int32: int32_array,
    int64: int64_array,
    float32: float32_array,
    float64: float64_array,
    string: string_array,
    Instant: instant_array,
}


_J_ARRAY_NP_TYPE_MAP = {
    boolean_array.j_type: np.dtype("?"),
    byte_array.j_type: np.dtype("b"),
    char_array.j_type: np.dtype("uint16"),
    short_array.j_type: np.dtype("h"),
    int32_array.j_type: np.dtype("i"),
    long_array.j_type: np.dtype("l"),
    float32_array.j_type: np.dtype("f"),
    double_array.j_type: np.dtype("d"),
    string_array.j_type: np.dtype("U"),
    instant_array.j_type: np.dtype("datetime64[ns]"),
}


def null_remap(dtype: DType) -> Callable[[Any], Any]:
    """ Creates a null value remap function for the provided DType.

    Args:
        dtype (DType): the DType instance

    Returns:
        a Callable

    Raises:
        TypeError
    """
    null_value = _PRIMITIVE_DTYPE_NULL_MAP.get(dtype)
    if null_value is None:
        raise TypeError("null_remap() must be called with a primitive DType")

    return lambda v: null_value if v is None else v


def _instant_array(data: Sequence) -> jpy.JType:
    """Converts a sequence of either datetime64[ns], datetime.datetime, pandas.Timestamp, datetime strings,
    or integers in nanoseconds, to a Java array of Instant values. """
    # try to convert to numpy array of datetime64 if not already, so that we can call translateArrayLongToInstant on
    # it to reduce the number of round trips to the JVM
    if not isinstance(data, np.ndarray):
        try:
            data = np.array([pd.Timestamp(dt).to_numpy() for dt in data], dtype=np.datetime64)
        except Exception as e:
            ...

    if isinstance(data, np.ndarray) and data.dtype.kind in ('M', 'i', 'U'):
        if data.dtype.kind == 'M':
            longs = jpy.array('long', data.astype('datetime64[ns]').astype('int64'))
        elif data.dtype.kind == 'i':
            longs = jpy.array('long', data.astype('int64'))
        else:  # data.dtype.kind == 'U'
            longs = jpy.array('long', [pd.Timestamp(str(dt)).to_numpy().astype('int64') for dt in data])
        data = _JPrimitiveArrayConversionUtility.translateArrayLongToInstant(longs)
        return data

    if not isinstance(data, instant_array.j_type):
        from deephaven.time import to_j_instant
        data = [to_j_instant(d) for d in data]

    return jpy.array(Instant.j_type, data)


def array(dtype: DType, seq: Optional[Sequence], remap: Callable[[Any], Any] = None) -> Optional[jpy.JType]:
    """ Creates a Java array of the specified data type populated with values from a sequence.

    Note:
        this method does unsafe casting, meaning precision and values might be lost with down cast

    Args:
        dtype (DType): the component type of the array
        seq (Sequence): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.
        remap (optional): a callable that takes one value and maps it to another, for handling the translation of
            special DH values such as NULL_INT, NAN_INT between Python and the DH engine

    Returns:
        a Java array

    Raises:
        DHError
    """
    if seq is None:
        return None

    if isinstance(seq, np.ndarray) and seq.ndim > 1:
        raise ValueError("array() does not support multi-dimensional arrays")

    if not isinstance(dtype, DType):
        raise TypeError(f"array() expects a DType for the first argument but given a {type(dtype).__name__}")

    try:
        if isinstance(seq, str) and dtype == char:
            # ord is the Python builtin function that takes a unicode character and returns an integer code point value
            remap = ord

        if remap:
            if not callable(remap):
                raise ValueError("Not a callable")
            seq = [remap(v) for v in seq]

        if dtype == Instant:
            return _instant_array(seq)

        if isinstance(seq, np.ndarray):
            if dtype == bool_:
                bytes_ = seq.astype(dtype=np.int8)
                j_bytes = array(byte, bytes_)
                seq = _JPrimitiveArrayConversionUtility.translateArrayByteToBoolean(j_bytes)

        return jpy.array(dtype.j_type, seq)
    except Exception as e:
        raise DHError(e, f"failed to create a Java {dtype.j_name} array.") from e


def from_jtype(j_class: Any) -> DType:
    """ looks up a DType that matches the java type, if not found, creates a DType for it. """
    if not j_class:
        return None

    j_name = j_class.getName()
    dtype = _j_name_type_map.get(j_name)
    if not dtype:
        return DType(j_name=j_name, j_type=j_class, np_type=np.object_)
    else:
        return dtype


def from_np_dtype(np_dtype: Union[np.dtype, pd.api.extensions.ExtensionDtype]) -> DType:
    """ Looks up a DType that matches the provided numpy dtype or Pandas's nullable equivalent; if not found,
    returns PyObject. """

    if isinstance(np_dtype, pd.api.extensions.ExtensionDtype):
        # check if it is a Pandas nullable numeric types such as pd.Float64Dtype/Int32Dtype/BooleanDtype etc.
        if hasattr(np_dtype, "numpy_dtype"):
            np_dtype = np_dtype.numpy_dtype
        elif isinstance(np_dtype, pd.StringDtype):
            return string
        else:
            return PyObject

    if np_dtype.kind in {'U', 'S'}:
        return string

    if np_dtype.kind in {'M'}:
        return Instant

    for _, dtype in _j_name_type_map.items():
        if np.dtype(dtype.np_type) == np_dtype and dtype.np_type != np.object_:
            return dtype

    return PyObject


_NUMPY_INT_TYPE_CODES = {"b", "h", "H", "i", "l"}
_NUMPY_FLOATING_TYPE_CODES = {"f", "d"}


def _is_py_null(x: Any) -> bool:
    """Checks if the value is a Python null value, i.e. None or NaN, or Pandas.NA."""
    if x is None:
        return True

    try:
        return bool(pd.isna(x))
    except (TypeError, ValueError):
        return False


def _scalar(x: Any, dtype: DType) -> Any:
    """Converts a Python value to a Java scalar value. It converts the numpy primitive types, string to
    their Python equivalents so that JPY can handle them. For datetime values, it converts them to Java Instant.
    Otherwise, it returns the value as is."""

    # NULL_BOOL will appear in Java as a byte value which causes a cast error. We just let JPY converts it to Java null
    # and the engine has casting logic to handle it.
    if (dt := _PRIMITIVE_DTYPE_NULL_MAP.get(dtype)) and _is_py_null(x) and dtype not in (bool_, char):
        return dt

    try:
        if hasattr(x, "dtype"):
            if x.dtype.char == 'H':  # np.uint16 maps to Java char
                return Character(int(x))
            elif x.dtype.char in _NUMPY_INT_TYPE_CODES:
                return int(x)
            elif x.dtype.char in _NUMPY_FLOATING_TYPE_CODES:
                return float(x)
            elif x.dtype.char == '?':
                return bool(x)
            elif x.dtype.char == 'U':
                return str(x)
            elif x.dtype.char == 'O':
                return x
            elif x.dtype.char == 'M':
                from deephaven.time import to_j_instant
                return to_j_instant(x)
        elif isinstance(x, (datetime.datetime, pd.Timestamp)):
                from deephaven.time import to_j_instant
                return to_j_instant(x)
        return x
    except:
        return x


def _np_dtype_char(t: Union[type, str]) -> str:
    """Returns the numpy dtype character code for the given type."""
    try:
        np_dtype = np.dtype(t if t else "object")
        if np_dtype.kind == "O":
            if t in (datetime.datetime, pd.Timestamp):
                return "M"
    except TypeError:
        np_dtype = np.dtype("object")

    return np_dtype.char


def _component_np_dtype_char(t: type) -> Optional[str]:
    """Returns the numpy dtype character code for the given type's component type if the type is a Sequence type or
    numpy ndarray, otherwise return None. """
    component_type = None
    if isinstance(t, _GenericAlias) and issubclass(t.__origin__, Sequence):
        component_type = t.__args__[0]
        # if the component type is a DType, get its numpy type
        if isinstance(component_type, DType):
            component_type = component_type.np_type

    if not component_type:
        component_type = _np_ndarray_component_type(t)

    if component_type:
        return _np_dtype_char(component_type)
    else:
        return None


def _np_ndarray_component_type(t: type) -> Optional[type]:
    """Returns the numpy ndarray component type if the type is a numpy ndarray, otherwise return None."""

    # Py3.8: npt.NDArray can be used in Py 3.8 as a generic alias, but a specific alias (e.g. npt.NDArray[np.int64])
    # is an instance of a private class of np, yet we don't have a choice but to use it. And when npt.NDArray is used,
    # the 1st argument is typing.Any, the 2nd argument is another generic alias of which the 1st argument is the
    # component type
    component_type = None
    if sys.version_info.major == 3 and sys.version_info.minor == 8:
        if isinstance(t, np._typing._generic_alias._GenericAlias) and t.__origin__ == np.ndarray:
            component_type = t.__args__[1].__args__[0]
    # Py3.9+, np.ndarray as a generic alias is only supported in Python 3.9+, also npt.NDArray is still available but a
    # specific alias (e.g. npt.NDArray[np.int64]) now is an instance of typing.GenericAlias.
    # when npt.NDArray is used, the 1st argument is typing.Any, the 2nd argument is another generic alias of which
    # the 1st argument is the component type
    # when np.ndarray is used, the 1st argument is the component type
    if not component_type and sys.version_info.major == 3 and sys.version_info.minor > 8:
        import types
        if isinstance(t, types.GenericAlias) and (issubclass(t.__origin__, Sequence) or t.__origin__ == np.ndarray):
            nargs = len(t.__args__)
            if nargs == 1:
                component_type = t.__args__[0]
            elif nargs == 2:  # for npt.NDArray[np.int64], etc.
                a0 = t.__args__[0]
                a1 = t.__args__[1]
                if a0 == typing.Any and isinstance(a1, types.GenericAlias):
                    component_type = a1.__args__[0]
    return component_type
