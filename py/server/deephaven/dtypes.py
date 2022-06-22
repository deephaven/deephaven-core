#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines the data types supported by the Deephaven engine.

Each data type is represented by a DType class which supports creating arrays of the same type and more.
"""
from __future__ import annotations

from typing import Any, Sequence, Callable, Dict, Type, Union

import jpy
import numpy as np
import pandas as pd

from deephaven import DHError

_JQstType = jpy.get_type("io.deephaven.qst.type.Type")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")

_j_name_type_map: Dict[str, DType] = {}


def _qst_custom_type(cls_name: str):
    return _JQstType.find(_JTableTools.typeFromName(cls_name))


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
string = DType(j_name="java.lang.String", qst_type=_JQstType.stringType())
"""String type"""
BigDecimal = DType(j_name="java.math.BigDecimal")
"""Java BigDecimal type"""
StringSet = DType(j_name="io.deephaven.stringset.StringSet")
"""Deephaven StringSet type"""
DateTime = DType(j_name="io.deephaven.time.DateTime", np_type=np.dtype("datetime64[ns]"))
"""Deephaven DateTime type"""
Period = DType(j_name="io.deephaven.time.Period")
"""Deephaven time period type"""
PyObject = DType(j_name="org.jpy.PyObject")
"""Python object type"""
JObject = DType(j_name="java.lang.Object")
"""Java Object type"""
byte_array = DType(j_name='[B')
"""Byte array type"""
int8_array = byte_array
"""Byte array type"""
short_array = DType(j_name='[S')
"""Short array type"""
int16_array = short_array
"""Short array type"""
int32_array = DType(j_name='[I')
"""32bit integer array type"""
long_array = DType(j_name='[J')
"""64bit integer array type"""
int64_array = long_array
"""64bit integer array type"""
int_array = long_array
"""64bit integer array type"""
single_array = DType(j_name='[S')
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
datetime_array = DType(j_name='[Lio.deephaven.time.DateTime;')
"""Deephaven DateTime array type"""


def array(dtype: DType, seq: Sequence, remap: Callable[[Any], Any] = None) -> jpy.JType:
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
    try:
        if remap:
            if not callable(remap):
                raise ValueError("Not a callable")
            seq = [remap(v) for v in seq]
        else:
            if isinstance(seq, str) and dtype == char:
                return array(char, seq, remap=ord)

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

    for _, dtype in _j_name_type_map.items():
        if np.dtype(dtype.np_type) == np_dtype and dtype.np_type != np.object_:
            return dtype

    return PyObject
