#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module defines the data types supported by the Deephaven engine.

Each data type is represented by a DType class which supports creating arrays of the same type and more.
"""
from __future__ import annotations

from typing import Iterable, Any, Sequence, Callable, Dict, Type, Set

import jpy
import numpy as np

from deephaven2 import DHError

_JQstType = jpy.get_type("io.deephaven.qst.type.Type")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


def _qst_custom_type(cls_name: str):
    return _JQstType.find(_JTableTools.typeFromName(cls_name))


class DType:
    """ A class representing a data type in Deephaven."""

    _j_name_type_map: Dict[str, DType] = {}

    @classmethod
    def from_jtype(cls, j_class: Any) -> DType:
        """ look up a DType that matches the java type, if not found, create a DType for it. """
        if not j_class:
            return None

        j_name = j_class.getName()
        dtype = DType._j_name_type_map.get(j_name)
        if not dtype:
            return cls(j_name=j_name, j_type=j_class, np_type=np.object_)
        else:
            return dtype

    @classmethod
    def from_np_dtype(cls, np_dtype: np.dtype) -> DType:
        """ Look up a DType that matches the numpy.dtype, if not found, return PyObject. """
        for _, dtype in DType._j_name_type_map.items():
            if np.dtype(dtype.np_type) == np_dtype and dtype.np_type != np.object_:
                return dtype

        return PyObject

    def __init__(self, j_name: str, j_type: Type = None, qst_type: jpy.JType = None, is_primitive: bool = False,
                 np_type=None, factory: Callable = None):
        """
        Args:
             j_name (str): the full qualified name of the Java class
             j_type (Type): the mapped Python class created by JPY
             qst_type (JType): the JPY wrapped object for a instance of QST Type
             is_primitive (bool): whether this instance represents a primitive Java type
             np_type: an instance of numpy dtype (dtype("int64")or numpy class (e.g. np.int16)
             factory: a callable that returns an instance of the wrapped Java class
        """
        self.j_name = j_name
        self.j_type = j_type if j_type else jpy.get_type(j_name)
        self.qst_type = qst_type if qst_type else _qst_custom_type(j_name)
        self.is_primitive = is_primitive
        self.np_type = np_type
        self._factory = factory

        DType._j_name_type_map[j_name] = self

    def __repr__(self):
        return self.j_name

    def __call__(self, *args, **kwargs):
        try:
            if self._factory:
                return self._factory(*args, **kwargs)
            return self.j_type(*args, **kwargs)
        except Exception as e:
            raise DHError(e, f"failed to create an instance of {self.j_name}") from e

    def array(self, size: int):
        """ Creates a Java array of the same data type of the specified size.

        Args:
            size (int): the size of the array

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            return jpy.array(self.j_name, size)
        except Exception as e:
            raise DHError("failed to create a Java array.") from e

    def array_from(self, seq: Sequence, remap: Callable[[Any], Any] = None):
        """ Creates a Java array of the same data type populated with values from a sequence.

        Note:
            this method does unsafe casting, meaning precision and values might be lost with down cast

        Args:
            seq: a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.
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

            return jpy.array(self.j_name, seq)
        except Exception as e:
            raise DHError(e, f"failed to create a Java {self.j_name} array.") from e


class CharDType(DType):
    """ The class for char type. """

    def __init__(self):
        super().__init__(j_name="char", qst_type=_JQstType.charType(), is_primitive=True, np_type=np.dtype('uint16'))

    def array_from(self, seq: Sequence, remap: Callable[[Any], Any] = None):
        if isinstance(seq, str) and not remap:
            return super().array_from(seq, remap=ord)
        return super().array_from(seq, remap)


bool_ = DType(j_name="java.lang.Boolean", qst_type=_JQstType.booleanType(), np_type=np.bool_)
byte = DType(j_name="byte", qst_type=_JQstType.byteType(), is_primitive=True, np_type=np.int8)
int8 = byte
short = DType(j_name="short", qst_type=_JQstType.shortType(), is_primitive=True, np_type=np.int16)
int16 = short
char = CharDType()
int32 = DType(j_name="int", qst_type=_JQstType.intType(), is_primitive=True, np_type=np.int32)
long = DType(j_name="long", qst_type=_JQstType.longType(), is_primitive=True, np_type=np.int64)
int64 = long
int_ = int32
float_ = DType(j_name="float", qst_type=_JQstType.floatType(), is_primitive=True, np_type=np.float32)
single = float_
float32 = float_
double = DType(j_name="double", qst_type=_JQstType.doubleType(), is_primitive=True, np_type=np.float64)
float64 = double
string = DType(j_name="java.lang.String", qst_type=_JQstType.stringType(), np_type=np.object_)
BigDecimal = DType(j_name="java.math.BigDecimal", np_type=np.object_)
StringSet = DType(j_name="io.deephaven.stringset.StringSet", np_type=np.object_)
DateTime = DType(j_name="io.deephaven.time.DateTime", np_type=np.dtype("datetime64[ns]"))
Period = DType(j_name="io.deephaven.time.Period", np_type=np.object_)
PyObject = DType(j_name="org.jpy.PyObject", np_type=np.object_)
JObject = DType(j_name="java.lang.Object", np_type=np.object_)


def _j_array_list(values: Iterable):
    if values is None:
        return None
    r = jpy.get_type("java.util.ArrayList")(len(list(values)))
    for v in values:
        r.add(v)
    return r


def _j_hashmap(d: Dict):
    if d is None:
        return None

    r = jpy.get_type("java.util.HashMap")()
    for key, value in d.items():
        if value is None:
            value = ''
        r.put(key, value)
    return r


def _j_hashset(s: Set):
    if s is None:
        return None

    r = jpy.get_type("java.util.HashSet")()
    for v in s:
        r.add(v)
    return r


def _j_properties(d: Dict):
    if d is None:
        return None
    r = jpy.get_type("java.util.Properties")()
    for key, value in d.items():
        if value is None:
            value = ''
        r.setProperty(key, value)
    return r


HashSet = DType(j_name="java.util.HashSet", np_type=np.object_, factory=_j_hashset)
HashMap = DType(j_name="java.util.HashMap", np_type=np.object_, factory=_j_hashmap)
ArrayList = DType(j_name="java.util.ArrayList", np_type=np.object_, factory=_j_array_list)
Properties = DType(j_name="java.util.Properties", np_type=np.object_, factory=_j_properties)


def is_java_type(obj: Any) -> bool:
    """ Returns True if the object is originated in Java. """
    return isinstance(obj, jpy.JType)
