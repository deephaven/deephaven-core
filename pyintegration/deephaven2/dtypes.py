#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module defines the data types supported by the Deephaven engine.

Each data type is represented by a DType class which supports creating arrays of the same type and more.
"""
from __future__ import annotations

from typing import Iterable, Any, Tuple, Sequence, Callable

import numpy as np
import pandas as pd
import jpy

from deephaven2 import DHError
from deephaven2.constants import *

_JQstType = jpy.get_type("io.deephaven.qst.type.Type")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


def _qst_custom_type(cls_name: str):
    return _JQstType.find(_JTableTools.typeFromName(cls_name))


class DType:
    """ A class representing a data type in Deephaven."""
    _j_name_map = {}

    @classmethod
    def from_jtype(cls, j_class: Any) -> DType:
        if not j_class:
            return None

        j_name = j_class.getName()
        dtype = DType._j_name_map.get(j_name)
        if not dtype:
            return cls(j_name=j_name, j_type=j_class)
        else:
            return dtype

    def __init__(self, j_name: str, j_type: Any = None, qst_type: Any = None, is_primitive: bool = False):
        self.j_name = j_name
        self.j_type = j_type if j_type else jpy.get_type(j_name)
        self.qst_type = qst_type if qst_type else _qst_custom_type(j_name)
        self.is_primitive = is_primitive

        DType._j_name_map[j_name] = self

    def __repr__(self):
        return self.j_name

    def __call__(self, *args, **kwargs):
        return self.j_type(*args, **kwargs)

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
        super().__init__(j_name="char", qst_type=_JQstType.charType(), is_primitive=True)

    def array_from(self, seq: Sequence, remap: Callable[[Any], Any] = None):
        if isinstance(seq, str) and not remap:
            return super().array_from(seq, remap=ord)
        return super().array_from(seq, remap)


# region predefined types and aliases
bool_ = DType(j_name="java.lang.Boolean", qst_type=_JQstType.booleanType())
byte = DType(j_name="byte", qst_type=_JQstType.byteType(), is_primitive=True)
int8 = byte
short = DType(j_name="short", qst_type=_JQstType.shortType(), is_primitive=True)
int16 = short
char = CharDType()
int_ = DType(j_name="int", qst_type=_JQstType.intType(), is_primitive=True)
int32 = int_
long = DType(j_name="long", qst_type=_JQstType.longType(), is_primitive=True)
int64 = long
float_ = DType(j_name="float", qst_type=_JQstType.floatType(), is_primitive=True)
single = float_
float32 = float_
double = DType(j_name="double", qst_type=_JQstType.doubleType(), is_primitive=True)
float64 = double
string = DType(j_name="java.lang.String", qst_type=_JQstType.stringType())
BigDecimal = DType(j_name="java.math.BigDecimal")
StringSet = DType(j_name="io.deephaven.stringset.StringSet")
DateTime = DType(j_name="io.deephaven.time.DateTime")
Period = DType(j_name="io.deephaven.time.Period")
PyObject = DType(j_name="org.jpy.PyObject")
JObject = DType(j_name="java.lang.Object")


# endregion

def j_array_list(values: Iterable):
    j_list = jpy.get_type("java.util.ArrayList")(len(values))
    try:
        for v in values:
            j_list.add(v)
        return j_list
    except Exception as e:
        raise DHError(e, "failed to create a Java ArrayList from the Python collection.") from e
