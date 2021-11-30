#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module defines the data types supported by the Deephaven engine.

Each data type is represented by a DType class which supports creating arrays of the same type and more.
"""
from enum import Enum
from typing import Iterable

import jpy
from deephaven2 import DHError

_JQstType = jpy.get_type("io.deephaven.qst.type.Type")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


def _qst_custom_type(cls_name: str):
    return _JQstType.find(_JTableTools.typeFromName(cls_name))


class DType(Enum):
    """ An Enum for supported data types in Deephaven with type aliases to mirror the same ones in numpy or pyarrow.

    The complex types such as BigDecimal, Period can be called to create Java objects of the same types, e.g.
        big_decimal = BigDecimal(12.88)

    """
    bool_ = _JQstType.booleanType(), "java.lang.Boolean"
    byte = _JQstType.byteType(), "byte"
    int8 = byte
    short = _JQstType.shortType(), "short"
    int16 = short
    char = _JQstType.charType(), "char"
    int_ = _JQstType.intType(), "int"
    int32 = int_
    long = _JQstType.longType(), "long"
    int64 = long
    float_ = _JQstType.floatType(), "float"
    single = float_
    float32 = float_
    double = _JQstType.doubleType(), "double"
    float64 = double
    string = _JQstType.stringType(), "java.lang.String"
    BigDecimal = _qst_custom_type("java.math.BigDecimal"), "java.math.BigDecimal"
    StringSet = _qst_custom_type("io.deephaven.stringset.StringSet"), "io.deephaven.stringset.StringSet"
    DateTime = _qst_custom_type("io.deephaven.time.DateTime"), "io.deephaven.time.DateTime"
    Period = _qst_custom_type("io.deephaven.time.Period"), "io.deephaven.time.Period"

    def __new__(cls, qst_type, j_type):
        obj = object.__new__(cls)
        obj._value_ = qst_type
        return obj

    def __init__(self, qst_type, j_name):
        self._qst_type = qst_type
        self._j_name = j_name
        self._j_type = jpy.get_type(j_name)

    def __call__(self, *args, **kwargs):
        return self._j_type(*args, **kwargs)

    @property
    def qst_type(self):
        return self._qst_type

    @property
    def j_type(self):
        """ The corresponding Java type. """
        return self._j_type

    def array(self, size: int):
        """ Create a Java array of the same data type of the specified size.

        Args:
            size (int): the size of the array

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            return jpy.array(self._j_name, size)
        except Exception as e:
            raise DHError("failed to create a Java array.") from e

    def array_from(self, values: Iterable):
        """ Create a Java array of the same data type populated with values from a Python iterable.

        Args:
            values: a Python iterable of compatible data type

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            return jpy.array(self._j_name, values)
        except Exception as e:
            raise DHError("failed to create a Java array.") from e


bool_ = DType.bool_
byte = DType.byte
int8 = DType.int8
short = DType.short
int16 = DType.int16
char = DType.char
int_ = DType.int_
int32 = DType.int32
long = DType.long
int64 = DType.int64
float_ = DType.float_
single = DType.single
float32 = DType.float32
double = DType.double
float64 = DType.float64
string = DType.string
BigDecimal = DType.BigDecimal
StringSet = DType.StringSet
DateTime = DType.DateTime
Period = DType.Period
