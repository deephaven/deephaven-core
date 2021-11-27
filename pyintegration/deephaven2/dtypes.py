#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module defines the data types supported by the Deephaven engine.

Each data type is represented by a DType class which supports creating arrays of the same type and more.
"""
from enum import Enum
from typing import Iterable, Any, List, Tuple, Sequence

import numpy as np
import pandas as pd
import jpy
from deephaven2 import DHError

_JQstType = jpy.get_type("io.deephaven.qst.type.Type")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")

# region Deephaven Special Null values for primitive types
# contains appropriate seq for bidirectional conversion of null seq
NULL_CHAR = 65535  #: Null value for char.
NULL_FLOAT = float.fromhex('-0x1.fffffep127')  #: Null value for float.
NULL_DOUBLE = float.fromhex('-0x1.fffffffffffffP+1023')  #: Null value for double.
NULL_SHORT = -32768  #: Null value for short.
NULL_INT = -0x80000000  #: Null value for int.
NULL_LONG = -0x8000000000000000  #: Null value for long.
NULL_BYTE = -128  #: Null value for byte.


def _qst_custom_type(cls_name: str):
    return _JQstType.find(_JTableTools.typeFromName(cls_name))


class DType:
    """ A class represents a data type in Deephaven."""

    def __init__(self, j_name, qst_type=None, is_primitive=False):
        self.qst_type = qst_type
        self.j_name = j_name
        self.j_type = jpy.get_type(j_name)
        if not qst_type:
            self.qst_type = _qst_custom_type(j_name)
        self.is_primitive = is_primitive

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

    def array_of(self, seq: Sequence):
        """ Creates a Java array of the same data type populated with values from a Python sequence.

        Args:
            seq: a Python sequence of compatible data type

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            return jpy.array(self.j_name, seq)
        except Exception as e:
            raise DHError("failed to create a Java array.") from e


class IntegerDType(DType):
    def __init__(self, qst_type, j_name, np_dtypes: Tuple = (), null_value=None):
        super().__init__(qst_type=qst_type, j_name=j_name, is_primitive=True)
        self.np_types = np_dtypes
        self.null_value = null_value

    def array_of(self, seq: Sequence):
        """ Creates a Java array of the same integer type populated with values from a Python sequence.

        Args:
            seq: a Python sequence of compatible data type, can be a np array or Pandas series

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            if isinstance(seq, np.ndarray):
                if seq.dtype in self.np_types:
                    return super().array_of(seq)
                elif np.issubdtype(seq.dtype, np.integer):
                    return super().array_of(seq.astype(self.np_types[0]))
                elif np.issubdtype(seq.dtype, np.floating):
                    nan_arr = np.isnan(seq)
                    arr = seq.astype(self.np_types[0])
                    arr[nan_arr] = self.null_value
                    return super().array_of(arr)
                else:
                    raise ValueError(f"Incompatible np dtype ({seq.dtype}) for {self.j_name} array")
            elif isinstance(seq, pd.Series):
                return self.array_of(seq.values)
            else:
                return super().array_of(seq)
        except Exception as e:
            raise DHError(e, f"failed to create a Java {self.j_name} array.") from e


class FloatingDType(DType):
    def __init__(self, qst_type, j_name, np_dtypes: Tuple = (), null_value=None):
        super().__init__(qst_type=qst_type, j_name=j_name, is_primitive=True)
        self.np_types = np_dtypes
        self.null_value = null_value

    def array_of(self, seq: Sequence):
        """ Creates a Java array of the same floating type populated with values from a Python sequence.

        Args:
            seq: a Python sequence of compatible data type, can be a np array or Pandas series

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            if isinstance(seq, np.ndarray):
                if seq.dtype in self.np_types:
                    nan_arr = np.isnan(seq)
                    arr = seq.copy()
                    arr[nan_arr] = self.null_value
                    return super().array_of(arr)
                elif np.issubdtype(seq.dtype, np.floating):
                    nan_arr = np.isnan(seq)
                    arr = seq.astype(self.np_types[0])
                    arr[nan_arr] = self.null_value
                    return super().array_of(arr)
                elif np.issubdtype(seq.dtype, np.integer):
                    return super().array_of(seq.astype(self.np_types[0]))
                else:
                    raise ValueError(f"Incompatible np dtype ({seq.dtype}) for {self.j_name} array")
            elif isinstance(seq, pd.Series):
                return self.array_of(seq.values)
            else:
                return super().array_of(seq)
        except Exception as e:
            raise DHError(e, f"failed to create a Java {self.j_name} array.") from e


class CharDType(DType):
    def __init__(self, qst_type, j_name, np_dtypes: Tuple = (), null_value=None):
        super().__init__(qst_type=qst_type, j_name=j_name, is_primitive=True)
        self.np_types = np_dtypes
        self.null_value = null_value

    def array_of(self, seq):
        """ Creates a Java char array populated with values from a Python sequence.

        Args:
            seq: a Python sequence of compatible data type, can be a np array or Pandas series

        Returns:
            a Java array

        Raises:
            DHError
        """

        def to_char(el):
            if el is None:
                return NULL_CHAR
            if isinstance(el, int):
                return el
            if isinstance(el, str):
                if len(el) < 1:
                    return NULL_CHAR
                return ord(el[0])
            try:
                return int(el)
            except ValueError:
                return NULL_CHAR

        try:
            if isinstance(seq, str):
                return super().array_of([ord(c) for c in seq])
            elif isinstance(seq, np.ndarray):
                if seq.dtype == np.uint16:
                    return super().array_of(seq)
                elif np.issubdtype(seq.dtype, np.integer):
                    return super().array_of(seq.astype(np.uint16))
                elif seq.dtype == np.dtype('U1') and seq.dtype.name in ['unicode32', 'str32', 'string32',
                                                                        'bytes32']:
                    arr = np.copy(seq)
                    arr.dtype = np.uint32
                    return super().array_of(arr.astype(np.uint16))
                elif seq.dtype == np.dtype('S1') and seq.dtype.name in ['str8', 'string8', 'bytes8']:
                    arr = np.copy(seq)
                    arr.dtype = np.uint8
                    return super().array_of(arr.astype(np.uint16))
                elif seq.dtype == np.object:
                    return super().array_of(np.array([to_char(el) for el in seq], dtype=np.uint16))
                else:
                    # do our best
                    raise ValueError(
                        f"Passed in a numpy array, expect integer dtype or one char string dtype, and got {seq.dtype}")
            elif isinstance(seq, pd.Series):
                return self.array_of(seq.values)
            else:
                return self.array_of(np.asarray(seq))
        except Exception as e:
            raise DHError(e, f"failed to create a Java {self.j_name} array.") from e


# region convenience enum and module level type aliases
class DTypes(Enum):
    """ An enum that defines a predefined set of supported data types in Deephaven. """

    bool_ = DType(qst_type=_JQstType.booleanType(), j_name="java.lang.Boolean")
    byte = IntegerDType(qst_type=_JQstType.byteType(), j_name="byte", np_dtypes=(np.int8, np.uint8),
                        null_value=NULL_BYTE)
    int8 = byte
    short = IntegerDType(qst_type=_JQstType.shortType(), j_name="short", np_dtypes=(np.int16, np.uint16),
                         null_value=NULL_SHORT)
    int16 = short
    char = CharDType(qst_type=_JQstType.charType(), j_name="char", null_value=NULL_CHAR)
    int_ = IntegerDType(qst_type=_JQstType.intType(), j_name="int", np_dtypes=(np.int32, np.uint32),
                        null_value=NULL_INT)
    int32 = int_
    long = IntegerDType(qst_type=_JQstType.longType(), j_name="long", np_dtypes=(np.int64, np.uint64),
                        null_value=NULL_LONG)
    int64 = long
    float_ = FloatingDType(qst_type=_JQstType.floatType(), j_name="float", np_dtypes=(np.float32,),
                           null_value=NULL_FLOAT)
    single = float_
    float32 = float_
    double = FloatingDType(qst_type=_JQstType.doubleType(), j_name="double", np_dtypes=(np.float64,),
                           null_value=NULL_DOUBLE)
    float64 = double
    string = DType(qst_type=_JQstType.stringType(), j_name="java.lang.String")
    BigDecimal = DType(j_name="java.math.BigDecimal")
    StringSet = DType(j_name="io.deephaven.stringset.StringSet")
    DateTime = DType(j_name="io.deephaven.time.DateTime")
    Period = DType(j_name="io.deephaven.time.Period")
    PyObject = DType(j_name="org.jpy.PyObject")


bool_ = DTypes.bool_.value
byte = DTypes.byte.value
int8 = DTypes.int8.value
short = DTypes.short.value
int16 = DTypes.int16.value
char = DTypes.char.value
int_ = DTypes.int_.value
int32 = DTypes.int32.value
long = DTypes.long.value
int64 = DTypes.int64.value
float_ = DTypes.float_.value
single = DTypes.single.value
float32 = DTypes.float32.value
double = DTypes.double.value
float64 = DTypes.float64.value
string = DTypes.string.value
BigDecimal = DTypes.BigDecimal.value
StringSet = DTypes.StringSet.value
DateTime = DTypes.DateTime.value
Period = DTypes.Period.value
PyObject = DTypes.PyObject.value


# endregion

# region helper functions

def j_class_lookup(j_class: Any) -> DType:
    if not j_class:
        return None

    j_type = jpy.get_type(j_class.getName())
    for t in DTypes:
        if t.value.j_type == j_type:
            return t.value


def j_array_list(values: Iterable):
    j_list = jpy.get_type("java.util.ArrayList")(len(values))
    try:
        for v in values:
            j_list.add(v)
        return j_list
    except Exception as e:
        raise DHError(e, "failed to create a Java ArrayList from the Python collection.") from e

# endregion
