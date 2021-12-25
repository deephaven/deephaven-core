#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module defines the data types supported by the Deephaven engine.

Each data type is represented by a DType class which supports creating arrays of the same type and more.
"""
from __future__ import annotations

from typing import Iterable, Any, Tuple, Sequence

import numpy as np
import pandas as pd
import jpy

from deephaven2 import DHError
from deephaven2.constants import *

_JQstType = jpy.get_type("io.deephaven.qst.type.Type")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


def _qst_custom_type(cls_name: str):
    return _JQstType.find(_JTableTools.typeFromName(cls_name))


def _handle_nan_and_null(seq, dtype, nan_to_value, null_to_value):
    if nan_to_value is None and null_to_value is None:
        return seq

    lst = list(seq)
    for i, v in enumerate(seq):
        if nan_to_value is not None:
            if v != v:
                lst[i] = nan_to_value

        if null_to_value is not None:
            if v == dtype.null_value:
                lst[i] = null_to_value
    return lst


def _handle_nan(seq, nan_to_value):
    if nan_to_value is None:
        return seq

    if isinstance(seq, np.ndarray):
        seq[np.isnan(seq)] = nan_to_value
        return seq

    lst = list(seq)
    for i, v in enumerate(seq):
        if v != v:
            lst[i] = nan_to_value
    return lst


def _handle_null(seq, null_value, null_to_value):
    if null_to_value is None:
        return seq

    lst = list(seq)
    for i, v in enumerate(seq):
        if v == null_value:
            lst[i] = null_to_value
    return lst


class DType:
    """ A class represents a data type in Deephaven."""
    _NAME_DTYPE_MAP = {}

    @classmethod
    def from_jtype(cls, j_class: Any) -> DType:
        if not j_class:
            return None

        j_name = j_class.getName()
        dtype = DType._NAME_DTYPE_MAP.get(j_name)
        if not dtype:
            return cls(j_name=j_name, j_type=j_class)
        else:
            return dtype

    def __init__(self, j_name: str, j_type: Any = None, qst_type: Any = None, is_primitive: bool = False):
        self.j_name = j_name
        self.j_type = j_type if j_type else jpy.get_type(j_name)
        self.qst_type = qst_type if qst_type else _qst_custom_type(j_name)
        self.is_primitive = is_primitive

        DType._NAME_DTYPE_MAP[j_name] = self

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

    def array_of(self, seq: Sequence):
        """ Creates a Java array of the same data type populated with values from a sequence.

        Args:
            seq: a sequence of compatible data type

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
    """ The class for all integer types. """

    def __init__(self, j_name: str, qst_type: Any, np_dtypes: Tuple, null_value: int):
        super().__init__(j_name=j_name, qst_type=qst_type, is_primitive=True)
        self.np_types = np_dtypes
        self.null_value = null_value

    def array_of(self, seq: Sequence, nan_to_value: int = None, null_to_value: int = None):
        """ Creates a Java array of the same integer type populated with values from a sequence.

        Note:
            this method does unsafe casting, meaning precision and values might be lost with down cast

        Args:
            seq: a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.
            nan_to_value (float): if not None, convert NaN to the specified value
            null_to_value (float): if not None, convert NULL values in the casted input sequence to the specified value

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            if isinstance(seq, np.ndarray):
                if seq.dtype in self.np_types:
                    seq = seq
                elif np.issubdtype(seq.dtype, np.integer):
                    seq = seq.astype(self.np_types[0])
                elif np.issubdtype(seq.dtype, np.floating):
                    seq = _handle_nan(seq.copy(), nan_to_value)
                    seq = seq.astype(self.np_types[0])
                else:
                    raise ValueError(f"Incompatible np dtype ({seq.dtype}) for {self.j_name} array")
            elif isinstance(seq, pd.Series):
                return self.array_of(seq.values, nan_to_value, null_to_value)

            seq = _handle_null(seq, self.null_value, null_to_value)
            return super().array_of(seq)
        except Exception as e:
            raise DHError(e, f"failed to create a Java {self.j_name} array.") from e


class FloatingDType(DType):
    """ The class for all floating types. """

    def __init__(self, j_name: str, qst_type: Any, np_dtypes: tuple, null_value: float):
        super().__init__(qst_type=qst_type, j_name=j_name, is_primitive=True)
        self.np_types = np_dtypes
        self.null_value = null_value

    def array_of(self, seq: Sequence, nan_to_value: float = None, null_to_value: float = None):
        """ Creates a Java array of the same floating type populated with values from a sequence.

        Note:
            this method does unsafe casting, meaning precision and values might be lost with down cast

        Args:
            seq (Sequence): a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.
            nan_to_value (float): if not None, convert NaN to the specified value
            null_to_value (float): if not None, convert NULL values in the casted input sequence after cast to the
                specified value

        Returns:
            a Java array

        Raises:
            DHError
        """
        try:
            if isinstance(seq, np.ndarray):
                if seq.dtype not in self.np_types:
                    if np.issubdtype(seq.dtype, np.floating):
                        seq = seq.astype(self.np_types[0])
                    elif np.issubdtype(seq.dtype, np.integer):
                        seq = seq.astype(self.np_types[0])
                    else:
                        raise ValueError(f"Incompatible np dtype ({seq.dtype}) for {self.j_name} array")
            elif isinstance(seq, pd.Series):
                return self.array_of(seq.values, nan_to_value, null_to_value)

            seq = _handle_nan_and_null(seq, self, nan_to_value, null_to_value)
            return super().array_of(seq)
        except Exception as e:
            raise DHError(e, f"failed to create a Java {self.j_name} array.") from e


class CharDType(DType):
    """ The class for char type. """

    def __init__(self, j_name: str, qst_type: Any, np_dtypes: Tuple, null_value: Any):
        super().__init__(j_name=j_name, qst_type=qst_type, is_primitive=True)
        self.np_types = np_dtypes
        self.null_value = null_value

    def array_of(self, seq):
        """ Creates a Java char array populated with values from a sequence.

        Args:
            seq: a sequence of compatible data, e.g. list, tuple, numpy array, Pandas series, etc.

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
                elif seq.dtype == object:
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


# region predefined types and aliases
bool_ = DType(j_name="java.lang.Boolean", qst_type=_JQstType.booleanType(), )
byte = IntegerDType(j_name="byte", qst_type=_JQstType.byteType(), np_dtypes=(np.int8, np.uint8),
                    null_value=NULL_BYTE)
int8 = byte
short = IntegerDType(j_name="short", qst_type=_JQstType.shortType(), np_dtypes=(np.int16, np.uint16),
                     null_value=NULL_SHORT)
int16 = short
char = CharDType(j_name="char", qst_type=_JQstType.charType(), np_dtypes=(), null_value=NULL_CHAR)
int_ = IntegerDType(j_name="int", qst_type=_JQstType.intType(), np_dtypes=(np.int32, np.uint32),
                    null_value=NULL_INT)
int32 = int_
long = IntegerDType(j_name="long", qst_type=_JQstType.longType(), np_dtypes=(np.int64, np.uint64),
                    null_value=NULL_LONG)
int64 = long
float_ = FloatingDType(j_name="float", qst_type=_JQstType.floatType(), np_dtypes=(np.float32,),
                       null_value=NULL_FLOAT)
single = float_
float32 = float_
double = FloatingDType(j_name="double", qst_type=_JQstType.doubleType(), np_dtypes=(np.float64,),
                       null_value=NULL_DOUBLE)
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
