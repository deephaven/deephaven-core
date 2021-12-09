# 
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent Pending
# 
"""
Utilities for gathering Deephaven table data into Python objects
"""

import numpy as np
import enum
import jpy
import wrapt

# None until the first _defineSymbols() call
_gatherer = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _gatherer
    global Layout

    if _gatherer is None:
        _gatherer = jpy.get_type("io.deephaven.integrations.learn.gather.NumPy")
    
class MemoryLayout(enum.Enum):
    """
    Memory layouts for an array.
    """
    ROW_MAJOR = True
    """Row-major memory layout."""
    COLUMN_MAJOR = False
    """Column-major memory layout."""
    C = True
    """Memory layout consistent with C arrays (row-major)."""
    FORTRAN = False
    """Memory layout consistent with Fortran arrays (column-major)."""

    def __init__(self, is_row_major):
        self.is_row_major = is_row_major


# Every method that depends on symbols defined via _defineSymbols() should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)

try:
    _defineSymbols()
except Exception as e:
    pass

@_passThrough
def convert_to_numpy_dtype(dtype):
    """
    Convert an input type to the corresponding NumPy data type

    :param dtype: A Python type
    """
    if dtype.__module__ == np.__name__:
        return dtype
    elif dtype == bool:
        dtype = np.bool_
    elif dtype == float:
        dtype = np.double
    elif dtype == int:
        dtype = np.intc
    else:
        raise ValueError(f"{dtype} is not a data type that can be converted to a NumPy dtype.")
    return dtype

@_passThrough
def table_to_numpy_2d(row_set, col_set, order:MemoryLayout = MemoryLayout.ROW_MAJOR, dtype:np.dtype = np.intc):
    """
    Convert Deephaven table data to a 2d NumPy array of the appropriate size

    :param row_set: A RowSequence describing the number of rows in the table
    :param col_set: ColumnSources describing which columns to copy
    :param order: :param order: The desired memory layout of the output array
    :param dtype: The desired NumPy data type of the output NumPy array
    :return: A NumPy ndarray
    """

    if not(isinstance(order, MemoryLayout)):
        raise ValueError(f"Invalid major order {order}.  Please use an enum value from MemoryLayout.")

    dtype = convert_to_numpy_dtype(dtype)

    if dtype == np.byte:
        buffer = _gatherer.tensorBuffer2DByte(row_set, col_set, order.is_row_major)
    elif dtype == np.short:
        buffer = _gatherer.tensorBuffer2DShort(row_set, col_set, order.is_row_major)
    elif dtype == np.intc:
        buffer = _gatherer.tensorBuffer2DInt(row_set, col_set, order.is_row_major)
    elif dtype == np.int_:
        buffer = _gatherer.tensorBuffer2DLong(row_set, col_set, order.is_row_major)
    elif dtype == np.single:
        buffer = _gatherer.tensorBuffer2DFloat(row_set, col_set, order.is_row_major)
    elif dtype == np.double:
        buffer = _gatherer.tensorBuffer2DDouble(row_set, col_set, order.is_row_major)
    else:
        raise ValueError(f"Data type {dtype} is not supported.")

    tensor = np.frombuffer(buffer, dtype = dtype)

    if order.is_row_major:
        tensor.shape = (len(col_set), row_set.intSize())
        return tensor.T
    else:
        tensor.shape = (row_set.intSize(), len(col_set))
        return tensor