# 
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent Pending
# 
"""
Utilities for gathering Deephaven table data into Python objects
"""

import numpy as np
import jpy
import wrapt

# None until the first _defineSymbols() call
_gatherer = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _gatherer

    if _gatherer is None:
        _gatherer = jpy.get_type("io.deephaven.integrations.learn.Gatherer")

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
def table_to_numpy_2d(row_set, col_set, order = 0, dtype = None):
    """
    Convert Deephaven table data to a 2d NumPy array of the appropriate size

    :param row_set: A RowSequence describing the number of rows in the table
    :param col_set: ColumnSources describing which columns to copy
    :param order: The major order of copying -> either row major or column major
    :param dtype: The desired NumPy data type of the output NumPy array
    :return: A NumPy ndarray
    """

    if dtype == bool:
        dtype = np.bool_
    elif dtype == float:
        dtype = np.double
    elif dtype == int:
        dtype = np.intc

    if dtype == np.bool_:
        if order == 0:
            buffer = _gatherer.tensorBuffer2DBooleanColumns(row_set, col_set)
        elif order == 1:
            buffer = _gatherer.tensorBuffer2DBooleanRows(row_set, col_set)
        else:
            raise ValueError("Order must be either 0 (row major) or 1 (column major).  The default is 0.")
    elif dtype == np.byte:
        if order == 0:
            buffer = _gatherer.tensorBuffer2DByteRows(row_set, col_set)
        elif order == 1:
            buffer = _gatherer.tensorBuffer2DByteColumns(row_set, col_set)
        else:
            raise ValueError("Order must be either 0 (row major) or 1 (column major).  The default is 0.")
    elif dtype == np.short:
        if order == 0:
            buffer = _gatherer.tensorBuffer2DShortRows(row_set, col_set)
        elif order == 1:
            buffer = _gatherer.tensorBuffer2DShortColumns(row_set, col_set)
        else:
            raise ValueError("Order must be either 0 (row major) or 1 (column major).  The default is 0.")
    elif dtype == np.intc:
        if order == 0:
            buffer = _gatherer.tensorBuffer2DIntRows(row_set, col_set)
        elif order == 1:
            buffer = _gatherer.tensorBuffer2DIntColumns(row_set, col_set)
        else:
            raise ValueError("Order must be either 0 (row major) or 1 (column major).  The default is 0.")
    elif dtype == np.int_:
        if order == 0:
            buffer = _gatherer.tensorBuffer2DLongRows(row_set, col_set)
        elif order == 1:
            buffer = _gatherer.tensorBuffer2DLongColumns(row_set, col_set)
        else:
            raise ValueError("Order must be either 0 (row major) or 1 (column major).  The default is 0.")
    elif dtype == np.single:
        if order == 0:
            buffer = _gatherer.tensorBuffer2DFloatRows(row_set, col_set)
        elif order == 1:
            buffer = _gatherer.tensorBuffer2DFloatColumns(row_set, col_set)
        else:
            raise ValueError("Order must be either 0 (row major) or 1 (column major).  The default is 0.")
    elif dtype == np.double:
        if order == 0:
            buffer = _gatherer.tensorBuffer2DDoubleRows(row_set, col_set)
        elif order == 1:
            buffer = _gatherer.tensorBuffer2DDoubleColumns(row_set, col_set)
        else:
            raise ValueError("Order must be either 0 (row major) or 1 (column major).  The default is 0.")
    else:
        raise ValueError("Data type {input_type} is not supported.".format(input_type = dtype))

    tensor = np.frombuffer(buffer, dtype = dtype)
    if order == 0:
        tensor.shape = (row_set.intSize(), len(col_set))
        return tensor
    else:
        tensor.shape = (len(col_set), row_set.intSize())
        return tensor.T