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
def table_to_numpy_2d(row_set, col_set, dtype = None):
    """
    Convert Deephaven table data to a 2d NumPy array of the appropriate size

    :param row_set: A RowSequence describing the number of rows in the table
    :param col_set: ColumnSources describing which columns to copy
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
        buffer = _gatherer.tensorBuffer2DBoolean(row_set, col_set)
    elif dtype == np.byte:
        buffer = _gatherer.tensorBuffer2DByte(row_set, col_set)
    elif dtype == np.short:
        buffer = _gatherer.tensorBuffer2DShort(row_set, col_set)
    elif dtype == np.intc:
        buffer = _gatherer.tensorBuffer2DInt(row_set, col_set)
    elif dtype == np.int_:
        buffer = _gatherer.tensorBuffer2DLong(row_set, col_set)
    elif dtype == np.single:
        buffer = _gatherer.tensorBuffer2DFloat(row_set, col_set)
    elif dtype == np.double:
        buffer = _gatherer.tensorBuffer2DDouble(row_set, col_set)
    else:
        raise ValueError("Data type {input_type} is not supported.".format(input_type = dtype))

    tensor = np.frombuffer(buffer, dtype = dtype)
    tensor.shape = (len(col_set), row_set.intSize())

    return tensor.T
