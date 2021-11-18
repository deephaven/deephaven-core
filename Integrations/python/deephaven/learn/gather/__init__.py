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
def table_to_numpy_2d(idx, cols, np_dtype = None):
    """
    Convert Deephaven table data to a 2d NumPy array of the appropriate size

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy
    :param dtype: The desired NumPy data type of the output NumPy array
    :return: A NumPy ndarray
    """

    if np_dtype == bool:
        np_dtype = np.bool_
    elif np_dtype == float:
        np_dtype = np.double
    elif np_dtype == int:
        np_dtype = np.intc

    if np_dtype == bool:
        buffer = _gatherer.tensorBuffer2DBoolean(idx, cols,)
    elif np_dtype == np.byte:
        buffer = _gatherer.tensorBuffer2DByte(idx, cols)
    elif np_dtype == np.short:
        buffer = _gatherer.tensorBuffer2DShort(idx, cols)
    elif np_dtype == np.intc:
        buffer = _gatherer.tensorBuffer2DInt(idx, cols)
    elif np_dtype == np.int_:
        buffer = _gatherer.tensorBuffer2DLong(idx, cols)
    elif np_dtype == np.single:
        buffer = _gatherer.tensorBuffer2DFloat(idx, cols)
    elif np_dtype == np.double:
        buffer = _gatherer.tensorBuffer2DDouble(idx, cols)
    else:
        raise ValueError("Data type {input_type} is not supported.".format(input_type = np_dtype))

    tensor = np.frombuffer(buffer, dtype = np_dtype)
    tensor.shape = (idx.getSize(), len(cols))

    return tensor
