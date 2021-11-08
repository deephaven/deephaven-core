# 
# Copyright (c) 2016 - 2021 Deephaven Data Labs and Patent Pending
# 
"""
Utilities for transfer between Python objects and Deephaven Tables
"""

import numpy as np
import jpy
import wrapt

# None until the first _defineSymbols() call
gatherer = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global gatherer

    if gatherer is None:
        gatherer = jpy.get_type("io.deephaven.integrations.learn.Gatherer")

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

    if np_dtype == np.bool_:
        buffer = gatherer.booleanTensorBuffer2D(idx, cols)
    elif np_dtype == np.byte:
        buffer = gatherer.byteTensorBuffer2D(idx, cols)
    elif np_dtype == np.double:
        buffer = gatherer.doubleTensorBuffer2D(idx, cols)
    elif np_dtype == np.intc:
        buffer = gatherer.intTensorBuffer2D(idx, cols)
    elif np_dtype == np.int_:
        buffer = gatherer.longTensorBuffer2D(idx, cols)
    elif np_dtype == np.short:
        buffer = gatherer.shortTensorBuffer2D(idx, cols)
    elif np_dtype == np.single:
        buffer = gatherer.floatTensorBuffer2D(idx, cols)

    tensor = np.frombuffer(buffer, dtype = np_dtype)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)