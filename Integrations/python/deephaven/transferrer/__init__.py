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
def table_to_numpy(idx, cols, dtype = None):
    """
    Convert table data to a NumPy array of the appropriate size and data type

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    if dtype == np.bool_:
        buffer = gatherer.booleanTensorBuffer2D(idx, cols)
    elif dtype == np.byte:
        buffer = gatherer.byteTensorBuffer2D(idx, cols)
    elif dtype == np.short:
        buffer = gatherer.shortTensorBuffer2D(idx, cols)
    elif dtype == np.intc:
        buffer = gatherer.intTensorBuffer2D(idx, cols)
    elif dtype == np.int_:
        buffer = gatherer.longTensorBuffer2D(idx, cols)
    elif dtype == np.single:
        buffer = gatherer.floatTensorBuffer2D(idx, cols)
    elif dtype == np.double:
        buffer = gatherer.doubleTensorBuffer2D(idx, cols)
    else:
        raise ValueError(str(dtype) + " is not an accepted data type.")
    
    tensor = np.frombuffer(buffer, dtype = dtype)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)