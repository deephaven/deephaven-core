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
def table_to_numpy_bool(idx, cols):
    """
    Convert table data to a NumPy array of the appropriate size (dtype = np.bool_)

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    buffer = gatherer.booleanTensorBuffer2D(idx, cols)
    
    tensor = np.frombuffer(buffer, dtype = np.bool_)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)

@_passThrough
def table_to_numpy_byte(idx, cols):
    """
    Convert table data to a NumPy array of the appropriate size (dtype = np.byte)

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    buffer = gatherer.byteTensorBuffer2D(idx, cols)
    
    tensor = np.frombuffer(buffer, dtype = np.byte)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)

@_passThrough
def table_to_numpy_short(idx, cols):
    """
    Convert table data to a NumPy array of the appropriate size (dtype = np.short)

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    buffer = gatherer.shortTensorBuffer2D(idx, cols)
    
    tensor = np.frombuffer(buffer, dtype = np.short)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)

@_passThrough
def table_to_numpy_int(idx, cols):
    """
    Convert table data to a NumPy array of the appropriate size (dtype = np.intc)

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    buffer = gatherer.intTensorBuffer2D(idx, cols)
    
    tensor = np.frombuffer(buffer, dtype = np.intc)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)

@_passThrough
def table_to_numpy_long(idx, cols):
    """
    Convert table data to a NumPy array of the appropriate size (dtype = np.int_)

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    buffer = gatherer.longTensorBuffer2D(idx, cols)
    
    tensor = np.frombuffer(buffer, dtype = np.int_)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)

@_passThrough
def table_to_numpy_float(idx, cols):
    """
    Convert table data to a NumPy array of the appropriate size (dtype = np.single)

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    buffer = gatherer.floatTensorBuffer2D(idx, cols)
    
    tensor = np.frombuffer(buffer, dtype = np.single)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)

@_passThrough
def table_to_numpy_double(idx, cols):
    """
    Convert table data to a NumPy array of the appropriate size (dtype = np.double)

    :param idx: An IndexSet describing the number of rows in the table
    :param cols: ColumnSources describing which columns to copy to NumPy
    :param dtype: The desired data type of the resultant NumPy array
    :return: A NumPy ndarray
    """

    buffer = gatherer.boolearnTensorBuffer2D(idx, cols)
    
    tensor = np.frombuffer(buffer, dtype = np.double)
    tensor.shape = (idx.getSize(), len(cols))

    return np.squeeze(tensor)