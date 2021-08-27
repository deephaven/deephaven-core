
"""
Tools for users to manipulate tables.
"""


#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

############################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonIntegrationStaticMethods or
# "./gradlew :Generators:generatePythonIntegrationStaticMethods" to generate
#############################################################################


import jpy
import logging
import sys
import wrapt
import numpy
import pandas
from datetime import datetime, date

from ..conversion_utils import _isJavaType, _isStr, _isJavaArray, makeJavaArray, _parseJavaArrayType, \
    _basicArrayTypes, _boxedArrayTypes, _nullValues, NULL_CHAR, convertToJavaList


_java_type_ = None  # None until the first _defineSymbols() call


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_
    if _java_type_ is None:
        # This will raise an exception if the desired object is not the classpath
        _java_type_ = jpy.get_type("io.deephaven.db.tables.utils.TableTools")


# every module method should be decorated with @_passThrough
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


def _custom_newTable(*args):
    # should only be called via newTable method below
    if (len(args) in [2, 3]) and (isinstance(args[0], int) or isinstance(args[0], float)):
        rows = int(args[0])

        if isinstance(args[1], dict):
            # desired args = (long, map<String, ColumnSource>)
            pydict = args[1]
            java_map = jpy.get_type("java.util.HashMap")()
            for key in pydict:
                java_map.put(key, pydict[key])
            return _java_type_.newTable(rows, java_map)
        elif (len(args) == 3) and isinstance(args[1], list) and isinstance(args[2], list):
            # desired args = (long, List<String>, List<ColumnSource>)
            if _isJavaArray(args[1]):
                names = convertToJavaList(args[1])
            else:
                names = convertToJavaList(jpy.array('java.lang.String', args[1]))
            if _isJavaArray(args[2]):
                sources = convertToJavaList(args[2])
            else:
                sources = convertToJavaList(jpy.array('io.deephaven.db.v2.sources.ColumnSource', args[2]))
            return _java_type_.newTable(rows, names, sources)

    return _java_type_.newTable(*args)


def _custom_colSource(*args):
    # should only be called from colSource() below
    if len(args) < 1:
        # pass it straight through
        return _java_type_.colSource(*args)
    elif len(args) == 1:
        if isinstance(args[0], int) or isinstance(args[0], float) or isinstance(args[0], bool) or \
                isinstance(args[0], datetime) or isinstance(args[0], date) or _isStr(args[0]):
            return _custom_colSource(numpy.asarray(args))
        elif isinstance(args[0], numpy.ndarray) or isinstance(args[0], pandas.Series) \
                or isinstance(args[0], pandas.Categorical):
            arr = makeJavaArray(args[0], 'unknown')  # check if this is one of the basic primitive types...
            if arr is None:
                raise ValueError("Unable to parse input arguments to viable java array")

            dimension, basic_type = _parseJavaArrayType(arr)

            if dimension == 0:  # this is an empty array
                return _java_type_.colSource()
            elif dimension == 1 and basic_type == 'java.lang.String':
                # fetching a java.lang.String into python converts it to python str, so...
                return _java_type_.colSource(jpy.get_type(basic_type)().getClass(), convertToJavaList(arr))
            elif dimension == 1 and basic_type == 'java.lang.Boolean':
                # fetching a java.lang.Boolean into python converts it to python bool, so...
                return _java_type_.colSource(jpy.get_type(basic_type)(True).getClass(), convertToJavaList(arr))
            elif dimension > 1 or basic_type not in _basicArrayTypes:
                # we have to turn arr into a collection
                return _java_type_.colSource(arr[0].getClass(), convertToJavaList(arr))
            else:
                # it's a one-dimensional primitive type - so straight through
                return _java_type_.colSource(arr)
        elif isinstance(args[0], list) or isinstance(args[0], tuple):
            if len(args[0]) < 1:
                return _java_type_.colSource()
            # naively try to turn it into a numpy array and send it through
            return _custom_colSource(numpy.asarray(args[0]))
        elif _isJavaType(args[0]):
            return _java_type_.colSource(*args)
    else:
        if isinstance(args[0], int) or isinstance(args[0], float):
            # naively try to turn it into a numpy array and send it through
            return _custom_colSource(numpy.asarray(args))
        elif _isJavaType(args[0]):
            # push it straight through
            return _java_type_.colSource(*args)
        else:
            # naively try to turn it into a numpy array and send it through
            return _custom_colSource(numpy.asarray(args))
    # push it straight through
    return _java_type_.colSource(*args)


def _custom_objColSource(*args):
    # should only be called from objColSource() below
    if len(args) < 1 or _isJavaType(args[0]):
        # pass it straight through
        return _java_type_.objColSource(*args)
    elif len(args) == 1:
        if isinstance(args[0], numpy.ndarray) or isinstance(args[0], pandas.Series) \
                or isinstance(args[0], pandas.Categorical):
            arr = makeJavaArray(args[0], 'unknown')  # check if this is one of the basic primitive types...
            if arr is None:
                raise ValueError("Unable to parse input arguments to viable java array")

            dimension, basic_type = _parseJavaArrayType(arr)
            if dimension == 0:  # this is an empty array
                return _java_type_.objColSource()
            else:
                return _java_type_.objColSource(arr)
        elif isinstance(args[0], list) or isinstance(args[0], tuple):
            if len(args[0]) < 1:
                _java_type_.objColSource([])
            # naively try to turn it into a numpy array and send it through
            return _custom_objColSource(numpy.asarray(args[0]))
    # pass it straight through in any other circumstance
    return _java_type_.objColSource(*args)


def _custom_col(name, *data):
    # should only be called from col() below
    if len(data) < 1:
        raise ValueError("No data provided")
    if len(data) == 1:
        if isinstance(data[0], int) or isinstance(data[0], float) or isinstance(data[0], bool) or \
                isinstance(data[0], datetime) or isinstance(data[0], date):
            return _custom_col(name, numpy.asarray(data))
        elif _isJavaType(data[0]):
            return _java_type_.col(name, data[0])
        elif _isStr(data[0]):
            return _java_type_.col(name, jpy.array('java.lang.String', data))
        elif isinstance(data[0], numpy.ndarray) or isinstance(data[0], pandas.Series) \
                or isinstance(data[0], pandas.Categorical):
            arr = makeJavaArray(data[0], name)
            dimension, basic_type = _parseJavaArrayType(arr)
            if dimension == 0:  # this is an empty array
                return _java_type_.col(name, [])
            if dimension == 1 and basic_type in _boxedArrayTypes:
                # it's supposed to be boxed. This is silly.
                return _java_type_.col(name, jpy.array(_boxedArrayTypes[basic_type], arr))
            return _java_type_.col(name, arr)
        elif isinstance(data[0], list) or isinstance(data[0], tuple):
            if len(data[0]) < 1:
                return _java_type_.col(name, [])
            # naively try to turn it into a numpy array and send it through
            return _custom_col(name, numpy.asarray(data[0]))
        else:
            raise ValueError("Encountered unexpected type {}".format(type(data[0])))
    else:
        if isinstance(data[0], int) or isinstance(data[0], float):
            # naively try to turn it into a numpy array and send it through
            return _custom_col(name, numpy.asarray(data))
        elif _isJavaType(data[0]):
            # push it straight through
            return _java_type_.col(name, *data)
        else:
            # naively try to turn it into a numpy array and send it through
            return _custom_col(name, numpy.asarray(data))


def _custom_charCol(name, *data):
    def makeElementChar(el):
        if el is None:
            return NULL_CHAR
        if isinstance(el, int):
            return el
        if _isStr(el):
            if len(el) < 1:
                return NULL_CHAR
            return ord(el[0])
        try:
            return int(el)
        except ValueError:
            return NULL_CHAR

    # should only be called from charCol() below
    if len(data) < 1:
        raise ValueError("No data provided")
    if len(data) == 1:
        if _isJavaType(data[0]):
            return _java_type_.charCol(name, data[0])
        elif _isStr(data[0]):
            return _java_type_.charCol(name, [ord(char) for char in data[0]])  # NB: map returns an iterable in py3
        elif isinstance(data[0], numpy.ndarray):
            if data[0].dtype == numpy.uint16:
                return _java_type_.charCol(name, jpy.array('char', data[0]))
            elif data[0].dtype in [numpy.int8, numpy.uint8, numpy.int16, numpy.int32, numpy.uint32, numpy.int64, numpy.uint64]:
                # not entirely sure that this is necessary
                return _java_type_.charCol(name, jpy.array('char', data[0].astype(numpy.uint16)))
            elif data[0].dtype == numpy.dtype('U1') and data[0].dtype.name in ['unicode32', 'str32', 'string32', 'bytes32']:
                junk = numpy.copy(data[0])
                junk.dtype = numpy.uint32
                return _java_type_.charCol(name, jpy.array('char', junk.astype(numpy.uint16)))
            elif data[0].dtype == numpy.dtype('S1') and data[0].dtype.name in ['str8', 'string8', 'bytes8']:
                junk = numpy.copy(data[0])
                junk.dtype = numpy.uint8
                return _java_type_.charCol(name, jpy.array('char', junk.astype(numpy.uint16)))
            elif data[0].dtype == numpy.object:
                # do our best
                return _java_type_.charCol(name, jpy.array('char', numpy.array([makeElementChar(el) for el in data[0]], dtype=numpy.uint16)))
            else:
                raise ValueError("Input was an ndarray, expected integer dtype or "
                                 "one character string dtype, and got {}".format(data[0].dtype))
        elif isinstance(data[0], pandas.Series):
            return _custom_charCol(name, data[0].values)
        elif hasattr(data[0], '__iter__'):
            # naively turn it into a numpy array, and see what happens
            try:
                return _custom_charCol(name, numpy.asarray(data[0]))
            except Exception as e:
                logging.error("Attempted converting charCol() input to numpy array and failed.")
                raise e
    else:
        # naively turn it into a numpy array, and see what happens
        try:
            narr = numpy.asarray(data)
            return _custom_charCol(name, narr)
        except Exception as e:
            logging.error("Attempted converting charCol() input to numpy array and failed.")
            raise e


_intTypes = (numpy.int8, numpy.int16, numpy.int32, numpy.int64, numpy.uint8, numpy.uint16, numpy.uint32, numpy.uint64)
_floatTypes = (numpy.float32, numpy.float64)
_intTypeMapping = {
    'byte': [numpy.int8, numpy.uint8], 'short': [numpy.int16, numpy.uint16],
    'int': [numpy.int32, numpy.uint32], 'long': [numpy.int64, numpy.uint64]
}
_floatTypeMapping = {'float': numpy.float32, 'double': numpy.float64}


def _handleIntTypeColumn(typ, func, name, *data):
    if len(data) < 1:
        raise ValueError("No data provided")
    if len(data) == 1 and isinstance(data[0], numpy.ndarray):
        if data[0].dtype in _intTypeMapping[typ]:
            return func(name, jpy.array(typ, data[0]))
        elif data[0].dtype in _intTypes:
            return func(name, jpy.array(typ, data[0].astype(_intTypeMapping[typ][0])))
        elif data[0].dtype in _floatTypes:
            boolc = numpy.isnan(data[0])
            junk = data[0].astype(_intTypeMapping[typ][0])
            junk[boolc] = _nullValues[typ]
            return func(name, jpy.array(typ, junk))
        else:
            raise ValueError("Incompatible numpy dtype ({}) for {} array".format(data[0].dtype, typ))
    elif len(data) == 1 and isinstance(data[0], pandas.Series):
        return _handleIntTypeColumn(typ, func, name, data[0].values)
    else:
        return func(name, *data)


def _handleFloatTypeColumn(typ, func, name, *data):
    if len(data) < 1:
        raise ValueError("No data provided")
    if len(data) == 1 and isinstance(data[0], numpy.ndarray):
        if data[0].dtype == _floatTypeMapping[typ]:
            junk = data[0].copy()
            junk[numpy.isnan(junk)] = _nullValues[typ]
            return func(name, jpy.array(typ, data[0]))
        elif data[0].dtype in _floatTypes:
            junk = data[0].astype(typ)
            junk[numpy.isnan(junk)] = _nullValues[typ]
            return func(name, jpy.array(typ, junk))
        elif data[0].dtype in _intTypes:
            return func(name, jpy.array(typ, data[0].astype(_floatTypeMapping[typ])))
        else:
            raise ValueError("Incompatible numpy dtype ({}) for {} array".format(data[0].dtype, typ))
    elif len(data) == 1 and isinstance(data[0], pandas.Series):
        return _handleFloatTypeColumn(typ, func, name, data[0].values)
    else:
        return func(name, *data)


def _custom_byteCol(name, *data):
    # should only be called from byteCol() below
    return _handleIntTypeColumn('byte', _java_type_.byteCol, name, *data)


def _custom_shortCol(name, *data):
    # should only be called from shortCol() below
    return _handleIntTypeColumn('short', _java_type_.shortCol, name, *data)


def _custom_intCol(name, *data):
    # should only be called from intCol() below
    return _handleIntTypeColumn('int', _java_type_.intCol, name, *data)


def _custom_longCol(name, *data):
    # should only be called from longCol() below
    return _handleIntTypeColumn('long', _java_type_.longCol, name, *data)


def _custom_floatCol(name, *data):
    # should only be called from floatCol() below
    return _handleFloatTypeColumn('float', _java_type_.floatCol, name, *data)


def _custom_doubleCol(name, *data):
    # should only be called from doubleCol() below
    return _handleFloatTypeColumn('double', _java_type_.doubleCol, name, *data)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass


@_passThrough
def base64Fingerprint(source):
    """
    Compute the SHA256 hash of the input table and return it in base64 string format.
     
    
    :param source: (io.deephaven.db.tables.Table) - The table to fingerprint
    :return: (java.lang.String) The SHA256 hash of the table data and TableDefinition
    """
    
    return _java_type_.base64Fingerprint(source)


@_passThrough
def byteCol(name, *data):
    """
    Creates a new ColumnHolder of type `byte` that can be used when creating in-memory tables.
    
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    data structure:
      * an int or list of ints
      * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_BYTE`
        constant values, and all other values simply cast.
      * a :class:`pandas.Series` whose values are a numpy array described above
    """
    
    return _custom_byteCol(name, *data)


@_passThrough
def charCol(name, *data):
    """
    Creates a new ColumnHolder of type `char` that can be used when creating in-memory tables.
    
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    `data` structure:
      * an int
      * a string - will be interpreted as list of characters
      * a :class:`numpy.ndarray` of integer or one-character string type
      * a :class:`pandas.Series` whose values are a numpy array described above
      * an iterable of integers or strings - if string, only the first character will be used
    """
    
    return _custom_charCol(name, *data)


@_passThrough
def col(name, *data):
    """
    Returns a ColumnHolder that can be used when creating in-memory tables.
    
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    data structure:
      * an int, bool, float, datetime, date, string or iterable of (one) such
      * :class:`numpy.ndarray` containing boolean, numerical, datetime64, object, or string data (type inferred)
      * :class:`pandas.Series` object whose values are such a numpy array
    """
    
    return _custom_col(name, *data)


@_passThrough
def colSource(*args):
    """
    Creates a column of appropriate type, used for creating in-memory tables.
        
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.sources.ColumnSource<T>) a Deephaven ColumnSource of inferred type
    
    data structure:
      * a java object, or list of java objects
      * an int, bool, float, datetime, date, string or iterable of (one) such
      * :class:`pandas.Series` object whose values are such a numpy array
    """
    
    return _custom_colSource(*args)


@_passThrough
def computeFingerprint(source):
    """
    Compute the SHA256 hash of the input table.
     
    
     The hash is computed using every value in each row, using toString for unrecognized objects. The hash also
     includes the input table definition column names and types.
     
    
    :param source: (io.deephaven.db.tables.Table) - The table to fingerprint
    :return: (byte[]) The SHA256 hash of the table data and TableDefinition
    """
    
    return list(_java_type_.computeFingerprint(source))


@_passThrough
def dateTimeCol(name, *data):
    """
    Returns a ColumnHolder of type DBDateTime that can be used when creating in-memory tables.
    
    :param name: (java.lang.String) - name of the column
    :param data: (io.deephaven.db.tables.utils.DBDateTime...) - a list of values for the column
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    """
    
    return _java_type_.dateTimeCol(name, *data)


@_passThrough
def diff(*args):
    """
    Computes the difference of two tables for use in verification.
    
    *Overload 1*  
      :param actualResult: (io.deephaven.db.tables.Table) - first Deephaven table object to compare
      :param expectedResult: (io.deephaven.db.tables.Table) - second Deephaven table object to compare
      :param maxDiffLines: (long) - stop comparing after this many differences are found
      :return: (java.lang.String) String report of the detected differences
      
    *Overload 2*  
      :param actualResult: (io.deephaven.db.tables.Table) - first Deephaven table object to compare
      :param expectedResult: (io.deephaven.db.tables.Table) - second Deephaven table object to compare
      :param maxDiffLines: (long) - stop comparing after this many differences are found
      :param itemsToSkip: (java.util.EnumSet<io.deephaven.db.tables.utils.TableDiff.DiffItems>) - EnumSet of checks not to perform, such as checking column order, or exact match of double
              values
      :return: (java.lang.String) String report of the detected differences
    """
    
    return _java_type_.diff(*args)


@_passThrough
def diffPair(actualResult, expectedResult, maxDiffLines, itemsToSkip):
    """
    Computes the difference of two tables for use in verification.
    
    :param actualResult: (io.deephaven.db.tables.Table) - first Deephaven table object to compare
    :param expectedResult: (io.deephaven.db.tables.Table) - second Deephaven table object to compare
    :param maxDiffLines: (long) - stop comparing after this many differences are found
    :param itemsToSkip: (java.util.EnumSet<io.deephaven.db.tables.utils.TableDiff.DiffItems>) - EnumSet of checks not to perform, such as checking column order, or exact match of double
            values
    :return: (io.deephaven.base.Pair<java.lang.String,java.lang.Long>) a pair of String report of the detected differences, and the first different row (0 if there are no
             different data values)
    """
    
    return _java_type_.diffPair(actualResult, expectedResult, maxDiffLines, itemsToSkip)


@_passThrough
def doubleCol(name, *data):
    """
    Creates a new ColumnHolder of type `double` that can be used when creating in-memory tables.
    
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    data structure:
      * an int or float or list of ints or floats
      * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_DOUBLE`
         constant values, and all other values simply cast.
      * a :class:`pandas.Series` whose values are a numpy array described above
    """
    
    return _custom_doubleCol(name, *data)


@_passThrough
def emptyTable(size):
    """
    Returns a new, empty Deephaven Table.
    
    :param size: (long) - the number of rows to allocate space for
    :return: (io.deephaven.db.tables.Table) a Deephaven Table with no columns.
    """
    
    return _java_type_.emptyTable(size)


@_passThrough
def floatCol(name, *data):
    """
    Creates a new ColumnHolder of type `float` that can be used when creating in-memory tables.
    
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    data structure:
      * a int or float or list of ints or floats
      * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_FLOAT`
        constant values, and all other values simply cast.
      * a :class:`pandas.Series` whose values are a numpy array described above
    """
    
    return _custom_floatCol(name, *data)


@_passThrough
def getKey(groupByColumnSources, row):
    """
    Returns a SmartKey for the specified row from a set of ColumnSources.
    
    :param groupByColumnSources: (io.deephaven.db.v2.sources.ColumnSource<?>[]) - a set of ColumnSources from which to retrieve the data
    :param row: (long) - the row number for which to retrieve data
    :return: (java.lang.Object) a Deephaven SmartKey object
    """
    
    return _java_type_.getKey(groupByColumnSources, row)


@_passThrough
def getPrevKey(groupByColumnSources, row):
    """
    Returns a SmartKey for the row previous to the specified row from a set of ColumnSources.
    
    :param groupByColumnSources: (io.deephaven.db.v2.sources.ColumnSource<?>[]) - a set of ColumnSources from which to retrieve the data
    :param row: (long) - the row number for which to retrieve the previous row's data
    :return: (java.lang.Object) a Deephaven SmartKey object
    """
    
    return _java_type_.getPrevKey(groupByColumnSources, row)


@_passThrough
def html(source):
    """
    Returns a printout of a table formatted as HTML. Limit use to small tables to avoid running out of memory.
    
    :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
    :return: (java.lang.String) a String of the table printout formatted as HTML
    """
    
    return _java_type_.html(source)


@_passThrough
def intCol(name, *data):
    """
    Creates a new ColumnHolder of type `int` that can be used when creating in-memory tables.
    
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    data structure:
      * an int or list of ints
      * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_INT`
        constant values, and all other values simply cast.
      * a :class:`pandas.Series` whose values are a numpy array described above
    """
    
    return _custom_intCol(name, *data)


@_passThrough
def longCol(name, *data):
    """
    Creates a new ColumnHolder of type `long` that can be used when creating in-memory tables.
    
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    data structure:
      * an int or list of ints
      * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_LONG`
        constant values, and all other values simply cast.
      * a :class:`pandas.Series` whose values are a numpy array described above
    """
    
    return _custom_longCol(name, *data)


@_passThrough
def merge(*args):
    """
    Concatenates multiple Deephaven Tables into a single Table.
    
     
     The resultant table will have rows from the same table together, in the order they are specified as inputs.
     
    
     When ticking tables grow, they may run out of the 'pre-allocated' space for newly added rows. When more key-
     space is needed, tables in higher key-space are shifted to yet higher key-space to make room for new rows. Shifts
     are handled efficiently, but some downstream operations generate a linear O(n) amount of work per shifted row.
     When possible, one should favor ordering the constituent tables first by static/non-ticking sources followed by
     tables that are expected to grow at slower rates, and finally by tables that grow without bound.
    
    *Overload 1*  
      :param theList: (java.util.List<io.deephaven.db.tables.Table>) - a List of Tables to be concatenated
      :return: (io.deephaven.db.tables.Table) a Deephaven table object
      
    *Overload 2*  
      :param tables: (java.util.Collection<io.deephaven.db.tables.Table>) - a Collection of Tables to be concatenated
      :return: (io.deephaven.db.tables.Table) a Deephaven table object
      
    *Overload 3*  
      :param tables: (io.deephaven.db.tables.Table...) - a list of Tables to be concatenated
      :return: (io.deephaven.db.tables.Table) a Deephaven table object
    """
    
    return _java_type_.merge(*args)


@_passThrough
def mergeSorted(keyColumn, *tables):
    """
    Concatenates multiple sorted Deephaven Tables into a single Table sorted by the specified key column.
     
     The input tables must each individually be sorted by keyColumn, otherwise results are undefined.
    
    *Overload 1*  
      :param keyColumn: (java.lang.String) - the column to use when sorting the concatenated results
      :param tables: (io.deephaven.db.tables.Table...) - sorted Tables to be concatenated
      :return: (io.deephaven.db.tables.Table) a Deephaven table object
      
    *Overload 2*  
      :param keyColumn: (java.lang.String) - the column to use when sorting the concatenated results
      :param tables: (java.util.Collection<io.deephaven.db.tables.Table>) - a Collection of sorted Tables to be concatenated
      :return: (io.deephaven.db.tables.Table) a Deephaven table object
    """
    
    return _java_type_.mergeSorted(keyColumn, *tables)


@_passThrough
def newTable(*args):
    """
    Creates a new DynamicTable.
    
    *Overload 1*  
      :param size: (long) - the number of rows to allocate
      :param names: (java.util.List<java.lang.String>) - a List of column names
      :param columnSources: (java.util.List<io.deephaven.db.v2.sources.ColumnSource<?>>) - a List of the ColumnSource(s)
      :return: (io.deephaven.db.v2.DynamicTable) a Deephaven DynamicTable
      
    *Overload 2*  
      :param size: (long) - the number of rows to allocate
      :param columns: (java.util.Map<java.lang.String,io.deephaven.db.v2.sources.ColumnSource<?>>) - a Map of column names and ColumnSources
      :return: (io.deephaven.db.v2.DynamicTable) a Deephaven DynamicTable
      
    *Overload 3*  
      :param definition: (io.deephaven.db.tables.TableDefinition) - the TableDefinition (column names and properties) to use for the new table
      :return: (io.deephaven.db.v2.DynamicTable) an empty Deephaven DynamicTable object
      
    *Overload 4*  
      :param columnHolders: (io.deephaven.db.v2.utils.ColumnHolder...) - a list of ColumnHolders from which to create the table
      :return: (io.deephaven.db.v2.DynamicTable) a Deephaven DynamicTable
      
    *Overload 5*  
      :param definition: io.deephaven.db.tables.TableDefinition
      :param columnHolders: io.deephaven.db.v2.utils.ColumnHolder...
      :return: io.deephaven.db.v2.DynamicTable
    """
    
    return _custom_newTable(*args)


@_passThrough
def nullTypeAsString(dataType):
    """
    :param dataType: java.lang.Class<?>
    :return: java.lang.String
    """
    
    return _java_type_.nullTypeAsString(dataType)


@_passThrough
def objColSource(*values):
    """
    Creates a column of appropriate object type, used for creating in-memory tables.
    
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.sources.ColumnSource) a Deephaven ColumnSource of inferred type
    data structure:
        * a java object, or list of java objects
        * an int, bool, float, datetime, date, string or iterable of (one) such
    * :class:`numpy.ndarray` containing boolean, numerical, datetime64, object, or string data (type inferred)
    * :class:`pandas.Series` object whose values are such a numpy array
    """
    
    return _custom_objColSource(*values)


@_passThrough
def readCsv(*args):
    """
    Returns a memory table created from importing CSV data. The first row must be column names. Column data types are
     inferred from the data.
    
    *Overload 1*  
      :param is: (java.io.InputStream) - an InputStream providing access to the CSV data.
      :return: (io.deephaven.db.v2.DynamicTable) a Deephaven DynamicTable object
      
    *Overload 2*  
      :param is: (java.io.InputStream) - an InputStream providing access to the CSV data.
      :param separator: (char) - a char to use as the delimiter value when parsing the file.
      :return: (io.deephaven.db.v2.DynamicTable) a Deephaven DynamicTable object
      
    *Overload 3*  
      :param filePath: (java.lang.String) - the fully-qualified path to a CSV file to be read.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 4*  
      :param filePath: (java.lang.String) - the fully-qualified path to a CSV file to be read.
      :param format: (java.lang.String) - an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
              use as a delimiter.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 5*  
      :param filePath: (java.lang.String) - the fully-qualified path to a CSV file to be read.
      :param format: (java.lang.String) - an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
              use as a delimiter.
      :param progress: (io.deephaven.util.progress.StatusCallback) - a StatusCallback object that can be used to log progress details or update a progress bar. If
              passed explicitly as null, a StatusCallback instance will be created to log progress to the current
              logger.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 6*  
      :param file: (java.io.File) - a file object providing access to the CSV file to be read.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 7*  
      :param file: (java.io.File) - a file object providing access to the CSV file to be read.
      :param progress: (io.deephaven.util.progress.StatusCallback) - a StatusCallback object that can be used to log progress details or update a progress bar. If
              passed explicitly as null, a StatusCallback instance will be created to log progress to the current
              logger.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 8*  
      :param file: (java.io.File) - a file object providing access to the CSV file to be read.
      :param format: (java.lang.String) - an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
              use as a delimiter.
      :param progress: (io.deephaven.util.progress.StatusCallback) - a StatusCallback object that can be used to log progress details or update a progress bar. If
              passed explicitly as null, a StatusCallback instance will be created to log progress to the current
              logger.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
    """
    
    return _java_type_.readCsv(*args)


@_passThrough
def readHeaderlessCsv(*args):
    """
    Returns a memory table created from importing CSV data. Column data types are inferred from the data.
    
    *Overload 1*  
      :param filePath: (java.lang.String) - the fully-qualified path to a CSV file to be read.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 2*  
      :param filePath: (java.lang.String) - the fully-qualified path to a CSV file to be read.
      :param header: (java.util.Collection<java.lang.String>) - Column names to use for the resultant table.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 3*  
      :param filePath: (java.lang.String) - the fully-qualified path to a CSV file to be read.
      :param header: (java.lang.String...) - Column names to use for the resultant table.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 4*  
      :param filePath: (java.lang.String) - the fully-qualified path to a CSV file to be read.
      :param format: (java.lang.String) - an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
              use as a delimiter.
      :param progress: (io.deephaven.util.progress.StatusCallback) - a StatusCallback object that can be used to log progress details or update a progress bar. If
              passed explicitly as null, a StatusCallback instance will be created to log progress to the current
              logger.
      :param header: (java.util.Collection<java.lang.String>) - Column names to use for the resultant table.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
      
    *Overload 5*  
      :param file: (java.io.File) - a file object providing access to the CSV file to be read.
      :param format: (java.lang.String) - an Apache Commons CSV format name to be used to parse the CSV, or a single non-newline character to
              use as a delimiter.
      :param progress: (io.deephaven.util.progress.StatusCallback) - a StatusCallback object that can be used to log progress details or update a progress bar. If
              passed explicitly as null, a StatusCallback instance will be created to log progress to the current
              logger.
      :param header: (java.util.Collection<java.lang.String>) - Column names to use for the resultant table, or null if column names should be automatically
              generated.
      :return: (io.deephaven.db.tables.Table) a Deephaven Table object
    """
    
    return _java_type_.readHeaderlessCsv(*args)


@_passThrough
def roundDecimalColumns(*args):
    """
    Produce a new table with all the columns of this table, in the same order, but with double and
     float columns rounded to longs.
    
    *Overload 1*  
      :param table: io.deephaven.db.tables.Table
      :return: (io.deephaven.db.tables.Table) The new Table, with all double and float columns rounded to longs.
      
    *Overload 2*  
      :param table: io.deephaven.db.tables.Table
      :param columns: (java.lang.String...) - The names of the double and float columns to round.
      :return: (io.deephaven.db.tables.Table) The new Table, with the specified columns rounded to longs.
    """
    
    return _java_type_.roundDecimalColumns(*args)


@_passThrough
def roundDecimalColumnsExcept(table, *columnsNotToRound):
    """
    Produce a new table with all the columns of this table, in the same order, but with all double and
     float columns rounded to longs, except for the specified columnsNotToRound.
    
    :param table: io.deephaven.db.tables.Table
    :param columnsNotToRound: (java.lang.String...) - The names of the double and float columns not to round to
            longs
    :return: (io.deephaven.db.tables.Table) The new Table, with columns modified as explained above
    """
    
    return _java_type_.roundDecimalColumnsExcept(table, *columnsNotToRound)


@_passThrough
def shortCol(name, *data):
    """
    Creates a new ColumnHolder of type `short` that can be used when creating in-memory tables.
        
    :param name: name for the column
    :param data: variable argument for the data
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    
    data structure:
      * an int or list of ints
      * a :class:`numpy.ndarray` of integer or floating point values. `NaN` values will be mapped to `NULL_SHORT`
        constant values, and all other values simply cast.
      * a :class:`pandas.Series` whose values are a numpy array described above
    """
    
    return _custom_shortCol(name, *data)


@_passThrough
def show(*args):
    """
    Prints the first few rows of a table to standard output.
    
    *Overload 1*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 2*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 3*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param maxRowCount: (long) - the number of rows to return
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 4*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param maxRowCount: (long) - the number of rows to return
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 5*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param maxRowCount: (long) - the number of rows to return
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param out: (java.io.PrintStream) - a PrintStream destination to which to print the data
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 6*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param maxRowCount: (long) - the number of rows to return
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param delimiter: (java.lang.String) - a String value to use between printed values
      :param out: (java.io.PrintStream) - a PrintStream destination to which to print the data
      :param showIndex: (boolean) - a boolean indicating whether to also print index details
      :param columns: (java.lang.String...) - varargs of column names to display
    """
    
    return _java_type_.show(*args)


@_passThrough
def showCommaDelimited(*args):
    """
    Prints the first few rows of a table to standard output, with commas between values.
    
    *Overload 1*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 2*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param maxRowCount: (long) - the number of rows to return
      :param columns: (java.lang.String...) - varargs of column names to display
    """
    
    return _java_type_.showCommaDelimited(*args)


@_passThrough
def showWithIndex(*args):
    """
    Prints the first few rows of a table to standard output, and also prints the details of the index and record
     positions that provided the values.
    
    *Overload 1*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 2*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param maxRowCount: (long) - the number of rows to return
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 3*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param maxRowCount: (long) - the number of rows to return
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param out: (java.io.PrintStream) - a PrintStream destination to which to print the data
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 4*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param firstRow: (long) - the firstRow to display
      :param lastRow: (long) - the lastRow (exclusive) to display
      :param out: (java.io.PrintStream) - a PrintStream destination to which to print the data
      :param columns: (java.lang.String...) - varargs of column names to display
      
    *Overload 5*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param firstRow: (long) - the firstRow to display
      :param lastRow: (long) - the lastRow (exclusive) to display
      :param columns: (java.lang.String...) - varargs of column names to display
    """
    
    return _java_type_.showWithIndex(*args)


@_passThrough
def string(*args):
    """
    Returns the first few rows of a table as a pipe-delimited string.
    
    *Overload 1*  
      :param t: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param columns: (java.lang.String...) - varargs of columns to include in the result
      :return: (java.lang.String) a String
      
    *Overload 2*  
      :param t: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param size: (int) - the number of rows to return
      :param columns: (java.lang.String...) - varargs of columns to include in the result
      :return: (java.lang.String) a String
      
    *Overload 3*  
      :param t: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param columns: (java.lang.String...) - varargs of columns to include in the result
      :return: (java.lang.String) a String
      
    *Overload 4*  
      :param t: (io.deephaven.db.tables.Table) - a Deephaven table object
      :param size: (int) - the number of rows to return
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param columns: (java.lang.String...) - varargs of columns to include in the result
      :return: (java.lang.String) a String
    """
    
    return _java_type_.string(*args)


@_passThrough
def stringCol(name, *data):
    """
    Returns a ColumnHolder of type String that can be used when creating in-memory tables.
    
    :param name: (java.lang.String) - name of the column
    :param data: (java.lang.String...) - a list of values for the column
    :return: (io.deephaven.db.v2.utils.ColumnHolder) a Deephaven ColumnHolder object
    """
    
    return _java_type_.stringCol(name, *data)


@_passThrough
def timeTable(*args):
    """
    Creates a table that adds a new row on a regular interval.
    
    *Overload 1*  
      :param period: (java.lang.String) - time interval between new row additions.
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 2*  
      :param period: (java.lang.String) - time interval between new row additions
      :param replayer: (io.deephaven.db.v2.replay.ReplayerInterface) - data replayer
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 3*  
      :param startTime: (io.deephaven.db.tables.utils.DBDateTime) - start time for adding new rows
      :param period: (java.lang.String) - time interval between new row additions
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 4*  
      :param startTime: (io.deephaven.db.tables.utils.DBDateTime) - start time for adding new rows
      :param period: (java.lang.String) - time interval between new row additions
      :param replayer: (io.deephaven.db.v2.replay.ReplayerInterface) - data replayer
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 5*  
      :param startTime: (java.lang.String) - start time for adding new rows
      :param period: (java.lang.String) - time interval between new row additions
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 6*  
      :param startTime: (java.lang.String) - start time for adding new rows
      :param period: (java.lang.String) - time interval between new row additions
      :param replayer: (io.deephaven.db.v2.replay.ReplayerInterface) - data replayer
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 7*  
      :param periodNanos: (long) - time interval between new row additions in nanoseconds.
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 8*  
      :param periodNanos: (long) - time interval between new row additions in nanoseconds.
      :param replayer: (io.deephaven.db.v2.replay.ReplayerInterface) - data replayer
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 9*  
      :param startTime: (io.deephaven.db.tables.utils.DBDateTime) - start time for adding new rows
      :param periodNanos: (long) - time interval between new row additions in nanoseconds.
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 10*  
      :param startTime: (io.deephaven.db.tables.utils.DBDateTime) - start time for adding new rows
      :param periodNanos: (long) - time interval between new row additions in nanoseconds.
      :param replayer: (io.deephaven.db.v2.replay.ReplayerInterface) - data replayer
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 11*  
      :param startTime: (java.lang.String) - start time for adding new rows
      :param periodNanos: (long) - time interval between new row additions in nanoseconds.
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 12*  
      :param startTime: (java.lang.String) - start time for adding new rows
      :param periodNanos: (long) - time interval between new row additions in nanoseconds.
      :param replayer: (io.deephaven.db.v2.replay.ReplayerInterface) - data replayer
      :return: (io.deephaven.db.tables.Table) time table
      
    *Overload 13*  
      :param timeProvider: (io.deephaven.db.v2.utils.TimeProvider) - the time provider
      :param startTime: (io.deephaven.db.tables.utils.DBDateTime) - start time for adding new rows
      :param periodNanos: (long) - time interval between new row additions in nanoseconds.
      :return: (io.deephaven.db.tables.Table) time table
    """
    
    return _java_type_.timeTable(*args)


@_passThrough
def typeFromName(dataTypeStr):
    """
    :param dataTypeStr: java.lang.String
    :return: java.lang.Class<?>
    """
    
    return _java_type_.typeFromName(dataTypeStr)


@_passThrough
def writeCsv(*args):
    """
    Writes a DB table out as a CSV.
    
    *Overload 1*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param compressed: (boolean) - whether to compress (bz2) the file being written
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 2*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param compressed: (boolean) - whether to compress (bz2) the file being written
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param nullsAsEmpty: (boolean) - if nulls should be written as blank instead of '(null)'
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 3*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 4*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param nullsAsEmpty: (boolean) - if nulls should be written as blank instead of '(null)'
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 5*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param out: (java.io.PrintStream) - the stream to write to
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 6*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param out: (java.io.PrintStream) - the stream to write to
      :param nullsAsEmpty: (boolean) - if nulls should be written as blank instead of '(null)'
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 7*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param compressed: (boolean) - whether to zip the file being written
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 8*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param compressed: (boolean) - whether to zip the file being written
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param nullsAsEmpty: (boolean) - if nulls should be written as blank instead of '(null)'
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 9*  
      :param source: (io.deephaven.db.tables.Table) - a Deephaven table object to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param compressed: (boolean) - whether to zip the file being written
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param nullsAsEmpty: (boolean) - if nulls should be written as blank instead of '(null)'
      :param separator: (char) - the delimiter for the CSV
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 10*  
      :param sources: (io.deephaven.db.tables.Table[]) - an array of Deephaven table objects to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param compressed: (boolean) - whether to compress (bz2) the file being written
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param tableSeparator: (java.lang.String) - a String (normally a single character) to be used as the table delimiter
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 11*  
      :param sources: (io.deephaven.db.tables.Table[]) - an array of Deephaven table objects to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param compressed: (boolean) - whether to compress (bz2) the file being written
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param tableSeparator: (java.lang.String) - a String (normally a single character) to be used as the table delimiter
      :param nullsAsEmpty: boolean
      :param columns: (java.lang.String...) - a list of columns to include in the export
      
    *Overload 12*  
      :param sources: (io.deephaven.db.tables.Table[]) - an array of Deephaven table objects to be exported
      :param destPath: (java.lang.String) - path to the CSV file to be written
      :param compressed: (boolean) - whether to compress (bz2) the file being written
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - a DBTimeZone constant relative to which DBDateTime data should be adjusted
      :param tableSeparator: (java.lang.String) - a String (normally a single character) to be used as the table delimiter
      :param fieldSeparator: (char) - the delimiter for the CSV files
      :param nullsAsEmpty: (boolean) - if nulls should be written as blank instead of '(null)'
      :param columns: (java.lang.String...) - a list of columns to include in the export
    """
    
    return _java_type_.writeCsv(*args)
