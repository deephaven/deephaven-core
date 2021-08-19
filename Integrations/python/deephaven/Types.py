#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
"""
Basic Deephaven table related data types
"""

import jpy
import wrapt
import sys
import collections

# None until the first _defineSymbols() call
_table_tools_ = None
_col_def_ = None
_python_tools_ = None
_jclazz_type_ = None
_qst_col_header_ = None
_qst_column_ = None
_qst_newtable_ = None
_table_ = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _table_tools_, _col_def_, _python_tools_, _jclazz_type_, _qst_col_header_, _qst_column_, _qst_newtable_, _table_
    if _table_tools_ is None:
        # This will raise an exception if the desired object is not the classpath
        _table_tools_ = jpy.get_type("io.deephaven.db.tables.utils.TableTools")
        _col_def_ = jpy.get_type("io.deephaven.db.tables.ColumnDefinition")
        _python_tools_ = jpy.get_type("io.deephaven.integrations.python.PythonTools")
        _jclazz_type_ = jpy.get_type("java.lang.Class")
        _qst_col_header_ = jpy.get_type("io.deephaven.qst.column.header.ColumnHeader")
        _qst_column_ =  jpy.get_type("io.deephaven.qst.column.Column")
        _qst_newtable_ =  jpy.get_type("io.deephaven.qst.table.NewTable")
        _table_ = jpy.get_type("io.deephaven.db.tables.Table")

# every method that depends on symbols defined via _defineSymbols() should be decorated with @_passThrough
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
def asType(type_str):
    return _table_tools_.typeFromName(type_str)

bool_ = asType('java.lang.Boolean')
byte = asType('byte')
short = asType('short')
int16 = short  # make life simple for people who are used to pyarrow
int_ = asType('int')
int32 = int_  # make life simple for people who are used to pyarrow
long_ = asType('long')
int64 = long_   # make life simple for people who are used to pyarrow
float_ = asType('float')
single = float_   # make life simple for people who are used to NumPy
float32 = float_  # make life simple for people who are used to pyarrow
double = asType('double')
float64 = double  # make life simple for people who are used to pyarrow
string = asType('java.lang.String')
bigdecimal = asType('java.math.BigDecimal')
stringset = asType('io.deephaven.db.tables.libs.StringSet')

byte_array = asType('byte')
short_array = asType('short[]')
int16_array = short_array
int_array = asType('int[]')
int32_array = int_array
long_array = asType('long[]')
int64_array = long_array
float_array = asType('float[]')
single_array = float_array
float32_array = float_array
double_array = asType('double[]')
float64_array = double_array
string_array = asType('java.lang.String[]')

datetime = asType('io.deephaven.db.tables.utils.DBDateTime')

@_passThrough
def _jpyTypeFromDh(t):
    mapping = {
        bool_      : 'java.lang.Boolean',
        byte       : 'byte',
        short      : 'short',
        int16      : 'short',
        int_       : 'int',
        int32      : 'int',
        long_      : 'long',
        int64      : 'long',
        float_     : 'float',
        single     : 'float',
        float32    : 'float',
        double     : 'double',
        float64    : 'double',
        string     : 'java.lang.String',
        bigdecimal : 'java.math.BigDecimal',
        stringset  : 'io.deephaven.db.tables.libs.StringSet' }
    return mapping[t]

@_passThrough
def col(t):
    """
    Convert a tuple of strings of the form ('Price', double_type)
    or ('Prices', double_array_type, double_type)
    to a ColumnDefinition object.

    :param t: a 2 or 3 element tuple of string and type(s) specifying a column definition object
    :return: the column definition object.
    """

    if not isinstance(t, tuple):
        raise Exception('argument ' + t + ' is not a tuple')
    if len(t) < 2 or len(t) > 3:
        raise Exception('Only 2 or 3 element tuples expected, got ' + len(t))
    col_name = t[0]
    if not isinstance(col_name, str):
        raise Exception('Element at index 0 (' + str(col_name) + ') for column name is not of str type, but '
                        + type(col_name).__name__)
    data_type = t[1]
    if not isinstance(data_type, _jclazz_type_):
        raise Exception('Element at index 1 (' + str(data_type) + ') has the wrong type ' +
                        type(data_type).__name__)
    if len(t) == 2:
        return _col_def_.fromGenericType(col_name, data_type)
    component_type = t[2]
    if not isinstance(component_type, _jclazz_type_):
        raise Exception('Element at index 2 (' + str(component_type) + ') for component type has the wrong type ' +
                        type(component_type).__name__)
    return _col_def_.fromGenericType(col_name, data_type, component_type)

@_passThrough
def cols(ts):
    """
    Convert a sequence of tuples of the form ('Price', double_type)
    or ('Prices', double_array_type, double_type)
    to a list of ColumnDefinition objects.

    :param ts: a sequence of 2 or 3 element tuples of (str, type) or (str, type, type)  specifying a column definition object.
    :return: a list of column definition objects.
    """
    r = []
    for t in ts:
        r.append(col(t))
    return r


#
# import dhtypes as dh
#
# data = [['tom', 10], ['nick', 15], ['juli', 14]]
# columns = [ ('Name' : dh.string), ('Price' : dh.double) ]
# t = dh.table_of(data, columns)
#
#
@_passThrough
def table_of(data, columns):
    """
    Create a Deephaven table; the API should feel familiar to users of Panda's DataFrame.

    :param data: a list of rows, where each row is in turn a list of values.
    :param columns: a list of tuples of the form (name, type) where name is the intended column name
                    and type is the intended Deephaven column data type.
    :return: a table with columns and data as specified in the arguments.
    """

    if columns is None or len(columns) == 0:
        if data is not None or len(data) > 0:
            raise Exception("when no column definitions are provided in the 'columns' argument, " +
                            "only an empty table can be created, and no data can be specified; instead " +
                            "got a non-empty 'data' argument with " + str(data))
        return _table_tools_.emptyTable()

    if isinstance(columns, str) or not isinstance(columns, collections.Sequence) or len(columns) < 1:
        raise Exception("argument 'columns' needs to be a sequence with at least one element, " +
                        "instead got " + str(columns) + " of type " + type(columns).__name__)
    
    if data is None or len(data) == 0:
        col_header = None
        for t in columns:
            if len(t) != 2:
                raise Exception("only two element type tuples are supported, instead got " + str(t))
            try:
                if col_header is None:
                    col_header = _qst_col_header_.of(t[0], t[1])
                else:
                    col_header = col_header.header(t[0], t[1])
            except Exception as e:
                raise Exception("Could not create column definition from " + str(t)) from e
        return _table_.of(col_header)

    nrows = len(data)
    ncols = len(columns)

    # capture columns from data.
    qst_cols = []
    for c in range(ncols):
        col_name = columns[c][0]
        col_type = columns[c][1]
        col_data = []
        for r in range(nrows):
            row = data[r]
            if len(row) < c + 1:
                raise Exception("not enough columns provided in row " + r)
            col_data.append(row[c])
        jvalues = jpy.array(_jpyTypeFromDh(col_type), col_data)
        qst_col = _qst_column_.of(col_name, jvalues)
        qst_cols.append(qst_col)

    qst_newtable = _qst_newtable_.of(qst_cols)
    return _table_.of(qst_newtable)

