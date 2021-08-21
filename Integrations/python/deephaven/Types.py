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
from typing import NewType

# None until the first _defineSymbols() call
_table_tools_ = None
_col_def_ = None
_python_tools_ = None
_qst_col_header_ = None
_qst_column_ = None
_qst_newtable_ = None
_table_ = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _table_tools_, _col_def_, _python_tools_, _qst_col_header_, _qst_column_, _qst_newtable_, _table_
    if _table_tools_ is None:
        # This will raise an exception if the desired object is not the classpath
        _table_tools_ = jpy.get_type("io.deephaven.db.tables.utils.TableTools")
        _col_def_ = jpy.get_type("io.deephaven.db.tables.ColumnDefinition")
        _python_tools_ = jpy.get_type("io.deephaven.integrations.python.PythonTools")
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


# Represents a Deephaven column data type.
DataType = NewType('DataType', type)

#
# Basic Deephaven column data types.
# Column data types in python are represented as the jpy wrapper for the
# corresponding Java class object for the column's Java type.
#
bool_ = DataType(jpy.get_type('java.lang.Boolean'))
byte = DataType(jpy.get_type('byte'))
short = DataType(jpy.get_type('short'))
int16 = short  # make life simple for people who are used to pyarrow
int_ = DataType(jpy.get_type('int'))
int32 = int_  # make life simple for people who are used to pyarrow
long_ = DataType(jpy.get_type('long'))
int64 = long_   # make life simple for people who are used to pyarrow
float_ = DataType(jpy.get_type('float'))
single = float_   # make life simple for people who are used to NumPy
float32 = float_  # make life simple for people who are used to pyarrow
double = DataType(jpy.get_type('double'))
float64 = double  # make life simple for people who are used to pyarrow
string = DataType(jpy.get_type('java.lang.String'))
bigdecimal = DataType(jpy.get_type('java.math.BigDecimal'))
stringset = DataType(jpy.get_type('io.deephaven.db.tables.libs.StringSet'))
datetime = DataType(jpy.get_type('io.deephaven.db.tables.utils.DBDateTime'))

byte_array = DataType(jpy.get_type('[B'))
short_array = DataType(jpy.get_type('[S'))
int16_array = short_array
int_array = DataType(jpy.get_type('[I'))
int32_array = int_array
long_array = DataType(jpy.get_type('[J'))
int64_array = long_array
float_array = DataType(jpy.get_type('[F'))
single_array = float_array
float32_array = float_array
double_array = DataType(jpy.get_type('[D'))
float64_array = double_array
string_array = DataType(jpy.get_type('[Ljava.lang.String;'))

# For more involved types, you can always use the string representation
# of the Java class (Class.getName()) to get a python type for it.
@_passThrough
def typeFromName(name : str):
    """
    Get the column data type for the corresponding Java type string reprensentation
    The string provided should match the output in Java for Class.getName()
    for a class visible to the main ClassLoader in the Deephaven engine in use.
    """
    return DataType(jpy.get_type(name))


@_passThrough
def _jclassFromType(data_type : DataType):
    if data_type is None:
        return None
    type2name = {
        bool_ : 'java.lang.Boolean',
        byte : 'byte',
        short : 'short',
        int_ : 'int',
        long_ : 'long',
        float_ : 'float',
        double : 'double',
        string : 'java.lang.String',
        bigdecimal : 'java.math.BigDecimal',
        stringset : 'io.deephaven.db.tables.libs.StringSet',
        datetime : 'io.deephaven.db.tables.utils.DBDateTime',
        byte_array : 'byte[]',
        short_array : 'short[]',
        int_array : 'int[]',
        long_array : 'long[]',
        float_array : 'float[]',
        double_array : 'double[]',
        string_array : 'java.lang.String[]',
    }
    name = type2name.get(data_type, None)
    if name is None:
        type2str = str(data_type)
        q0 = type2str.index("'")
        q1 = type2str.index("'", q0 + 1)
        name = type2str[q0 : q1 + 1]

    return _table_tools_.typeFromName(name)


@_passThrough
def _isPrimitive(data_type : DataType):
    primitives = { bool_, byte, short, int_, long_, float_, double }
    return data_type in primitives

@_passThrough
def col(col_name : str, data_type : DataType, component_type : DataType = None):
    """
    Create a ColumnDefinition object.
    :param col_name: The column's new.
    :param data_type: The column's data type.
    :param component_type: The column's component type, or None if none.
    :return: the column definition object.
    """
    data_type = _jclassFromType(data_type)
    component_type = _jclassFromType(component_type)

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
        r.append(col(*t))
    return r


@_passThrough
def _getQstCol(col_name, col_type, col_data=None):
    if col_data is None or len(col_data) < 1:
        jclass = _jclassFromType(col_type)
        col_header = _qst_col_header_.of(col_name, jclass)
        return _qst_column_.empty(col_header)
    jvalues = jpy.array(col_type, col_data)
    if _isPrimitive(col_type):
        return _qst_column_.ofUnsafe(col_name, jvalues)
    else:
        return _qst_column_.of(col_name, jvalues)
    

@_passThrough
def _getTable(qst_cols):
    qst_newtable = _qst_newtable_.of(qst_cols)
    return _table_.of(qst_newtable)        
    
@_passThrough
def _table_by_rows(data, columns):
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
        qst_col = _getQstCol(col_name, col_type, col_data)
        qst_cols.append(qst_col)

    return _getTable(qst_cols)


@_passThrough
def _table_by_cols(data:dict):
    qst_cols = []
    for col_name, type_values in data.items():
        if not isinstance(type_values, collections.Sequence):
            col_type = type_values
            col_data = None
        else:
            col_type = type_values[0]
            if len(type_values) < 2:
                col_data = None
            else:
                col_data = type_values[1]
                if not isinstance(col_data, collections.Sequence):
                    raise Exception("'data' argument is expected to contain a dict with " +
                                    "values of sequence type, with a first element indicating a " +
                                    "data type, and the second element is a sequence of column values, " +
                                    "instead got " + str(col_data) + " of type " + type(col_data).__name__)
        qst_col = _getQstCol(col_name, col_type, col_data)
        qst_cols.append(qst_col)

    return _getTable(qst_cols)


#
# import dhtypes as dh
#
# data = [['tom', 10], ['nick', 15], ['juli', 14]]
# columns = [ ('Name' : dh.string), ('Price' : dh.double) ]
# t = dh.table_of(data, columns)
#
#
@_passThrough
def table_of(data, columns=None):
    """
    Create a Deephaven table; the API should feel familiar to users of Panda's DataFrame.

    :param data: Either a sequence of rows, where each row is in turn a list of values (requires passing
                 the additional columns parameneter to specify column names and data types) or
                 a dict whose keys are column names and the values are either a data type, or a sequence of (1) type,
                 for the intended Deephaven column data type, and (2) values for the column
    :param columns: a list of tuples of the form (name, type) where name is the intended column name
                    and type is the intended Deephaven column data type.
    :return: a table with columns and data as specified in the arguments.
    """

    if (columns is None or len(columns) == 0) and not isinstance(data, dict):
        if data is not None or len(data) > 0:
            raise Exception("when no column definitions are provided in the 'columns' argument, " +
                            "only an empty table can be created, and no data can be specified; instead " +
                            "got a non-empty 'data' argument with " + str(data))
        return _table_tools_.emptyTable(0)

    if not isinstance(data, dict) and \
       (isinstance(columns, str) or not isinstance(columns, collections.Sequence) or len(columns) < 1):
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

    if isinstance(data, collections.Sequence):
        return _table_by_rows(data, columns)
    elif isinstance(data, dict):
        return _table_by_cols(data)
