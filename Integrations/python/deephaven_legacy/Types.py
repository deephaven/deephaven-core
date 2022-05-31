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
_table_factory_ = None
_col_def_ = None
_python_tools_ = None
_qst_col_header_ = None
_qst_column_ = None
_qst_newtable_ = None
_qst_type_ = None
_table_ = None

# Represents a Deephaven column data type.
# None until the first _defineSymbols() call
DataType = None

#
# Define basic Deephaven column data types.
# Column data types in python are represented as the jpy wrapper for the
# corresponding Java class object for the column's Java type.
#
# None until the first _defineSymbols() call
bool_ = None
byte = None
short = None
int16 = None
char = None
int_ = None
int32 = None
long_ = None
int64 = None
float_ = None
single = None
float32 = None
double = None
float64 = None
string = None
bigdecimal = None
biginteger = None
stringset = None
datetime = None
timeperiod = None
byte_array = None
short_array = None
int16_array = None
int_array = None
int32_array = None
long_array = None
int64_array = None
float_array = None
single_array = None
float32_array = None
double_array = None
float64_array = None
string_array = None
_type2jtype = None


def _typeFromJavaClassName(name : str):
    jclass = _table_tools_.typeFromName(name)
    return DataType(_qst_type_.find(jclass))

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _table_tools_, _table_factory_, _col_def_, _python_tools_, _qst_col_header_, \
        _qst_column_, _qst_newtable_, _qst_type_, _table_, \
        DataType, bool_, byte, short, int16, char, int_, int32, long_, int64, \
        float_, single, float32, double, float64, \
        string, bigdecimal, biginteger, stringset, datetime, timeperiod, \
        byte_array, short_array, int16_array, int_array, int32_array, long_array, int64_array, \
        float_array, single_array, float32_array, double_array, float64_array, string_array, \
        _type2jtype

    if _table_tools_ is None:
        # This will raise an exception if the desired object is not the classpath
        _table_tools_ = jpy.get_type("io.deephaven.engine.util.TableTools")
        _table_factory_ = jpy.get_type("io.deephaven.engine.table.TableFactory")
        _col_def_ = jpy.get_type("io.deephaven.engine.table.ColumnDefinition")
        _python_tools_ = jpy.get_type("io.deephaven.integrations.python.PythonTools")
        _qst_col_header_ = jpy.get_type("io.deephaven.qst.column.header.ColumnHeader")
        _qst_column_ =  jpy.get_type("io.deephaven.qst.column.Column")
        _qst_newtable_ = jpy.get_type("io.deephaven.qst.table.NewTable")
        _qst_type_ = jpy.get_type("io.deephaven.qst.type.Type")
        _table_ = jpy.get_type("io.deephaven.engine.table.Table")

    if DataType is None:
        DataType = NewType('DataType', _qst_type_)
        bool_ = DataType(_qst_type_.booleanType())
        byte = DataType(_qst_type_.byteType())
        short = DataType(_qst_type_.shortType())
        int16 = short  # make life simple for people who are used to pyarrow
        char = DataType(_qst_type_.charType())
        int_ = DataType(_qst_type_.intType())
        int32 = int_  # make life simple for people who are used to pyarrow
        long_ = DataType(_qst_type_.longType())
        int64 = long_   # make life simple for people who are used to pyarrow
        float_ = DataType(_qst_type_.floatType())
        single = float_   # make life simple for people who are used to NumPy
        float32 = float_  # make life simple for people who are used to pyarrow
        double = DataType(_qst_type_.doubleType())
        float64 = double  # make life simple for people who are used to pyarrow
        string = DataType(_qst_type_.stringType())
        bigdecimal = _typeFromJavaClassName('java.math.BigDecimal')
        biginteger = _typeFromJavaClassName('java.math.BigInteger')
        stringset =  _typeFromJavaClassName('io.deephaven.stringset.StringSet')
        datetime = _typeFromJavaClassName('io.deephaven.time.DateTime')
        timeperiod = _typeFromJavaClassName('io.deephaven.time.Period')

        # Array types.
        byte_array = DataType(byte.arrayType())
        short_array = DataType(short.arrayType())
        int16_array = short_array
        int_array = DataType(int_.arrayType())
        int32_array = int_array
        long_array = DataType(long_.arrayType())
        int64_array = long_array
        float_array = DataType(float_.arrayType())
        single_array = float_array
        float32_array = float_array
        double_array = DataType(double.arrayType())
        float64_array = double_array
        string_array = DataType(string.arrayType())

        _type2jtype = {
            bool_ : jpy.get_type('java.lang.Boolean'),
            byte : jpy.get_type('byte'),
            short : jpy.get_type('short'),
            int_ : jpy.get_type('int'),
            long_ : jpy.get_type('long'),
            float_ : jpy.get_type('float'),
            double : jpy.get_type('double'),
            string : jpy.get_type('java.lang.String'),
            bigdecimal : jpy.get_type('java.math.BigDecimal'),
            biginteger : jpy.get_type('java.math.BigInteger'),
            stringset : jpy.get_type('io.deephaven.stringset.StringSet'),
            datetime : jpy.get_type('io.deephaven.time.DateTime'),
            timeperiod : jpy.get_type('io.deephaven.time.Period'),
            byte_array : jpy.get_type('[B'),
            short_array : jpy.get_type('[S'),
            int_array : jpy.get_type('[I'),
            long_array : jpy.get_type('[J'),
            float_array : jpy.get_type('[S'),
            double_array : jpy.get_type('[D'),
            string_array : jpy.get_type('[Ljava.lang.String;')
        }


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
def _jclassFromType(data_type : DataType):
    if data_type is None:
        return None
    try:
        jclass = data_type.clazz()
    except Exception as e:
        raise Exception("Could not get java class type from " + str(data_type)) from e
    return jclass


@_passThrough
def _jpyTypeFromType(data_type : DataType):
    if data_type is None:
        return None
    jpy_type = _type2jtype.get(data_type, None)
    if jpy_type is not None:
        return jpy_type
    jclass = _jclassFromType(data_type)
    return jpy.get_type(jclass.getName())


@_passThrough
def _isPrimitive(data_type : DataType):
    primitives = { byte, short, char, int_, long_, float_, double }
    return data_type in primitives

@_passThrough
def _colDef(col_name : str, data_type : DataType, component_type : DataType = None):
    """
    Create a ColumnDefinition object.
    :param col_name: The column's new.
    :param data_type: The column's data type.
    :param component_type: The column's component type, or None if none.
    :return: the column definition object.
    """
    jdata_type = _jclassFromType(data_type)
    jcomponent_type = _jclassFromType(component_type)

    return _col_def_.fromGenericType(col_name, jdata_type, jcomponent_type)

@_passThrough
def _colDefs(ts):
    """
    Convert a sequence of tuples of the form ('Price', double_type)
    or ('Prices', double_array_type, double_type)
    to a list of ColumnDefinition objects.

    :param ts: a sequence of 2 or 3 element tuples of (str, type) or (str, type, type)  specifying a column definition object.
    :return: a list of column definition objects.
    """
    r = []
    for t in ts:
        r.append(_colDef(*t))
    return r


@_passThrough
def _getQstCol(col_name:str, col_type:DataType, col_data=None):
    if col_data is None or len(col_data) < 1:
        col_header = _qst_col_header_.of(col_name, col_type)
        return _qst_column_.empty(col_header)
    jtype = _jpyTypeFromType(col_type)
    if jtype is None:
        raise Exception("value for argument 'col_type' " +
                        str(col_type) + " is not a known data type.")
    if col_type is char and len(col_data) > 0 and isinstance(col_data[0], str):
        col_data = [ ord(x[0]) for x in col_data ]
    jvalues = jpy.array(jtype, col_data)
    if _isPrimitive(col_type):
        return _qst_column_.ofUnsafe(col_name, jvalues)
    else:
        return _qst_column_.of(col_name, col_type, jvalues)
    

@_passThrough
def _getTable(qst_cols):
    qst_newtable = _qst_newtable_.of(qst_cols)
    return _table_factory_.of(qst_newtable)
    
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
        return _table_factory_.of(col_header)

    if isinstance(data, collections.Sequence):
        return _table_by_rows(data, columns)
    elif isinstance(data, dict):
        return _table_by_cols(data)
