#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
"""
Utilities for converting java objects to python object, or vice versa.
"""

import re
import jpy
import wrapt
import sys
import numpy
import pandas
import logging
from datetime import datetime, date
from calendar import timegm  # for datetime -> timestamp conversion


if sys.version_info[0] > 2:
    def _isStr(input):
        return isinstance(input, str)
else:
    def _isStr(input):
        return isinstance(input, basestring)

# None until the first _defineSymbols() call
_table_tools_ = None
_col_def_ = None
_jprops_ = None
_jmap_ = None
_python_tools_ = None
IDENTITY = None

def _defineSymbols():
    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _table_tools_, _col_def_, _jprops_, _jmap_, _python_tools_, IDENTITY
    if _table_tools_ is None:
        # This will raise an exception if the desired object is not the classpath
        _table_tools_ = jpy.get_type("io.deephaven.db.tables.utils.TableTools")
        _col_def_ = jpy.get_type("io.deephaven.db.tables.ColumnDefinition")
        _jprops_ = jpy.get_type("java.util.Properties")
        _jmap_ = jpy.get_type("java.util.HashMap")
        _python_tools_ = jpy.get_type("io.deephaven.integrations.python.PythonTools")
        IDENTITY = object()  # Ensure IDENTITY is unique.


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

__ObjectColumnSource__ = 'io.deephaven.db.v2.sources.immutable.ImmutableObjectArraySource'
__DatetimeColumnSource__ = 'io.deephaven.db.v2.sources.immutable.ImmutableDateTimeArraySource'
__ArrayConversionUtility__ = 'io.deephaven.integrations.common.PrimitiveArrayConversionUtility'

NULL_CHAR = 65534                                         #: Null value for char.
NULL_FLOAT = float.fromhex('-0x1.fffffep127')             #: Null value for float.
NULL_DOUBLE = float.fromhex('-0x1.fffffffffffffP+1023')   #: Null value for double.
NULL_SHORT = -32768                                       #: Null value for short.
NULL_INT = -0x80000000                                    #: Null value for int.
NULL_LONG = -0x8000000000000000                           #: Null value for long.
NULL_BYTE = -128                                          #: Null value for byte.


# contains appropriate values for bidirectional conversion of null values
_nullValues = {
    # java version
    'char': NULL_CHAR,
    'float':  NULL_FLOAT,
    'double': NULL_DOUBLE,
    'short':  NULL_SHORT,
    'int': NULL_INT,
    'long':  NULL_LONG,
    'byte': NULL_BYTE,
    # numpy version
    'int8': NULL_BYTE,
    'int16': NULL_SHORT,
    'int32': NULL_INT,
    'int64': NULL_LONG,
    'float32': NULL_FLOAT,
    'float64': NULL_DOUBLE,
}


_dtypeToJavaDirect = {
    'bool': 'java.lang.Boolean',
    'int8': 'byte',
    'int16': 'short',
    'int32': 'int',
    'int64': 'long',
    'float32': 'float',
    'float64': 'double',
}

_basicArrayTypes = {
    'byte': 'B',
    'short': 'S',
    'int': 'I',
    'long': 'J',
    'float': 'F',
    'double': 'D',
    'char': 'C',
}

_boxedArrayTypes = {
    'byte': 'java.lang.Byte',
    'short': 'java.lang.Short',
    'int': 'java.lang.Integer',
    'long': 'java.lang.Long',
    'float': 'java.lang.Float',
    'double': 'java.lang.Double',
    'char': 'java.lang.Character',
}


_javaTypeToDbarrayType = {
    'java.lang.String': 'io.deephaven.db.tables.dbarrays.DbArrayDirect',
    'char': 'io.deephaven.db.tables.dbarrays.DbCharArrayDirect',
    'java.lang.Boolean': 'io.deephaven.db.tables.dbarrays.DbArrayDirect',
    'boolean': 'io.deephaven.db.tables.dbarrays.DbArrayDirect',  # it really should be boxed...
    'byte': 'io.deephaven.db.tables.dbarrays.DbByteArrayDirect',
    'short': 'io.deephaven.db.tables.dbarrays.DbShortArrayDirect',
    'int': 'io.deephaven.db.tables.dbarrays.DbIntArrayDirect',
    'long': 'io.deephaven.db.tables.dbarrays.DbLongArrayDirect',
    'float': 'io.deephaven.db.tables.dbarrays.DbFloatArrayDirect',
    'double': 'io.deephaven.db.tables.dbarrays.DbDoubleArrayDirect',
}

_javaTypeToImmutableColumnSource = {
    'byte': 'io.deephaven.db.v2.sources.immutable.ImmutableByteArraySource',
    'short': 'io.deephaven.db.v2.sources.immutable.ImmutableShortArraySource',
    'int': 'io.deephaven.db.v2.sources.immutable.ImmutableIntArraySource',
    'long': 'io.deephaven.db.v2.sources.immutable.ImmutableLongArraySource',
    'float': 'io.deephaven.db.v2.sources.immutable.ImmutableFloatArraySource',
    'double': 'io.deephaven.db.v2.sources.immutable.ImmutableDoubleArraySource',
    'char': 'io.deephaven.db.v2.sources.immutable.ImmutableCharArraySource',
}

###########################################################################################################
#  python to java methods
###########################################################################################################


def getJavaClassObject(classString):
    """
    Gets a java.lang.Class instance corresponding to classString. This is primarily intended as a helper for calling
    java methods which require setting a type.

    :param classString: either a primitive (byte, short, int, long, float, double, char) or a fully qualified java path
    :return: java.lang.Class instance
    """

    if classString in _boxedArrayTypes:
        return jpy.get_type(_boxedArrayTypes[classString]).TYPE
    else:
        return jpy.get_type(classString).jclass


def convertToJavaArray(input, boxed=False):
    """
    Convert the input list/tuple or numpy array to a java array. The main utility of the method is likely
    providing an object required for a specific java function signature. This is a helper method unifying
    hung on top of :func:`makeJavaArray` and ultimately defined via the proper `jpy.array` call.

    A string will be treated as a character array (i.e. java char array). The user has most control over type
    in providing a :class:`numpy.ndarray`, otherwise the contents will be converted to a :class:`numpy.ndarray`
    first.

    **numpy.ndarray Type Conversion:**

    * basic numpy primitive dtype are converted to their java analog, ``NaN`` values in floating point columns are
      converted to their respective Deephaven NULL constant values.
    * dtype ``datatime64[*]`` are converted to ``DBDateTime``
    * dtype of one of the basic string type *(unicode\*, str\*, bytes\*):
        - if all elements are one character long: converted to ``char`` array
        - otherwise, ``String`` array
    * ndarrays of dtype ``numpy.object``:

        - ndarrays which are empty or all elements are null are converted to java type ``Object``.
        - Otherwise, the first non-null value is used to determine the type for the column.

      If the first non-null element is an instance of:

        - ``bool`` - converted to ``Boolean`` array with null values preserved
        - ``str`` - converted to ``String`` array with null values as empty string
        - ``datetime.date`` or ``datetime.datetime`` - the array is converted to ``DBDateTime``
        - ``numpy.ndarray`` - converted to java array. All elements are assumed null, or ndarray of the same type and
          compatible shape, or an exception will be raised.
        - ``dict`` - **unsupported**
        - ``other iterable type`` - naive conversion to :class:`numpy.ndarray` is attempted, then as above.
        - any other type:

            + ``convertUnknownToString=True`` - attempt to naively convert to ``String`` array
            + otherwise, raise exception

    * ndarrays of any other dtype (namely *complex\*, uint\*, void\*,* or custom dtypes):

        1. ``convertUnknownToString=True`` - attempt to convert to column of string type
        2. otherwise, raise exception

    :param input: string, tuple/list or :class:`numpy.ndarray` - java type will be inferred
    :param boxed: should we ensure that the constructed array is of boxed type?
    :return: a java array instance
    """

    if _isJavaArray(input):
        if boxed:
            return _ensureBoxedArray(input)
        return input
    elif isinstance(input, numpy.ndarray):
        arr = makeJavaArray(input, 'unknown', convertUnknownToString=False)
        if boxed:
            return _ensureBoxedArray(arr)
        return arr
    elif isinstance(input, list) or isinstance(input, tuple):
        ndarray = numpy.asarray(input)
        return convertToJavaArray(ndarray, boxed=boxed)
    elif _isStr(input):  # treat it as a character array
        ndarray = numpy.asarray([ch for ch in input])
        return convertToJavaArray(ndarray, boxed=boxed)
    else:
        raise ValueError("Expect input an instance of numpy.ndarray, list, or tuple, and got {}".format(type(input)))


def convertToJavaList(input):
    """
    Convert the input list/tuple or numpy array to a (fixed size) java List. The main utility of the method is likely
    providing an object required for a specific java function signature.

    :param input: tuple/list or :class:`numpy.ndarray` - java type will be inferred
    :return: best :class:`java.util.List`

    .. note::  The user has most control over type in providing a :class:`numpy.ndarray`, otherwise the
        contents will be converted to a :class:`numpy.ndarray` first. Type mapping will be determined as in
        :func:`makeJavaArray`
    """

    jarray = convertToJavaArray(input, boxed=True)  # it's got to be boxed, or jpy will mangle the matching
    return jpy.get_type('java.util.Arrays').asList(jarray)


def convertToJavaArrayList(input):
    """
    Convert the input list/tuple or numpy array to a java ArrayList. The main utility of the method is likely
    providing an object required for a specific java function signature.

    :param input: tuple/list or :class:`numpy.ndarray` - java type will be inferred
    :return: best :class:`java.util.ArrayList`

    .. note::  The user has most control over type in providing a :class:`numpy.ndarray`, otherwise the
        contents will be converted to a :class:`numpy.ndarray` first. Type mapping will be determined as in
        :func:`convertToJavaArray`
    """

    javaList = convertToJavaList(input)
    return jpy.get_type('java.util.ArrayList')(javaList)


def convertToJavaHashSet(input):
    """
    Convert the input list/tuple or numpy array to a java HashSet. The main utility of the method is likely
    for quick inclusion check inside of a deephaven query.

    :param input: tuple/list or :class:`numpy.ndarray` - java type will be inferred
    :return: best :class:`java.util.ArrayList`

    .. note::  The user has most control over type in providing a :class:`numpy.ndarray`, otherwise the
        contents will be converted to a :class:`numpy.ndarray` first. Type mapping will be determined as in
        :func:`convertToJavaArray`
    """

    javaList = convertToJavaList(input)
    return jpy.get_type('java.util.HashSet')(javaList)


def convertToJavaHashMap(input1, input2=None):
    """
    Create a java hashmap from provided input of the form (input1=keys, input2=values) or (input1=dict).

    :param input1: dict or tuple/list/:class:`numpy.ndarray`, assumed to be keys
    :param input2: ignored if `input1` is a dict, otherwise tuple/list/:class:`numpy.ndarray`, assumed to be values
    :return: best :class:`java.util.HashMap`

    .. note::  The user has most control over type in providing keys and values as :class:`numpy.ndarray`. Otherwise, the
        keys and values will be converted to a :class:`numpy.ndarray` first. Type mapping will be determined as in
        :func:`convertToJavaArray`
    """

    if isinstance(input1, dict):
        keys = convertToJavaArray(list(input1.keys()))
        values = convertToJavaArray(list(input1.values()))
    elif (isinstance(input1, list) or isinstance(input1, tuple) or isinstance(input1, numpy.ndarray) or _isJavaArray(input1)) and \
            (isinstance(input2, list) or isinstance(input2, tuple) or isinstance(input2, numpy.ndarray) or _isJavaArray(input2)):
        if len(input1) == len(input2):
            keys = convertToJavaArray(input1)
            values = convertToJavaArray(input2)
        else:
            raise ValueError("Got iterables input1 of length {} and input2 of length {}. "
                             "Must be the same length.".format(len(input1), len(input2)))
    else:
        raise ValueError("Expected input1 to be a dict, or input1 and input2 to be "
                         "lists/tuples/numpy.ndarrays/java arrays of the same length. "
                         "Got input1 of type {} and input2 of type {}".format(type(input1), type(input2)))

    java_map = jpy.get_type("java.util.HashMap")()
    for key, val in zip(keys, values):
        java_map.put(key, val)
    return java_map


def _isStringDtype(dtypename):
    return dtypename.startswith('unicode') or dtypename.startswith('str') or dtypename.startswith('bytes')


def _getFullTypeName(obj):
    if sys.version_info[0] < 3:
        instances = re.findall("<type '(.*?)'>", str(type(obj)))
    else:
        instances = re.findall("<class '(.*?)'>", str(type(obj)))
    if len(instances) > 0:
        return instances[0]
    else:
        return None


def _isJavaType(obj, returnName=False):
    className = _getFullTypeName(obj)
    value = (className is not None) and (className in jpy.types)
    if returnName:
        return value, className
    else:
        return value


def _makeString(inp):
    if inp is None:
        return ''
    elif _isStr(inp):
        return inp
    else:
        return str(inp)


def _datetimeToLong(inp):
    if inp is None:
        return NULL_LONG
    elif isinstance(inp, datetime):
        # NB: this will provide the incorrect timestamp for naive datetime NOT IN UTC time
        #     This was the the least error prone way - naive datetimes are garbage, and should always be avoided
        return int(timegm(inp.utctimetuple()))*1000000000 + inp.microsecond*1000
    elif isinstance(inp, date):
        return int(timegm(inp.timetuple()))*1000000000
    else:
        return None


def _booleanColumnSource(array, columnName, typ):
    # Assumed to be called strictly by _convertNdarrayToImmutableSource...no error checking at all performed
    try:
        clsType = 'java.lang.Boolean'
        return __ObjectColumnSource__, _makeJavaArray(array, clsType), jpy.get_type(clsType)(True).getClass()
    except Exception as e:
        logging.warning("Failed to convert column {} to a column of type java.lang.Boolean. "
                        "The first non-null element of the array has type {}.".format(columnName, typ))
        raise e


def _charColumnSource(array, columnName, typ):
    # Assumed to be called strictly by _convertNdarrayToImmutableSource...no error checking at all performed
    colClassType = 'io.deephaven.db.v2.sources.immutable.ImmutableCharArraySource'
    try:
        return colClassType, _makeJavaArray(array, 'char')
    except Exception as e:
        logging.warning("Failed to convert column {} to a column of type char. "
                        "The first non-null element of the array has type {}.".format(columnName, typ))
        raise e


def _stringColumnSource(array, columnName, typ):
    # For Python 2/3 compliance, str() should be aliased to unicode() for this purpose
    try:
        clsType = 'java.lang.String'
        return __ObjectColumnSource__, _makeJavaArray(array, clsType), jpy.get_type(clsType)().getClass()
    except Exception as e:
        logging.warning("Failed to convert column {} to a column of type string. "
                        "The first non-null element of the array has type {}.".format(columnName, typ))
        raise e


def _plainColumnSource(array, columnName, typ):
    # Assumed to be called strictly by _convertNdarrayToImmutableSource...no error checking at all performed
    try:
        clsType = 'java.lang.Object'
        return __ObjectColumnSource__, _makeJavaArray(array, clsType), jpy.get_type(clsType)().getClass()
    except Exception as e:
        logging.warning("Failed to convert column {} to a column of type Object. "
                        "The first non-null element of the array has type {}.".format(columnName, typ))
        raise e


def _arrayColumnSource(array, javaTypeString):
    # Assumed to be called strictly by _convertNdarrayToImmutableSource...no error checking at all performed

    # create a placeholder for our array elements
    data = []

    arrayType = None

    if javaTypeString in _javaTypeToDbarrayType:
        arrayType = _javaTypeToDbarrayType[javaTypeString]
    elif javaTypeString in jpy.dtypes:
        arrayType = 'io.deephaven.db.tables.dbarrays.DbArrayDirect'

    arrayCls = jpy.get_type(arrayType)
    if javaTypeString == 'java.lang.Boolean':
        clsInstance = arrayCls(True).getClass()  # constructor must take an argument...
    else:
        clsInstance = arrayCls().getClass()

    for el in array:
        if el is None:
            data.append(None)
        else:
            javaArray = _makeJavaArray(el, javaTypeString)
            if javaArray is None:
                data.append(None)
            else:
                data.append(arrayCls(javaArray))
    return __ObjectColumnSource__, jpy.array(arrayType,  data), clsInstance


def _failedToConvertArray(data, name, goodElement, convertUnknownToString, msg):
    if convertUnknownToString:
        logging.warning("{} - Converting to String.".format(msg))
        return _stringColumnSource(data, name, type(goodElement))
    else:
        raise ValueError(msg)


def _nestedArrayDetails(data, name):
    shape = None
    dimension = None
    javaTypeString = None

    for el in data:
        if el is None:
            shape = 'bad'
            continue
        if not isinstance(el, numpy.ndarray):
            raise ValueError("The data for {} presented as numpy.ndarray of dtype `object` with first non-null "
                             "element of type numpy.ndarray. Another element of type {} was found. "
                             "This is unsupported.".format(name, type(el)))
        newJavaType = _getJavaTypeFromArray(el)
        if javaTypeString is None:
            javaTypeString = newJavaType
            if javaTypeString is None:
                raise ValueError("The data for {} is a numpy.ndarray of dtype `object` with first non-null "
                                 "element an ndarray of dtype `{}`, and the appropriate java "
                                 "conversion type was not inferred.".format(name, el.dtype.name))
        elif javaTypeString != newJavaType:
            raise ValueError("The data for {} is a numpy.ndarray of of dtype `object` with first non-null "
                             "element of inferred java conversion type `{}`, and subsequent element of inferred "
                             "java conversion type `{}`.".format(name, javaTypeString, newJavaType))

        if shape is None:
            shape = el.shape
            if dimension is None:
                dimension = len(shape)
        elif shape != el.shape:
            shape = 'bad'

        if dimension != len(el.shape):
            dimension = 'bad'
    return dimension, shape, javaTypeString


def _getJavaTypeFromArray(ndarray):
    dtype = ndarray.dtype.name
    if dtype in _dtypeToJavaDirect:
        return _dtypeToJavaDirect[dtype]
    elif _isStringDtype(dtype):
        if ndarray.dtype == numpy.dtype('U1') or ndarray.dtype == numpy.dtype('S1'):
            return 'char'
        else:
            return 'java.lang.String'
    elif dtype.startswith('datetime64'):
        return 'io.deephaven.db.tables.utils.DBDateTime'
    elif dtype == 'object':
        # infer type from the first non-stupid element
        goodElement = None
        for el in ndarray:
            if el is not None:
                goodElement = el
                break

        if goodElement is None:
            # no non-null element, so return a java array of type Object with all null elements
            return 'java.lang.Object'
        elif isinstance(goodElement, bool):
            return 'java.lang.Boolean'
        elif isinstance(goodElement, int):
            return 'java.lang.Long'
        elif isinstance(goodElement, float):
            return 'java.lang.Double'
        elif isinstance(goodElement, datetime) or isinstance(goodElement, date):
            return 'datetime'
        elif _isStr(goodElement):
            return 'java.lang.String'
        elif isinstance(goodElement, numpy.ndarray):
            return 'ndarray'
        elif isinstance(goodElement, list) or isinstance(goodElement, tuple):
            return 'list'
        elif isinstance(goodElement, dict):
            return 'dict'
        else:
            # It's not clear what it is, so let's check if it's a java class
            condition, className = _isJavaType(goodElement, returnName=True)
            if condition:
                # It is a java type, so just leave it alone
                return className
    # it's unsupported...
    return None


def _makeJavaArray(ndarray, javaType, containsArrays=False, depth=None):
    """
    Internal helper method for transforming a one-dimensional numpy.ndarray to a corresponding java array. It is
    assumed that error checking will be performed before this stage

    :param ndarray: numpy.ndarray instance
    :param javaType: desired java type - in keeping with jpy usage
    :param containsArrays: boolean indicating whether ndarray is a 1-d array of dtype `object` whose elemnts are
        themselves arrays
    :param depth: if `containsArrays`, then this must be an integer argument for the depth (dimension) of the output array
    :return: the java array
    """

    # No error checking, this is just to avoid tremendous code duplication
    if ndarray is None:
        return None
    elif not isinstance(ndarray, numpy.ndarray):
        raise ValueError("Cannot convert input of type {}".format(type(ndarray)))
    elif ndarray.size < 1:
        return jpy.array(javaType, 0)  # return an empty array of the proper type
    elif len(ndarray.shape) > 1 or containsArrays:
        # recursively build the array
        if containsArrays:
            dimension = depth-1
        else:
            dimension = len(ndarray.shape)-1

        if javaType in _basicArrayTypes:
            arrayType = '['*dimension + _basicArrayTypes[javaType]
        else:
            arrayType = '['*dimension + 'L' + javaType + ';'
        javaArray = jpy.array(arrayType, [_makeJavaArray(subarray, javaType) for subarray in ndarray])
        return javaArray
    elif javaType == 'char':
        # expecting only dtype = 'S1', 'U1'
        junk = None
        if ndarray.dtype == numpy.dtype('U1') and ndarray.dtype.name in ['unicode32', 'str32', 'string32', 'bytes32']:
            junk = numpy.copy(ndarray)
            junk.dtype = numpy.uint32
            junk = junk.astype(numpy.uint16)
        elif ndarray.dtype == numpy.dtype('S1') and ndarray.dtype.name in ['str8', 'string8', 'bytes8']:
            junk = numpy.copy(ndarray)
            junk.dtype = numpy.uint8
            junk = junk.astype(numpy.uint16)

        if junk is None:
            raise ValueError("Cannot convert ndarray of dtype {} and dtype.name {} "
                             "to type `char`".format(ndarray.dtype, ndarray.dtype.name))
        junk[junk == 0] = NULL_CHAR
        return jpy.array('char', junk)
    elif javaType == 'float':
        newarray = numpy.copy(ndarray)
        newarray[numpy.isnan(newarray)] = NULL_FLOAT  # setting NaN to NULL
        return jpy.array('float', newarray)
    elif javaType == 'double':
        newarray = numpy.copy(ndarray)
        newarray[numpy.isnan(newarray)] = NULL_DOUBLE  # setting NaN to NULL
        return jpy.array('double', newarray)
    elif ndarray.dtype.name.startswith('datetime64'):
        if ndarray.dtype.name != 'datetime64[ns]':
            # convert to proper units (ns), then convert that to long
            longs = jpy.array('long', ndarray.astype('datetime64[ns]').astype('int64'))
        else:
            longs = jpy.array('long', ndarray.astype('int64'))
        if javaType == 'long':
            return longs
        else:
            return jpy.get_type(__ArrayConversionUtility__).translateArrayLongToDBDateTime(longs)
    elif javaType == 'java.lang.Boolean':
        # make corresponding byte version: None -> -1, False -> 0, True -> 1
        byts = numpy.full(ndarray.shape, -1, dtype=numpy.int8)
        numpy.copyto(byts, ndarray, casting='unsafe', where=(ndarray != None))
        # make the java version of the byte array
        jbytes = jpy.array('byte', byts)
        # convert to java boolean array
        return jpy.get_type(__ArrayConversionUtility__).translateArrayByteToBoolean(jbytes)
    elif javaType == 'java.lang.String':
        return jpy.array('java.lang.String', [_makeString(el) for el in ndarray])  # make every element a string
    elif javaType in _basicArrayTypes:
        return jpy.array(javaType, ndarray)
    elif javaType in jpy.types:
        # can we pass it straight-through?
        return jpy.array(javaType, ndarray)
    raise Exception("Conversion of ndarray to type {} failed".format(javaType))


def makeJavaArray(data, name, convertUnknownToString=False):
    """
    Utility function which converts a :class:`numpy.ndarray` to a corresponding java array, where
    appropriate type is inferred.

    **numpy.ndarray Type Conversion:**

    * basic numpy primitive dtype are converted to their java analog, ``NaN`` values in floating point columns are
      converted to their respective Deephaven NULL constant values.
    * dtype ``datatime64[*]`` are converted to ``DBDateTime``
    * dtype of one of the basic string type *(unicode\*, str\*, bytes\*):
        - if all elements are one character long: converted to ``char`` array
        - otherwise, ``String`` array
    * ndarrays of dtype ``numpy.object``:

        - ndarrays which are empty or all elements are null are converted to java type ``Object``.
        - Otherwise, the first non-null value is used to determine the type for the column.

      If the first non-null element is an instance of:

        - ``bool`` - converted to ``Boolean`` array with null values preserved
        - ``str`` - converted to ``String`` array with null values as empty string
        - ``datetime.date`` or ``datetime.datetime`` - the array is converted to ``DBDateTime``
        - ``numpy.ndarray`` - converted to java array. All elements are assumed null, or ndarray of the same type and
          compatible shape, or an exception will be raised.
        - ``dict`` - **unsupported**
        - ``other iterable type`` - naive conversion to :class:`numpy.ndarray` is attempted, then as above.
        - any other type:

            + ``convertUnknownToString=True`` - attempt to naively convert to ``String`` array
            + otherwise, raise exception

    * ndarrays of any other dtype (namely *complex\*, uint\*, void\*,* or custom dtypes):

        1. ``convertUnknownToString=True`` - attempt to convert to column of string type
        2. otherwise, raise exception

    :param data: input :class:`numpy.ndarray`
    :param name: name used to specify source, for logging purposes in case of failure
    :param convertUnknownToString: boolean specifying whether to convert unknown types to string
    :return: java array
    """

    junk = _convertNdarrayToImmutableSource(data, name, convertUnknownToString=convertUnknownToString)
    if junk is None:
        raise ValueError("Conversion failed")
    elif len(junk) in [2, 3]:
        if junk[0] == 'io.deephaven.db.v2.sources.immutable.ImmutableDateTimeArraySource':
            return jpy.get_type(__ArrayConversionUtility__).translateArrayLongToDBDateTime(junk[1])
        else:
            return junk[1]


def _handleCategorical(data, columnName, convertUnknownToString=False):
    # assumed to only be called by _convertNdarrayToImmutableSource
    # fetch the underlying "codes"
    indices = data._get_codes()  # this is the "index" array - integer type of appropriate bit depth
    # fetch the "values"
    values = data.categories.values  # if this isn't a primitive type, then it will very likely be of dtype object

    # actually null values are represented by an index of -1, we should address this, if necessary
    if -1 in indices:
        # I'm fairly sure that dtype of values will either be an appropriate primitive, or object
        if values.dtype.name in _nullValues:
            nullConstant = _nullValues[values.dtype.name]
            # update the values array
            tempValues = numpy.empty((len(values)+1, ), dtype=numpy.object)
            tempValues[:len(values)] = values
            tempValues[-1] = nullConstant
            # update the indices array
            maxVal = indices.max()
            tempIndices = numpy.array(indices, copy=True, dtype=numpy.int32)
            tempIndices[tempIndices < 0] = maxVal+1
        else:
            # should be values.dtype.name == 'object':
            # update the values array
            tempValues = numpy.empty((len(values)+1, ), dtype=numpy.object)
            tempValues[:len(values)] = values
            tempValues[-1] = None
            # update the indices array
            maxVal = indices.max()
            tempIndices = numpy.array(indices, copy=True, dtype=numpy.int32)
            tempIndices[tempIndices < 0] = maxVal+1
        junk = tempValues[tempIndices]
        del tempIndices, tempValues
    else:
        # blast away - just plug the indices into the values and plug away
        junk = values[indices]
    conversion = _convertNdarrayToImmutableSource(junk, columnName, convertUnknownToString=convertUnknownToString)
    del junk  # trying to find a memory leak
    return conversion


def _convertNdarrayToImmutableSource(data, name, convertUnknownToString=False):
    """
    Helper function for converting a :class:`numpy.ndarray` to the appropriate immutable column source.
    Largely intended to be used as part of :class:`pandas.DataFrame` column conversion to column source.

    :param data: input :class:`numpy.ndarray`
    :param name: name used to specify source, for logging purposes in case of failure
    :param convertUnknownToString: boolean specifying whether to convert unknown types to string
    :return: None if not converted, (array source type string, javaArray) if one of the basic/primitive types,
            (immutable object source string, javaArray, Class) for other types
    """

    def doMultidimensional(javaTypeString):
        # for multi-dimensional ndarray
        javaArray = _makeJavaArray(data, javaTypeString)
        return __ObjectColumnSource__, javaArray, javaArray[0].getClass()

    def doCompoundArray(javaTypeString, dimension):
        # for object arrays of ndarrays
        javaArray = _makeJavaArray(data, javaTypeString, containsArrays=True, depth=dimension)
        return __ObjectColumnSource__, javaArray, javaArray[0].getClass()

    if isinstance(data, pandas.Series):
        return _convertNdarrayToImmutableSource(data.values, name, convertUnknownToString=convertUnknownToString)
    if isinstance(data, pandas.Categorical):
        return _handleCategorical(data, name, convertUnknownToString=convertUnknownToString)

    if not isinstance(data, numpy.ndarray):
        raise ValueError("Input is not an instance of numpy.ndarray")

    if data.size < 1:
        # This is an empty array
        return None

    javaType = None
    if len(data.shape) > 1:
        javaType = 'ndarray'

    # any other special cases?
    if javaType is None:
        # infer the output type from the array
        javaType = _getJavaTypeFromArray(data)

    if javaType is None:
        return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                     "The appropriate java type for {} could not be inferred".format(name))
    elif javaType == 'dict':
        return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                     "The data for {} has first non-null of type `dict`, "
                                     "which is unsupported".format(name))
    elif javaType == 'java.lang.Object':
        # should only happen if all elements are None...this is dumb
        return _plainColumnSource(data, name, type(None))
    elif javaType in _javaTypeToImmutableColumnSource:
        columnSource = _javaTypeToImmutableColumnSource[javaType]
        return columnSource, _makeJavaArray(data, javaType)
    elif javaType == 'java.lang.Long':
        columnSource = _javaTypeToImmutableColumnSource['long']
        junk = numpy.full(data.shape, NULL_LONG, dtype=numpy.int64)
        numpy.copyto(junk, data, casting='unsafe', where=(data != None))
        return columnSource, _makeJavaArray(junk, 'long')
    elif javaType == 'java.lang.Double':
        columnSource = _javaTypeToImmutableColumnSource['double']
        junk = numpy.full(data.shape, NULL_DOUBLE, dtype=numpy.float64)
        numpy.copyto(junk, data, casting='unsafe', where=(data != None))  # NB: can't check for the presence of NaN
        return columnSource, _makeJavaArray(junk, 'double')
    elif javaType == 'java.lang.String':
        return _stringColumnSource(data, name, type(data[0]))
    elif javaType == 'java.lang.Boolean':
        return _booleanColumnSource(data, name, type(data[0]))
    elif javaType == 'io.deephaven.db.tables.utils.DBDateTime':
        return __DatetimeColumnSource__, _makeJavaArray(data, 'long')
    elif javaType == 'datetime':
        return __DatetimeColumnSource__, jpy.array('long', [_datetimeToLong(el) for el in data])
    elif javaType == 'list':
        try:
            # Let it barf if this fails...
            junkData = numpy.empty((len(data), ), dtype=numpy.object)
            junkData[:] = [None if el is None else numpy.asarray(el) for el in data]
            return _convertNdarrayToImmutableSource(junkData, name, convertUnknownToString=False)
        except Exception as e:
            return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                         "The data for {} has first non-null element which is an instance "
                                         "of list or tuple, but conversion attempts failed with error: {}.".format(name, e))
    elif javaType == 'ndarray':
        if data.dtype.name == 'object' and len(data.shape) == 1:
            # check if all entries are ndarray with compatible shape and dtype
            try:
                dimension, shape, javaTypeString = _nestedArrayDetails(data, name)
            except ValueError as e:
                return _failedToConvertArray(data, name, data[0], convertUnknownToString, str(e))

            if dimension == 'bad':
                return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                             "The data for {} are nested ndarray of incompatible shape. "
                                             "This is unsupported.".format(name))
            elif javaTypeString == 'ndarray':
                return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                             "The data for {} are nested ndarrays. "
                                             "This is unsupported.".format(name))
            else:
                if dimension == 1:
                    return _arrayColumnSource(data, javaTypeString)
                else:
                    return doCompoundArray(javaTypeString, dimension+1)
        elif data.dtype.name == 'object':
            return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                         "The data for {} is a higher dimensional ndarray of dtype = `object`. "
                                         "This is unsupported.".format(name))
        else:
            # It is directly a multi-dimensional array
            javaTypeString = _getJavaTypeFromArray(data)
            if javaTypeString is None:
                return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                             "The data for {} is a higher dimensional ndarray of dtype = `{}`. "
                                             "The proper java type is not handled "
                                             "in the conversion.".format(name, data.dtype.name))
            return doMultidimensional(javaTypeString)
    else:
        return _failedToConvertArray(data, name, data[0], convertUnknownToString,
                                     "The data for {} was inferred to have appropriate intermediate type `{}`, "
                                     "which was unhandled in the conversion.".format(name, javaType))


###########################################################################################################
#  java to python methods
###########################################################################################################

_arrayTypes = {
    'Z': 'boolean',  # NB: this would generally be encountered as java.lang.Boolean versus primitive
    'B': 'byte',
    'S': 'short',
    'I': 'int',
    'J': 'long',
    'F': 'float',
    'D': 'double',
    'java.lang.String': 'java.lang.String',
    'C': 'char',  # this has no python/numpy analog, and is manually converted to string
}


class NULL_CONVERSION(object):
    """
    Enum class for specifying desired deephaven -> numpy null conversion behavior.
    """

    ERROR = 0  #: **[default]** Raise an exception for integer type *(i.e. byte, short, int, or long)* primitive null values
    PASS = 1  #: Do not interpret integer type primitive nulls - leave as the reserved constant value
    CONVERT = 2  #: Pandas style conversion by casting integer type to floating point type, and replacing null values with ``NaN``
    _valueToString = {0: 'ERROR', 1: 'PASS', 2: 'CONVERT'}  # doc:
    _stringToValue = {'ERROR': 0, 'PASS': 1, 'CONVERT': 2}

    @classmethod
    def validateValue(cls, value):
        """
        Validates input value for the enum. Returns the default value (i.e. ERROR) if validation fails.

        :param value: input value
        :return: best enum version of value
        """

        if value in cls._valueToString:
            return value
        elif isinstance(value, str):
            tval = value.upper()
            return cls._stringToValue.get(tval, cls.ERROR)
        return cls.ERROR


def _isDbArray(obj):
    try:
        classString = obj.getClass().getName()
        cond = classString.startswith('io.deephaven.db.tables.dbarrays.Db') or \
               classString.startswith('io.deephaven.db.v2.dbarrays.Db')
        return cond
    except Exception as e:
        return False


def _isJavaArray(obj):
    try:
        classString = obj.getClass().getName()
        return classString.startswith('[')
    except Exception as e:
        return False


def _parseJavaArrayType(javaArray):
    """
    Get the dimension and type for the given java array

    :param javaArray:
    :return:
    * None if not a java array
    * (dimension, basic type string)
    """

    try:
        classString = javaArray.getClass().getName()
    except Exception as e:
        return None

    if not classString.startswith('['):
        return None

    # is this some mixed travesty of types?
    dim1 = classString.count('[')  # how many array dimensions?
    dim2 = 0  # are they all at the beginning?
    for char in classString:
        if char != '[':
            break
        dim2 += 1

    if not dim1 == dim2:
        # what will I do with this abomination?
        raise ValueError("Unable to determine array details for java array of type - {} - "
                         "and class string - {}".format(type(javaArray), classString))

    dimension = dim1

    # classString looks like `...[<type info>`, where type info is:
    #   - one basic character for primitive type
    #   - L<type>; for anything else
    if len(classString) == dimension + 1:
        basicType = _arrayTypes[classString[-1]]
    elif (len(classString) > dimension+2) and (classString[dimension] == 'L') and (classString[-1] == ';'):
        basicType = classString[dimension+1:-1]
    else:
        raise ValueError("Unable to determine array details for java array of type - {} "
                         "with class string {}".format(type(javaArray), classString))

    return dimension, basicType


def _getJavaArrayDetails(javaArray):
    """
    Get the dimension and type for the given java array, also checks if the array is rectangular.

    :param javaArray:
    :return:
    * None if not a java array or the type is ridiculous
    * (dimension, basic type string, shape), where shape is None if not a rectangular array
    """

    basicTypeInfo = _parseJavaArrayType(javaArray)
    if basicTypeInfo is None:
        return None

    dimension, basicType = basicTypeInfo

    if dimension == 1:
        return dimension, basicType, (len(javaArray),)

    # we have to check the size of every element recursively...this may be slow for higher dimensional arrays
    def checkSize(depth, element):
        out = [len(element), ]
        if len(element) < 1:
            return None
        if depth == dimension-1:
            return out

        shapeSuffix = None
        for el in element:
            thisSuffix = checkSize(depth+1, el)
            if thisSuffix is None:
                # it didn't match further down the recursion, so NOT RECTANGULAR
                return None
            elif shapeSuffix is None:
                # this is our first value
                shapeSuffix = thisSuffix
            elif shapeSuffix != thisSuffix:
                # this value didn't match all previous value, so NOT RECTANGULAR
                return None
        # they were all consistent to this level, so could be rectangular
        # return the trailing end of the possible array shape
        return out.extend(shapeSuffix)

    finalShape = checkSize(0, javaArray)
    if finalShape is not None:
        return dimension, basicType, tuple(finalShape)  # should be tuple (immutable) versus list
    return dimension, basicType, finalShape


def _ensureBoxedArray(javaArray):
    """
    Ensure a one-dimensional primitive array gets boxed. Needed for signature matching efforts.

    :param javaArray: object
    :return: boxed array, if applicable
    """

    basicTypeInfo = _parseJavaArrayType(javaArray)
    if basicTypeInfo is None:
        # It's not an array at all...leave it alone?
        return javaArray

    dimension, basicType = basicTypeInfo
    if dimension != 1:
        # nothing to be done
        return javaArray
    elif basicType in _boxedArrayTypes:
        # let's box it
        return jpy.array(_boxedArrayTypes[basicType], javaArray)
    return javaArray

@_passThrough
def _typeFromName(type_str):
    return _table_tools_.typeFromName(type_str)

@_passThrough
def _tupleToColDef(t):
    """
    Convert a tuple of strings of the form ('Price', 'double')
    or ('Prices', 'double[]', 'double')
    to a ColumnDefinition object.

    :param t: a 2 or 3 element tuple of strings specifying a column definition object
    :return: the column definition object.
    """

    if not isinstance(t, tuple):
        raise Exception('argument ' + t + ' is not a tuple')
    if len(t) < 2 or len(t) > 3:
        raise Exception('Only 2 or 3 element tuples expected, got ' + len(t))
    col_name = t[0]
    if not _isStr(col_name):
        raise Exception('Element at index 0 (' + col_name + ') for column name is not of string type')
    type_name = t[1]
    if not _isStr(type_name):
        raise Exception('Element at index 1 (' + type_name + ') for type name is not of string type')
    type_class = _typeFromName(type_name)
    if len(t) == 2:
        return _col_def_.fromGenericType(col_name, type_class)
    component_type_name = t[2]
    if not _isStr(component_type_name):
        raise Exception('Element at index 2 (' + component_type_name + ') for component type name is not of string type')
    component_type_class = _typeFromName(component_type_name)
    return _col_def_.fromGenericType(col_name, type_class, component_type_class)

@_passThrough
def _tuplesListToColDefsList(ts):
    """
    Convert a list of tuples of strings of the form ('Price', 'double')
    or ('Prices', 'double[]', 'double')
    to a list of ColumnDefinition objects.

    :param ts: a list of 2 or 3 element tuples of strings specifying a column definition object.
    :return: a list of column definition objects.
    """
    if not isinstance(ts, list):
        raise Exception('argument ' + ts + ' is not a list')

    r = []
    for t in ts:
        r.append(_tupleToColDef(t))
    return r

@_passThrough
def _dictToProperties(d):
    r = _jprops_()
    for key, value in d.items():
        if value is None:
            value = ''
        r.setProperty(key, value)
    return r

@_passThrough
def _dictToMap(d):
    r = _jmap_()
    for key, value in d.items():
        if value is None:
            value = ''
        r.put(key, value)
    return r

@_passThrough
def _dictToFun(mapping, default_value):
    mapping = _dictToMap(d)
    if default_value is IDENTITY:
        return _python_tools_.functionFromMapWithIdentityDefaults(m)
    else:
        return _python_tools_.functionfromMapWithDefault(m, default_value)
