#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Utilities for converting Deephaven java objects to appropriate python objects.
"""

import logging
import jpy

import numpy
import pandas

from .conversion_utils import _nullValues, NULL_BYTE, NULL_CHAR, NULL_CONVERSION, _arrayTypes, \
    _isStr, _isVectorType, _isVector, _isJavaArray, _getJavaArrayDetails
from .TableTools import emptyTable


_javaTypeMap = {
    'float': numpy.float32,
    'double': numpy.float64,
    'short': numpy.int16,
    'int': numpy.int32,
    'long': numpy.int64,
    'byte': numpy.int8,
    'object': numpy.object,
    'boolean': numpy.bool_,
    'java.lang.Boolean': numpy.bool_,
    'java.lang.String': numpy.unicode_,
}


__arrayConversionUtility__ = 'io.deephaven.integrations.common.PrimitiveArrayConversionUtility'


def _fillRectangular(javaArray, shape, basicType, convertNulls):
    """
    Convert the java array a rectangular :class:`numpy.ndarray`. Note that no error checking is performed, and
    it is assumed that the parameters passed in are correct.

    :param javaArray:
    :param shape:
    :param basicType:
    :return: :class:`numpy.ndarray` of appropriate shape and dtype
    """

    def fillValuesIn(dimension, ndElement, arrElement):
        # assumes that shape is in scope...
        if dimension < len(shape)-1:
            # not to the final dimension, so recurse
            for ndSub, arrSub in zip(ndElement, arrElement):
                fillValuesIn(dimension+1, ndSub, arrSub)
        else:
            # at the final dimension, and arrElement is a one dimensional array
            if _isVectorType(basicType):
                # convert each leaf
                for i, leafElement in enumerate(arrElement):
                    ndElement[i] = convertJavaArray(leafElement.toArray(), convertNulls=convertNulls)
            elif basicType == 'io.deephaven.time.DateTime':
                # get long array
                longs = jpy.get_type(__arrayConversionUtility__).translateArrayDateTimeToLong(arrElement)
                ndElement[:] = longs
            elif basicType == 'java.lang.Boolean':
                # create byte array
                byts = jpy.get_type(__arrayConversionUtility__).translateArrayBooleanToByte(arrElement)
                ndElement[:] = byts
            elif basicType in ['C', 'char']:
                # converting as uint32, then fixing after
                ndElement[:] = numpy.asarray(arrElement, dtype=numpy.uint32)
            elif basicType == 'java.lang.String':
                ndElement[:] = ['' if x is None else x for x in arrElement]
            else:
                ndElement[:] = arrElement

    def getStringLength(dimension, arr):
        if arr is None:
            return 0
        if dimension < len(shape):
            obsMax = 0
            for el in arr:
                val = getStringLength(dimension+1, el)
                if obsMax < val:
                    obsMax = val
            return obsMax
        else:
            # it should be a string...
            try:
                return len(arr)
            except Exception as e:
                return 0

    if _isVectorType(basicType):
        out = numpy.empty(shape, dtype=numpy.object)
        fillValuesIn(0, out, javaArray)  # recursively fill
        return out
    elif basicType == 'io.deephaven.time.DateTime':
        out = numpy.empty(shape, dtype='datetime64[ns]')
        fillValuesIn(0, out, javaArray)  # recursively fill
        return out
    elif basicType == 'java.lang.Boolean':
        # NOTE: this has generally been overidden before this point
        out = numpy.empty(shape, dtype=numpy.int8)
        fillValuesIn(0, out, javaArray)  # recursively fill
        return _handleNulls(out, basicType, convertNulls)
    elif basicType in ['C', 'char']:
        out = numpy.empty(shape, dtype=numpy.uint32)
        fillValuesIn(0, out, javaArray)  # recursively fill
        return _handleNulls(out, basicType, convertNulls)
    elif basicType == 'java.lang.String':
        # Need to know the length of longest string to preallocate...go recursion
        slength = getStringLength(0, javaArray)
        out = numpy.empty(shape, dtype=numpy.dtype('U{}'.format(slength)))
        fillValuesIn(0, out, javaArray)  # recursively fill
        return _handleNulls(out, basicType, convertNulls)
    elif basicType in _javaTypeMap:
        out = numpy.empty(shape, dtype=_javaTypeMap[basicType])
        fillValuesIn(0, out, javaArray)  # recursively fill
        return _handleNulls(out, basicType, convertNulls)
    elif basicType in _arrayTypes:
        out = numpy.empty(shape, dtype=_javaTypeMap[_arrayTypes[basicType]])
        fillValuesIn(0, out, javaArray)  # recursively fill
        return _handleNulls(out, basicType, convertNulls)
    # Question: should we explicitly map out what we are going to convert, and return None otherwise? - Tom McCullough
    try:
        out = numpy.empty(shape, dtype=numpy.object)
        fillValuesIn(0, out, javaArray)  # recursively fill
        return out
    except Exception as e:
        return None


def _handleNulls(nparray, javaArrayType, convertNulls):
    # NOTE: no error checking at all
    if nparray is None:
        return None
    elif javaArrayType == 'java.lang.String':
        return nparray
    elif javaArrayType == 'java.lang.Boolean':  # nparray is assumed to be dtype=int8
        if convertNulls == NULL_CONVERSION.PASS:
            return nparray > 0  # any instance of None got converted to False
        if convertNulls == NULL_CONVERSION.ERROR:
            if NULL_BYTE in nparray:
                raise ValueError("The input java.lang.Boolean array contains a null element, and convertNulls=ERROR")
            else:
                return nparray > 0
        if convertNulls == NULL_CONVERSION.CONVERT:
            if NULL_BYTE in nparray:
                newarray = numpy.full(nparray.shape, None, dtype=numpy.object)
                numpy.copyto(newarray, (nparray > 0), casting='unsafe', where=(nparray >= 0))
                return newarray
            else:
                return nparray > 0
    elif javaArrayType in ['C', 'char']:
        if convertNulls == NULL_CONVERSION.PASS:
            nparray.dtype = numpy.dtype('U1')
        elif NULL_CHAR in nparray:
            mask = (nparray == NULL_CHAR)
            nparray.dtype = numpy.dtype('U1')
            nparray[mask] = ''
        else:
            nparray.dtype = numpy.dtype('U1')
        return nparray
    elif javaArrayType in _arrayTypes or javaArrayType in _nullValues:
        if javaArrayType in _arrayTypes:
            primitiveType = _arrayTypes[javaArrayType]
        else:
            primitiveType = javaArrayType
        nullConstant = _nullValues[primitiveType]
        if nullConstant in nparray:
            if nparray.dtype in (numpy.float32, numpy.float64) and (nullConstant in nparray):
                # our array is already the correct dtype, just replace the NULL values with NaN
                nparray[nparray == nullConstant] = numpy.nan
                return nparray
            if nparray.dtype in (numpy.int8, numpy.int16):
                if convertNulls == NULL_CONVERSION.ERROR:
                    raise ValueError("The input java primitive {} array contains a NULL constant, "
                                     "and convertNulls=ERROR".format(primitiveType))
                elif convertNulls == NULL_CONVERSION.CONVERT:
                    newarray = numpy.full(nparray.shape, numpy.nan, dtype=numpy.float32)
                    numpy.copyto(newarray, nparray, casting='unsafe', where=(nparray != nullConstant))
                    # Note that integer -> floating point of same bit depth is not actually a safe cast
                    return newarray
            if nparray.dtype in (numpy.int32, numpy.int64):
                if convertNulls == NULL_CONVERSION.ERROR:
                    raise ValueError("The input java primitive {} array contains a NULL constant, "
                                     "and convertNulls=ERROR".format(primitiveType))
                elif convertNulls == NULL_CONVERSION.CONVERT:
                    newarray = numpy.full(nparray.shape, numpy.nan, dtype=numpy.float64)
                    numpy.copyto(newarray, nparray, casting='unsafe', where=(nparray != nullConstant))
                    # Note that integer -> floating point of same bit depth is not actually a safe cast
                    return newarray
    return nparray


def convertJavaArray(javaArray, convertNulls='ERROR', forPandas=False):
    """
    Converts a java array to it's closest :class:`numpy.ndarray` alternative.

    :param javaArray: input java array or vector object
    :param convertNulls: member of :class:`NULL_CONVERSION` enum, specifying how to treat null values. Can be
      specified by string value (i.e. ``'ERROR'``), enum member (i.e. ``NULL_CONVERSION.PASS``), or integer value
      (i.e. ``2``)
    :param forPandas: boolean indicating whether output will be fed into a pandas.Series, which requires that the
      underlying data is one-dimensional
    :return: :class:`numpy.ndarray` representing as faithful a copy of the java array as possible

    The value for `convertNulls` only applies to java integer type (byte, short, int, long) or java.lang.Boolean
    array types:

    * ``NULL_CONVERSION.ERROR (=0)`` **[default]** inspect for the presence of null values, and raise an exception
      if one is encountered.
    * ``NULL_CONVERSION.PASS (=1)`` do not inspect for the presence of null values, and pass value straight through
      without interpretation (Boolean null -> False). *This is intended for conversion which is as fast as possible.
      No warning is generated if null value(s) present, since no inspection is performed.*
    * ``NULL_CONVERSION.CONVERT (=2)`` inspect for the presence of null values, and take steps to return the closest
      analogous numpy alternative (motivated by pandas behavior):

        - integer type columns with null value(s), the :class:`numpy.ndarray` will have float-point type and null values
          will be replaced with ``NaN``
        - Boolean type columns with null value(s), the :class:`numpy.ndarray` will have ``numpy.object`` type and null
          values will be ``None``.


    Type mapping will be performed as indicated here:

    * ``byte -> numpy.int8``, or ``numpy.float32`` if necessary for null conversion
    * ``short -> numpy.int16``, or ``numpy.float32`` if necessary for null conversion
    * ``int -> numpy.int32``, or ``numpy.float64`` if necessary for null conversion
    * ``long -> numpy.int64``, or ``numpy.float64`` if necessary for null conversion
    * ``Boolean -> numpy.bool``, or ``numpy.object`` if necessary for null conversion
    * ``float -> numpy.float32`` and ``NULL_FLOAT -> numpy.nan``
    * ``double -> numpy.float64`` and ``NULL_DOUBLE -> numpy.nan``
    * ``DateTime -> numpy.dtype(datetime64[ns])`` and ``null -> numpy.nat``
    * ``String -> numpy.unicode_`` (of appropriate length) and ``null -> ''``
    * ``char -> numpy.dtype('U1')`` (one character string) and ``NULL_CHAR -> ''``
    * ``array/Vector``
       - if ``forPandas=False`` and all entries are of compatible shape, then will return a rectangular
         :class:`numpy.ndarray` of dtype in keeping with the above
       - if ``forPandas=False`` or all entries are not of compatible shape, then returns one-diemnsional
         :class:`numpy.ndarray` with dtype ``numpy.object``, with each entry :class:`numpy.ndarray` and type mapping
         in keeping with the above
    * Anything else should present as a one-dimensional array of type ``numpy.object`` with entries uninterpreted
      except by the jpy JNI layer.

    .. note:: The numpy unicode type uses **32-bit** characters (there is no 16-bit option), and is implemented as a
        character array of fixed-length entries, padded as necessary by the null character (i.e. character of integer
        value ``0``). Every entry in the array will actually use as many characters as the longest entry, and the numpy
        fetch of an entry automatically trims the trailing null characters.

        This will require much more memory (doubles bit-depth and pads all strings to the length of the longest) in
        python versus a corresponding java String array. If the original java String has any trailing null (zero-value)
        characters, these will be ignored in python usage. For ``char`` arrays, we cannot differentiate between entries
        whose original value (in java) was ``0`` or ``NULL_CHAR``.
    """

    convertNulls = NULL_CONVERSION.validateValue(convertNulls)

    if javaArray is None:
        return None

    if _isVector(javaArray):
        # convert a engine array to a java array, if necessary
        return convertJavaArray(javaArray.toArray(), convertNulls=convertNulls)

    # is it an array?
    if not _isJavaArray(javaArray):
        return javaArray  # nothing to be done

    # get array details
    details = _getJavaArrayDetails(javaArray)
    if details is None:
        # I don't know what to do
        raise ValueError("Failed to convert array of type {}".format(type(javaArray)))

    dimension, basicType, shape = details

    if len(javaArray) == 0:
        # this is an empty array
        return _fillRectangular(javaArray, (0, ), basicType, convertNulls)
    elif dimension > 1 and forPandas:
        # The output must be one-dimensional, don't inspect here
        out = numpy.empty((len(javaArray), ), dtype=numpy.object)
        # NB: don't recursively call with forPandas=True...
        out[:] = [convertJavaArray(x, convertNulls=convertNulls) for x in javaArray]
        return out
    elif shape is None:
        # it's a ragged multi-dimensional array, so recurse
        out = numpy.empty((len(javaArray), ), dtype=numpy.object)
        out[:] = [convertJavaArray(x, convertNulls=convertNulls) for x in javaArray]
        return out
    else:
        # it's a rectangular array. Fill in as appropriate.
        # _fillRectangular method
        return _fillRectangular(javaArray, shape, basicType, convertNulls)


def freezeTable(table):
    """
    Helper method for freezing a table

    :param table: the deephaven table
    :return: the frozen table
    """

    return emptyTable(0).snapshot(table, True)


def createCategoricalSeries(table, columnName, convertNulls=NULL_CONVERSION.ERROR):
    """
    Produce a copy of the specified column as a :class:`pandas.Series` object containing categorical data.

    :param table: the Table object
    :param columnName: the name of the desired column
    :param convertNulls: member of NULL_CONVERSION enum, specifying how to treat null values. Can be
      specified by string value (i.e. ``'ERROR'``), enum member (i.e. ``NULL_CONVERSION.PASS``), or integer value
      (i.e. ``2``)
    :return: :class:`pandas.Series` object which reproduces the given column of table (as faithfully as possible)

    .. warning:: The table will be frozen prior to conversion. A table which updates mid-conversion would lead to errors
        or other undesirable behavior.
    """

    if table.isRefreshing():
        table = freezeTable(table)

    # construct query determining categorical mapping - what are the pluses and the minuses of this?
    columnId = columnName + "id"
    mappedIdsTable = table.selectDistinct(columnName).update("{}=i".format(columnId))
    tempTable = table.naturalJoin(mappedIdsTable, columnName).renameColumns("{}={}".format(columnId, columnName))
    # first, construct the "values" array - the type will be inferred
    mapping = columnToNumpyArray(mappedIdsTable, columnName, convertNulls=convertNulls)

    # create a dumb intermediate map array
    tempMapping = numpy.arange(len(mapping), dtype=numpy.int64)
    # now, construct the "index" array - *SHOULD* be of type int
    indices = columnToNumpyArray(tempTable, columnId, convertNulls=NULL_CONVERSION.PASS, forPandas=False)

    # TODO: if null is in the categorical set, then those indices should probably be set to -1,
    #       and that element dropped from the set...

    if tempMapping is None or indices is None:
        # TODO: raise an exception here?
        logging.warning("We failed constructing the categorical series for {}. Skipping.".format(columnName))
        return None

    # now, create our pandas categorical with the dumb mapping
    cat = pandas.Categorical(indices, categories=tempMapping, fastpath=True)
    # update to the correct mapping - adjusting th types in the fastest and most sensible way
    cat.categories = mapping  # pandas will probably fiddle with the dtype of this array
    return pandas.Series(data=cat, copy=False)


def columnToNumpyArray(table, columnName, convertNulls=NULL_CONVERSION.ERROR, forPandas=False):
    """
    Produce a copy of the specified column as a :class:`numpy.ndarray`.

    :param table: the Table object
    :param columnName: the name of the desired column
    :param convertNulls: member of NULL_CONVERSION enum, specifying how to treat null values. Can be
      specified by string value (i.e. ``'ERROR'``), enum member (i.e. ``NULL_CONVERSION.PASS``), or integer value
      (i.e. ``2``)
    :param forPandas: boolean for whether the output will be fed into a pandas.Series (i.e. must be 1-dimensional)
    :return: :class:`numpy.ndarray` object which reproduces the given column of table (as faithfully as possible)

    Note that the **entire column** is going to be cloned into memory, so the total number of entries in the column
    should be considered before blindly doing this. For large tables (millions of entries or more?), consider
    measures such as down-selecting rows using the Deephaven query language **before** converting.

    .. warning:: The table will be frozen prior to conversion. A table which updates mid-conversion would lead to errors
        or other undesirable behavior.

    The value for `convertNulls` only applies to java integer type (byte, short, int, long) or java.lang.Boolean
    array types:

    * ``NULL_CONVERSION.ERROR (=0)`` **[default]** inspect for the presence of null values, and raise an exception
      if one is encountered.
    * ``NULL_CONVERSION.PASS (=1)`` do not inspect for the presence of null values, and pass value straight through
      without interpretation (Boolean null -> False). *This is intended for conversion which is as fast as possible.
      No warning is generated if null value(s) present, since no inspection is performed.*
    * ``NULL_CONVERSION.CONVERT (=2)`` inspect for the presence of null values, and take steps to return the closest
      analogous numpy alternative (motivated by pandas behavior):

        - integer type columns with null value(s), the :class:`numpy.ndarray` will have float-point type and null values
          will be replaced with ``NaN``
        - Boolean type columns with null value(s), the :class:`numpy.ndarray` will have ``numpy.object`` type and null
          values will be ``None``.


    Type mapping will be performed as indicated here:

    * ``byte -> numpy.int8``, or ``numpy.float32`` if necessary for null conversion
    * ``short -> numpy.int16``, or ``numpy.float32`` if necessary for null conversion
    * ``int -> numpy.int32``, or ``numpy.float64`` if necessary for null conversion
    * ``long -> numpy.int64``, or ``numpy.float64`` if necessary for null conversion
    * ``Boolean -> numpy.bool``, or ``numpy.object`` if necessary for null conversion
    * ``float -> numpy.float32`` and ``NULL_FLOAT -> numpy.nan``
    * ``double -> numpy.float64`` and ``NULL_DOUBLE -> numpy.nan``
    * ``DateTime -> numpy.dtype(datetime64[ns])`` and ``null -> numpy.nat``
    * ``String -> numpy.unicode_`` (of appropriate length) and ``null -> ''``
    * ``char -> numpy.dtype('U1')`` (one character string) and ``NULL_CHAR -> ''``
    * ``array/Vector``
       - if ``forPandas=False`` and all entries are of compatible shape, then will return a rectangular
         :class:`numpy.ndarray` of dtype in keeping with the above
       - if ``forPandas=False`` or all entries are not of compatible shape, then returns one-diemnsional
         :class:`numpy.ndarray` with dtype ``numpy.object``, with each entry :class:`numpy.ndarray` and type mapping
         in keeping with the above
    * Anything else should present as a one-dimensional array of type ``numpy.object`` with entries uninterpreted
      except by the jpy JNI layer.

    .. note:: The numpy unicode type uses **32-bit** characters (there is no 16-bit option), and is implemented as a
        character array of fixed-length entries, padded as necessary by the null character (i.e. character of integer
        value ``0``). Every entry in the array will actually use as many characters as the longest entry, and the numpy
        fetch of an entry automatically trims the trailing null characters.

        This will require much more memory (doubles bit-depth and pads all strings to the length of the longest) in
        python versus a corresponding java String array. If the original java String has any trailing null (zero-value)
        characters, these will be ignored in python usage. For ``char`` arrays, we cannot differentiate between entries
        whose original value (in java) was ``0`` or ``NULL_CHAR``.

    """

    if table.isRefreshing():
        table = freezeTable(table)

    convertNulls = NULL_CONVERSION.validateValue(convertNulls)

    col = table.getColumn(columnName)
    return convertJavaArray(col.getDirect(), convertNulls=convertNulls, forPandas=forPandas)


def columnToSeries(table, columnName, convertNulls=NULL_CONVERSION.ERROR):
    """
    Produce a copy of the specified column as a :class:`pandas.Series` object.

    :param table: the Table object
    :param columnName: the name of the desired column
    :param convertNulls: member of NULL_CONVERSION enum, specifying how to treat null values. Can be
      specified by string value (i.e. ``'ERROR'``), enum member (i.e. ``NULL_CONVERSION.PASS``), or integer value
      (i.e. ``2``)
    :return: :class:`pandas.Series` object which reproduces the given column of table as faithfully as possible

    Performance for :class:`numpy.ndarray` object is generally much better than :class:`pandas.Series` objects.
    Consider using the :func:`columnToNumpyArray` method, unless you really need a :class:`pandas.Series` object.

    Note that the **entire column** is going to be cloned into memory, so the total number of entries in the column
    should be considered before blindly doing this. For large tables (millions of entries or more?), consider
    measures such as down-selecting rows using the Deephaven query language **before** converting.

    .. warning:: The table will be frozen prior to conversion. A table which updates mid-conversion would lead to errors
        or other undesirable behavior.
    """

    if table.isRefreshing():
        table = freezeTable(table)

    convertNulls = NULL_CONVERSION.validateValue(convertNulls)

    column = table.getColumn(columnName)
    columnType = column.getType().getName()
    # initial numpy array conversion, will be None or 1-dimensional output
    nparray = columnToNumpyArray(table, columnName, convertNulls=convertNulls, forPandas=True)
    if nparray is None:
        return None

    if columnType == 'io.deephaven.time.DateTime':
        # NOTE: I think that we should localize to UTC, and then let the user convert that if they want to...
        #       Note that localizing does not actually effect the underlying numpy array,
        #       but only a pandas construct on top
        return pandas.Series(nparray).dt.tz_localize('UTC')  # .dt.tz_convert(time.tzname[0])
    else:
        return pandas.Series(data=nparray, copy=False)


def tableToDataFrame(table, convertNulls=NULL_CONVERSION.ERROR, categoricals=None):
    """
    Produces a copy of a table object as a :class:`pandas.DataFrame`.

    :param table: the Table object
    :param convertNulls: member of :class:`NULL_CONVERSION` enum, specifying how to treat null values.
    :param categoricals: None, column name, or list of column names to convert a 'categorical' data series
    :return: :class:`pandas.Dataframe` object which reproduces table as faithfully as possible

    Note that the **entire table** is going to be cloned into memory, so the total number of entries in the table
    should be considered before blindly doing this. For large tables (millions of entries or more?), consider
    measures such as dropping unnecessary columns and/or down-selecting rows using the Deephaven query language
    **before** converting.

    .. warning:: The table will be frozen prior to conversion. A table which updates mid-conversion would lead to errors
        or other undesirable behavior.

    The value for `convertNulls` only applies to java integer type (byte, short, int, long) or java.lang.Boolean
    array types:

    * ``NULL_CONVERSION.ERROR (=0)`` **[default]** inspect for the presence of null values, and raise an exception
      if one is encountered.
    * ``NULL_CONVERSION.PASS (=1)`` do not inspect for the presence of null values, and pass value straight through
      without interpretation (Boolean null -> False). *This is intended for conversion which is as fast as possible.
      No warning is generated if null value(s) present, since no inspection is performed.*
    * ``NULL_CONVERSION.CONVERT (=2)`` inspect for the presence of null values, and take steps to return the closest
      analogous numpy alternative (motivated by pandas behavior):

        - integer type columns with null value(s), the :class:`numpy.ndarray` will have float-point type and null values
          will be replaced with ``NaN``
        - Boolean type columns with null value(s), the :class:`numpy.ndarray` will have ``numpy.object`` type and null
          values will be ``None``.


    Conversion for different data types will be performed as indicated here:

    * ``byte -> numpy.int8``, or ``numpy.float32`` if necessary for null conversion
    * ``short -> numpy.int16``, or ``numpy.float32`` if necessary for null conversion
    * ``int -> numpy.int32``, or ``numpy.float64`` if necessary for null conversion
    * ``long -> numpy.int64``, or ``numpy.float64`` if necessary for null conversion
    * ``Boolean -> numpy.bool``, or ``numpy.object`` if necessary for null conversion
    * ``float -> numpy.float32`` and ``NULL_FLOAT -> numpy.nan``
    * ``double -> numpy.float64`` and ``NULL_DOUBLE -> numpy.nan``
    * ``DateTime -> numpy.dtype(datetime64[ns])`` and ``null -> numpy.nat``
    * ``String -> numpy.unicode_`` (of appropriate length) and ``null -> ''``
    * ``char -> numpy.dtype('U1')`` (one character string) and ``NULL_CHAR -> ''``
    * ``array/Vector``
       - if ``forPandas=False`` and all entries are of compatible shape, then will return a rectangular
         :class:`numpy.ndarray` of dtype in keeping with the above
       - if ``forPandas=False`` or all entries are not of compatible shape, then returns one-diemnsional
         :class:`numpy.ndarray` with dtype ``numpy.object``, with each entry :class:`numpy.ndarray` and type mapping
         in keeping with the above
    * Anything else should present as a one-dimensional array of type ``numpy.object`` with entries uninterpreted
      except by the jpy JNI layer.

    .. note:: The numpy unicode type uses **32-bit** characters (there is no 16-bit option), and is implemented as a
        character array of fixed-length entries, padded as necessary by the null character (i.e. character of integer
        value ``0``). Every entry in the array will actually use as many characters as the longest entry, and the numpy
        fetch of an entry automatically trims the trailing null characters.

        This will require much more memory (doubles bit-depth and pads all strings to the length of the longest) in
        python versus a corresponding java String array. If the original java String has any trailing null (zero-value)
        characters, these will be ignored in python usage. For ``char`` arrays, we cannot differentiate between entries
        whose original value (in java) was ``0`` or ``NULL_CHAR``.

    """

    if table.isRefreshing():
        table = freezeTable(table)

    convertNulls = NULL_CONVERSION.validateValue(convertNulls)

    # get the column objects
    columnDefs = table.getDefinition().getColumns()
    # extract the names
    columnNames = [col.getName() for col in columnDefs]
    # NB: map will provide a generator in Python 3, which will fail here...

    categoricalSet = set()
    if categoricals is not None:
        if _isStr(categoricals):
            categoricalSet.add(categoricals)
        else:
            categoricalSet = set(categoricals)
    if not categoricalSet.issubset(columnNames):
        raise KeyError("Categorical set -{}- not contained in column names set -{}".format(categoricals, columnNames))

    data = {}
    useColumnNames = []
    for columnName in columnNames:
        logging.info("Column {} get.....;".format(columnName))
        if columnName in categoricalSet:
            series = createCategoricalSeries(table, columnName, convertNulls=convertNulls)
        else:
            series = columnToSeries(table, columnName, convertNulls=convertNulls)

        if series is None:
            logging.warning("Conversion failed for column {} of "
                            "type {}".format(columnName, table.getColumn(columnName).getType().getName()))
        else:
            useColumnNames.append(columnName)
            data[columnName] = series

    dtype_set = set([v.dtype for k, v in data.items()])
    if len(dtype_set) == 1:
        return pandas.DataFrame(data=numpy.stack([v.array for k, v in data.items()], axis=1), columns=useColumnNames, copy=False)
    else:
        return pandas.DataFrame(data=data, columns=useColumnNames, copy=False)
