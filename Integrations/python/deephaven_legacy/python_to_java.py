#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

"""
Utilities for converting python objects to appropriate Deephaven java objects.
"""

import logging
import jpy
import re
import numpy
import pandas

from .TableTools import newTable
from .conversion_utils import _convertNdarrayToImmutableSource


def _getValidColumnName(prospectiveName):
    # convert to string, and replace all non-alphanumeric characters with a space and strip potential ends off
    strName = re.sub('\W+', ' ', str(prospectiveName)).strip()
    # compress multiple spaces to a single underscore
    return re.sub('\s+', '_', strName)


def _createColumnSource(data, columnName, convertUnknownToString=False):
    """
    Simple wrapper for converting dataframe column to appropriate type of immutable column source

    :param data: list/tuple/numpy.ndarray/pandas.Series or pandas.Categorical
    :param columnName: name of desired column
    :param convertUnknownToString: if the conversion is not successful, convert column to string type
        (i.e. explicitly call str())
    :return: None - conversion failed, or appropriate Immutable column source type
    """

    if isinstance(data, numpy.ndarray) or isinstance(data, pandas.Categorical):
        conversion = _convertNdarrayToImmutableSource(data, columnName, convertUnknownToString=convertUnknownToString)
    elif isinstance(data, pandas.Series):
        conversion = _convertNdarrayToImmutableSource(data.values, columnName,
                                                      convertUnknownToString=convertUnknownToString)
    elif isinstance(data, tuple) or isinstance(data, list):
        conversion = _convertNdarrayToImmutableSource(numpy.asarray(data), columnName,
                                                      convertUnknownToString=convertUnknownToString)
    else:
        raise ValueError("The data underlying column {} is of type {}, and only data which is an "
                         "instance of tuple, list, numpy.ndarray, pandas.Series, or a pandas.Categorical "
                         "is eligible for conversion to a table column".format(columnName, type(data)))

    if conversion is None:
        return None
    if len(conversion) == 2:
        # specified/primitive column with return value of form (<column class type>, javaArray)
        return jpy.get_type(conversion[0])(conversion[1])
    if len(conversion) == 3:
        # object type column with return value of form (<column class type>, javaArray, Class)
        return jpy.get_type(conversion[0])(conversion[2], None, conversion[1])
    else:
        # I have no idea what happened
        return None


def dataFrameToTable(dataframe, convertUnknownToString=False):
    """
    Converts the provided :class:`pandas.DataFrame` object to a deephaven table object.

    :param dataframe: :class:`pandas.DataFrame` object
    :param convertUnknownToString: option for whether to attempt to convert unknown elements to a column of string type
    :return: Table object, which represents `dataframe` as faithfully as possible

    **Type Conversion:**

    * Columns basic primitive type are converted to their java analog, ``NaN`` values in floating point columns are
      converted to their respective Deephaven NULL constant values.
    * Columns of underlying type ``datatime64[*]`` are converted to ``DateTime``
    * Columns of one of the basic string type *(unicode\*, str\*, bytes\*)* are converted to String
    * Columns of type ``numpy.object`` - arrays which are empty or all elements are null are converted to java
      type ``Object``. Otherwise, the first non-null value is used to determine the type for the column.

      If the first non-null element is an instance of:

        - ``bool`` - the array is converted to ``Boolean`` with null values preserved
        - ``str`` - the array is converted to a column of ``String`` type
        - ``datetime.date`` or ``datetime.datetime`` - the array is converted to ``DateTime``
        - ``numpy.ndarray`` - all elements are assumed null, or ndarray of the same type and compatible shape, or an
          exception will be raised.

            + if one-dimensional, then column of appropriate ``Vector`` type
            + otherwise, column of java array type
        - ``dict`` - **unsupported**
        - ``other iterable type`` - naive conversion to :class:`numpy.ndarray` is attempted, then as above.
        - any other type:

            + ``convertUnknownToString=True`` - attempt to convert to column of string type
            + otherwise, raise exception
    * Columns of any other type (namely *complex\*, uint\*, void\*,* or custom dtypes):

        1. ``convertUnknownToString=True`` - attempt to convert to column of string type
        2. otherwise, raise exception
    """

    def extractColumnSource(dataframe, columnName, convertUnknownToString):
        data = dataframe.get(columnName).values
        if not (isinstance(data, numpy.ndarray) or isinstance(data, pandas.Categorical)):
            raise ValueError("The data (series.values) underlying column {} is of type {}, and only data which is an "
                             "instance of numpy.ndarray or a pandas.Categorical is eligible for conversion to a "
                             "table column".format(columnName, type(data)))
        return _createColumnSource(data, columnName, convertUnknownToString=convertUnknownToString)

    if not isinstance(dataframe, pandas.DataFrame):
        raise ValueError("Input must be an instance of pandas.DataFrame, "
                         "but the provided is of type {}".format(type(dataframe)))

    # NB: the column names may be ridiculous for multi-indexed DataFrame, but should work

    columnNames = list(dataframe)
    rowCount = len(dataframe)

    useNames = []
    useSources = []
    for columnName in columnNames:
        # remove non-alphanumeric characters from names
        goodName = _getValidColumnName(columnName)
        columnSrc = extractColumnSource(dataframe, columnName, convertUnknownToString=convertUnknownToString)
        if columnSrc is None:
            logging.warning("Failed at converting column {}, skipping from output table".format(columnName))
        else:
            useNames.append(goodName)
            useSources.append(columnSrc)
    if len(useNames) > 0:
        return newTable(rowCount, useNames, useSources)
    return None


def createTableFromData(data, columns=None, convertUnknownToString=False):
    """
    Create a deephaven table object from a collection of column data

    :param data: a dict of the form {column_name: column_data} or list of the form [column0_data, column1_data, ...]
    :param columns: a list of column names to use
    :param convertUnknownToString: option for whether to attempt to convert unknown elements to a column of string type
    :return: the deephaven table

    If data is a dict and columns is given, then only data corresponding to the names in columns is used. If data is a
    list of column data and columns is not given, then column names will be provided as col_0, col_1, ...

    **Type Conversion:**

    * Columns which are an instance of tuple or list are first naively converted to :class:`numpy.ndarray`
    * Columns basic primitive type are converted to their java analog, ``NaN`` values in floating point columns are
      converted to their respective Deephaven NULL constant values.
    * Columns of underlying type ``datatime64[*]`` are converted to ``DateTime``
    * Columns of one of the basic string type *(unicode\*, str\*, bytes\*)* are converted to String
    * Columns of type ``numpy.object`` - arrays which are empty or all elements are null are converted to java
      type ``Object``. Otherwise, the first non-null value is used to determine the type for the column.

      If the first non-null element is an instance of:

        - ``bool`` - the array is converted to ``Boolean`` with null values preserved
        - ``str`` - the array is converted to a column of ``String`` type
        - ``datetime.date`` or ``datetime.datetime`` - the array is converted to ``DateTime``
        - ``numpy.ndarray`` - all elements are assumed null, or ndarray of the same type and compatible shape, or an
          exception will be raised.

            + if one-dimensional, then column of appropriate ``Vector`` type
            + otherwise, column of java array type
        - ``dict`` - **unsupported**
        - ``other iterable type`` - naive conversion to :class:`numpy.ndarray` is attempted, then as above.
        - any other type:

            + ``convertUnknownToString=True`` - attempt to convert to column of string type
            + otherwise, raise exception
    * Columns of any other type (namely *complex\*, uint\*, void\*,* or custom dtypes):

        1. ``convertUnknownToString=True`` - attempt to convert to column of string type
        2. otherwise, raise exception
    """

    def addSource(columnName, columnSrc, thisLength, rowCount):
        if columnSrc is None:
            logging.warning("Failed at converting column {}, skipping from output table".format(columnName))
            return rowCount
        goodName = _getValidColumnName(columnName)
        useNames.append(goodName)
        useSources.append(columnSrc)
        if rowCount is None:
            return thisLength
        elif rowCount != thisLength:
            logging.warning("Differing row lengths found {}, {}".format(rowCount, thisLength))
            return max(thisLength, rowCount)
        return rowCount

    useNames = []
    useSources = []
    rowCount = None

    if isinstance(data, dict):
        if columns is None:
            columns = list(data.keys())  # column order determined by key iteration order
        for columnName in columns:
            thisLength = len(data[columnName])
            columnSrc = _createColumnSource(data[columnName], columnName, convertUnknownToString=convertUnknownToString)
            rowCount = addSource(columnName, columnSrc, thisLength, rowCount)
    elif isinstance(data, tuple) or isinstance(data, list):
        if columns is None:
            columns = ['col_{}'.format(i) for i in range(len(data))]
        if len(columns) != len(data):
            raise ValueError("The length of data ({}) is not equal to the length of columns ({})".format(len(data),
                                                                                                         len(columns)))
        for columnName, columnData in zip(columns, data):
            thisLength = len(columnData)
            columnSrc = _createColumnSource(columnData, columnName, convertUnknownToString=convertUnknownToString)
            rowCount = addSource(columnName, columnSrc, thisLength, rowCount)
    else:
        raise ValueError("Received unexpected input types data = {} and columns = {}".format(type(data), type(columns)))

    if len(useNames) > 0:
        return newTable(rowCount, useNames, useSources)
    return None
