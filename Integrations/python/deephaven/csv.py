#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" The deephaven.csv module supports reading an external CSV file into a Deephaven table and writing a
Deephaven table out as a CSV file.
"""
from enum import Enum
from typing import Dict, Any, List

import jpy
import wrapt

import deephaven.Types as dht

_JCsvHelpers = None
_JTableHeader = None
_JCsvTools = None
_JParsers = None
_JArrays = None


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _JCsvHelpers, _JTableHeader, _JCsvTools, _JParsers, _JArrays

    if _JCsvHelpers is None:
        # This will raise an exception if the desired object is not the classpath
        _JCsvHelpers = jpy.get_type("io.deephaven.csv.CsvTools")
        _JTableHeader = jpy.get_type("io.deephaven.qst.table.TableHeader")
        _JCsvTools = jpy.get_type("io.deephaven.csv.CsvTools")
        _JParsers = jpy.get_type("io.deephaven.csv.parsers.Parsers")
        _JArrays = jpy.get_type("java.util.Arrays")


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


@_passThrough
def read(path: str,
         header: Dict[str, dht.DataType] = None,
         headless: bool = False,
         delimiter: str = ",",
         quote: str = "\"",
         ignore_surrounding_spaces: bool = True,
         trim: bool = False,
         charset: str = "utf-8") -> object:
    """ Read the CSV data specified by the path parameter as a table.

    Args:
        path (str): a file path or a URL string
        header (Dict[str, DataType]): a dict to define the table columns with key being the name, value being the data type
        headless (bool): indicates if the CSV data is headless, default is False
        delimiter (str): the delimiter used by the CSV, default is the comma
        quote (str): the quote character for the CSV, default is double quote
        ignore_surrounding_spaces (bool): indicates whether surrounding white space should be ignored for unquoted text
            fields, default is True
        trim (bool) : indicates whether to trim white space inside a quoted string, default is False
        charset (str): the name of the charset used for the CSV data, default is 'utf-8'

    Returns:
        a table

    Raises:
        Exception
    """

    csv_specs_builder = _JCsvTools.builder()

    if header:
        csv_specs_builder.headers(_JArrays.asList(list(header.keys())))
        parser_map = {
            dht.bool_ : _JParsers.BOOLEAN,
            dht.byte : _JParsers.BYTE,
            dht.char : _JParsers.CHAR,
            dht.short : _JParsers.SHORT,
            dht.int_ : _JParsers.INT,
            dht.long_ : _JParsers.LONG,
            dht.float_ : _JParsers.FLOAT_FAST,
            dht.double : _JParsers.DOUBLE,
            dht.string : _JParsers.STRING,
            dht.datetime : _JParsers.DATETIME
        }
        for column_name, column_type in header.items():
            csv_specs_builder.putParserForName(column_name, parser_map[column_type])

    csv_specs = (csv_specs_builder
                 .hasHeaderRow(not headless)
                 .delimiter(ord(delimiter))
                 .quote(ord(quote))
                 .ignoreSurroundingSpaces(ignore_surrounding_spaces)
                 .trim(trim)
                 .build())

    return _JCsvHelpers.readCsv(path, csv_specs)


@_passThrough
def write(table: object, path: str, cols: List[str] = []) -> None:
    """ Write a table to a standard CSV file.

    Args:
        table (Table): the source table
        path (str): the path of the CSV file
        cols (List[str]): the names of the columns to be written out

    Raises:
        Exception
    """
    _JCsvTools.writeCsv(table, False, path, *cols)
