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

from deephaven.Types import DataType

_JCsvHelpers = None
_JCsvSpecs = None
_JInferenceSpecs = None
_JTableHeader = None
_JCharset = None
_JCsvTools = None


INFERENCE_STRINGS = None
""" The order of parsing: STRING, INSTANT, SHORT, INT, LONG, DOUBLE, BOOL, CHAR, BYTE, FLOAT. 
The parsers after STRING are only relevant when a specific column data type is given.
"""

INFERENCE_MINIMAL = None
""" The order of parsing: INSTANT, LONG, DOUBLE, BOOL, STRING, BYTE, SHORT, INT, FLOAT, CHAR.
The parsers after STRING are only relevant when a specific column data type is given.
"""

INFERENCE_STANDARD = None
""" The order of parsing: INSTANT, SHORT, INT, LONG, DOUBLE, BOOL, CHAR, STRING, BYTE, FLOAT.
The parsers after STRING are only relevant when a specific column data type is given.
"""

INFERENCE_STANDARD_TIMES = None
""" The order of parsing: INSTANT, INSTANT_LEGACY, SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS, SHORT, INT, 
LONG, DOUBLE, BOOL, CHAR, STRING, BYTE, FLOAT.
 
For values that can be parsed as SECONDS/MILLISECONDS/MICROSECONDS/NANOSECONDS, they must be within the 21 century.

The parsers after STRING are only relevant when a specific column data type is given.
"""


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _JCsvHelpers, _JCsvSpecs, _JInferenceSpecs, _JTableHeader, _JCharset, _JCsvTools, \
        INFERENCE_STRINGS, INFERENCE_MINIMAL, INFERENCE_STANDARD, INFERENCE_STANDARD_TIMES

    if _JCsvHelpers is None:
        # This will raise an exception if the desired object is not the classpath
        _JCsvHelpers = jpy.get_type("io.deephaven.csv.CsvTools")
        _JCsvSpecs = jpy.get_type("io.deephaven.csv.CsvSpecs")
        _JInferenceSpecs = jpy.get_type("io.deephaven.csv.InferenceSpecs")
        _JTableHeader = jpy.get_type("io.deephaven.qst.table.TableHeader")
        _JCharset = jpy.get_type("java.nio.charset.Charset")
        _JCsvTools = jpy.get_type("io.deephaven.csv.CsvTools")

        INFERENCE_STRINGS = _JInferenceSpecs.strings()
        INFERENCE_MINIMAL = _JInferenceSpecs.minimal()
        INFERENCE_STANDARD = _JInferenceSpecs.standard()
        INFERENCE_STANDARD_TIMES = _JInferenceSpecs.standardTimes()

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
def _build_header(header: Dict[str, DataType] = None):
    if not header:
        return None

    table_header_builder = _JTableHeader.builder()
    for k, v in header.items():
        table_header_builder.putHeaders(k, v)

    return table_header_builder.build()


@_passThrough
def read(path: str,
         header: Dict[str, DataType] = None,
         inference: Any = None,
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
        inference (csv.Inference): an Enum value specifying the rules for data type inference, default is INFERENCE_STANDARD_TIMES
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

    if inference is None:
        inference = INFERENCE_STANDARD_TIMES

    try:
        csv_specs_builder = _JCsvSpecs.builder()

        # build the head spec
        table_header = _build_header(header)
        if table_header:
            csv_specs_builder.header(table_header)

        csv_specs = (csv_specs_builder.inference(inference)
                     .hasHeaderRow(not headless)
                     .delimiter(ord(delimiter))
                     .quote(ord(quote))
                     .ignoreSurroundingSpaces(ignore_surrounding_spaces)
                     .trim(trim)
                     .charset(_JCharset.forName(charset))
                     .build())

        return _JCsvHelpers.readCsv(path, csv_specs)
    except Exception as e:
        raise Exception(e, "read_csv failed") from e


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
    try:
        _JCsvTools.writeCsv(table.j_table, False, path, *cols)
    except Exception as e:
        raise Exception("write csv failed.") from e