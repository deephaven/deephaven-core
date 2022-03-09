#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" The deephaven.csv module supports reading an external CSV file into a Deephaven table and writing a
Deephaven table out as a CSV file.
"""
from typing import Dict, List

import jpy

import deephaven2.dtypes as dht
from deephaven2 import DHError
from deephaven2.table import Table

_JCsvTools = jpy.get_type("io.deephaven.csv.CsvTools")
_JParsers = jpy.get_type("io.deephaven.csv.parsers.Parsers")
_JArrays = jpy.get_type("java.util.Arrays")


def read(
    path: str,
    header: Dict[str, dht.DType] = None,
    headless: bool = False,
    delimiter: str = ",",
    quote: str = '"',
    ignore_surrounding_spaces: bool = True,
    trim: bool = False,
) -> Table:
    """Read the CSV data specified by the path parameter as a table.

    Args:
        path (str): a file path or a URL string
        header (Dict[str, DType]): a dict to define the table columns with key being the name, value being the data type
        headless (bool): indicates if the CSV data is headless, default is False
        delimiter (str): the delimiter used by the CSV, default is the comma
        quote (str): the quote character for the CSV, default is double quote
        ignore_surrounding_spaces (bool): indicates whether surrounding white space should be ignored for unquoted text
            fields, default is True
        trim (bool) : indicates whether to trim white space inside a quoted string, default is False

    Returns:
        a table

    Raises:
        DHError
    """
    try:
        csv_specs_builder = _JCsvTools.builder()

        if header:
            csv_specs_builder.headers(_JArrays.asList(list(header.keys())))
            parser_map = {
                dht.bool_: _JParsers.BOOLEAN,
                dht.byte: _JParsers.BYTE,
                dht.char: _JParsers.CHAR,
                dht.short: _JParsers.SHORT,
                dht.int_: _JParsers.INT,
                dht.long: _JParsers.LONG,
                dht.float_: _JParsers.FLOAT_FAST,
                dht.double: _JParsers.DOUBLE,
                dht.string: _JParsers.STRING,
                dht.DateTime: _JParsers.DATETIME,
            }
            for column_name, column_type in header.items():
                csv_specs_builder.putParserForName(column_name, parser_map[column_type])

        csv_specs = (
            csv_specs_builder.hasHeaderRow(not headless)
            .delimiter(ord(delimiter))
            .quote(ord(quote))
            .ignoreSurroundingSpaces(ignore_surrounding_spaces)
            .trim(trim)
            .build()
        )

        j_table = _JCsvTools.readCsv(path, csv_specs)

        return Table(j_table=j_table)
    except Exception as e:
        raise DHError(e, "read csv failed") from e


def write(table: Table, path: str, cols: List[str] = []) -> None:
    """Write a table to a standard CSV file.

    Args:
        table (Table): the source table
        path (str): the path of the CSV file
        cols (List[str]): the names of the columns to be written out

    Raises:
        DHError
    """
    try:
        _JCsvTools.writeCsv(table.j_table, False, path, *cols)
    except Exception as e:
        raise DHError(message="write csv failed.") from e
