#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" The deephaven.csv module supports reading an external CSV file into a Deephaven table and writing a
Deephaven table out as a CSV file.
"""
from typing import Dict, List

import jpy

import deephaven.dtypes as dht
from deephaven import DHError
from deephaven.constants import MAX_LONG
from deephaven.table import Table

_JCsvTools = jpy.get_type("io.deephaven.csv.CsvTools")
_JParsers = jpy.get_type("io.deephaven.csv.parsers.Parsers")
_JArrays = jpy.get_type("java.util.Arrays")


def read(
    path: str,
    header: Dict[str, dht.DType] = None,
    headless: bool = False,
    header_row: int = 0,
    skip_rows: int = 0,
    num_rows: int = MAX_LONG,
    ignore_empty_lines: bool = False,
    allow_missing_columns: bool = False,
    ignore_excess_columns: bool = False,
    delimiter: str = ",",
    quote: str = '"',
    ignore_surrounding_spaces: bool = True,
    trim: bool = False,
) -> Table:
    """Read the CSV data specified by the path parameter as a table.

    Args:
        path (str): a file path or a URL string
        header (Dict[str, DType]): a dict to define the table columns with key being the name, value being the data type
        headless (bool): whether the csv file doesn't have a header row, default is False
        header_row (int): the header row number, all the rows before it will be skipped, default is 0. Must be 0 if
            headless is True, otherwise an exception will be raised
        skip_rows (long): number of data rows to skip before processing data. This is useful when you want to parse
            data in chunks. Defaults to 0
        num_rows (long): max number of rows to process. This is useful when you want to parse data in chunks.
            Defaults to the maximum 64bit integer value
        ignore_empty_lines (bool): whether to ignore empty lines, default is False
        allow_missing_columns (bool): whether the library should allow missing columns in the input. If this flag is
            set, then rows that are too short (that have fewer columns than the header row) will be interpreted as if
            the missing columns contained the empty string. Defaults to false.
        ignore_excess_columns (bool): whether the library should allow excess columns in the input. If this flag is
            set, then rows that are too long (that have more columns than the header row) will have those excess columns
            dropped. Defaults to false.
        delimiter (str): the delimiter used by the CSV, default is the comma
        quote (str): the quote character for the CSV, default is double quote
        ignore_surrounding_spaces (bool): Indicates whether surrounding white space should be ignored for unquoted
            text fields, default is True
        trim (bool): indicates whether to trim white space inside a quoted string, default is False

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
                dht.Instant: _JParsers.DATETIME,
            }
            for column_name, column_type in header.items():
                csv_specs_builder.putParserForName(column_name, parser_map[column_type])

        csv_specs = (
            csv_specs_builder.hasHeaderRow(not headless)
            .skipHeaderRows(header_row)
            .skipRows(skip_rows)
            .numRows(num_rows)
            .ignoreEmptyLines(ignore_empty_lines)
            .allowMissingColumns(allow_missing_columns)
            .ignoreExcessColumns(ignore_excess_columns)
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
