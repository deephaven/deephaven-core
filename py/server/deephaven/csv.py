#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

"""The deephaven.csv module supports reading an external CSV file into a Deephaven table and writing a
Deephaven table out as a CSV file.
"""

from collections.abc import Mapping, Sequence

import jpy

import deephaven.dtypes as dht
from deephaven import DHError
from deephaven.constants import MAX_LONG
from deephaven.dtypes import DType
from deephaven.jcompat import j_lambda
from deephaven.table import Table

_JCsvTools = jpy.get_type("io.deephaven.csv.CsvTools")
_JParsers = jpy.get_type("io.deephaven.csv.parsers.Parsers")
_JArrays = jpy.get_type("java.util.Arrays")
_JColumnNameLegalizer = jpy.get_type("io.deephaven.csv.ColumnNameLegalizer")
_JFunction = jpy.get_type("java.util.function.Function")

_PARSERS = {
    dht.bool_: _JParsers.BOOLEAN,
    dht.byte: _JParsers.BYTE,
    dht.char: _JParsers.CHAR,
    dht.short: _JParsers.SHORT,
    dht.int32: _JParsers.INT,
    dht.long: _JParsers.LONG,
    dht.float32: _JParsers.FLOAT_FAST,
    dht.double: _JParsers.DOUBLE,
    dht.string: _JParsers.STRING,
    dht.Instant: _JParsers.DATETIME,
}


def _check_parser_types(arg_name: str, types: Mapping[str, DType]) -> None:
    """Raises a TypeError naming every column whose data type has no CSV parser."""
    unsupported = [
        f"'{name}': {dtype}" for name, dtype in types.items() if dtype not in _PARSERS
    ]
    if unsupported:
        raise TypeError(
            f"{arg_name} has unsupported data types: {', '.join(unsupported)}"
        )


def _col_types_legalizer(col_types: dict[str, DType]) -> jpy.JType:
    """Returns a CSV header legalizer that checks the col_types keys against the header names, then applies
    Deephaven's column name legalizer.

    The CSV reader calls the legalizer once, with the header names in effect: the file's header row, the names the
    caller supplied, or the synthetic 'Column1', 'Column2', ... of a headless file. The reader matches parsers by these
    names, before legalization, and silently ignores a parser whose name is not among them. Raising here stops the read
    before any data is parsed.
    """

    def legalize(raw_names: jpy.JType) -> jpy.JType:
        legal_names = _JColumnNameLegalizer.INSTANCE.apply(raw_names)
        raw = list(raw_names)
        missing = [name for name in col_types if name not in raw]
        if missing:
            # A key may be the legalized name the table shows, such as 'my_venue' for the header 'my venue'. The
            # reader cannot match it, so point at the header name to use instead.
            raw_by_legal = dict(zip(list(legal_names), raw))
            problems = [
                f"'{name}' (use the header name '{raw_by_legal[name]}')"
                if name in raw_by_legal
                else f"'{name}'"
                for name in missing
            ]
            raise ValueError(
                f"col_types names not found in the CSV header: {', '.join(problems)}. "
                f"The header names are: {raw}"
            )
        return legal_names

    return j_lambda(legalize, _JFunction)


def read(
    path: str,
    header: Sequence[str] | Mapping[str, DType] | None = None,
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
    col_types: dict[str, DType] | None = None,
) -> Table:
    """Read the CSV data specified by the path parameter as a table.

    The column names come from the file's header row, from header, or are 'Column1', 'Column2', ... for a headless
    file. Deephaven turns names that aren't legal column names into legal ones, for example 'my venue' becomes
    'my_venue'. The column types are inferred from the data, unless a header dict or col_types sets them.

    header applies names by position. The first name goes to the first column, the second to the second, and so on.
    The names replace the file's header names and are not matched against them. col_types matches by name, in any
    order, and uses the names before Deephaven makes them legal.

    Args:
        header (Optional[Union[Sequence[str], Mapping[str, DType]]]): the column names, applied by position. The
            number of names sets the number of columns. A row with more cells is an error, unless
            ignore_excess_columns is True. A sequence only renames the columns. A dict also sets each column's
            type, so its keys must be in the file's column order, or the names and types go to the wrong columns.
            To set types by name, use col_types. A dict cannot be combined with col_types. Defaults to None,
            meaning the names come from the file.
        headless (bool): whether the csv file doesn't have a header row, default is False
        header_row (int): the header row number, all the rows before it will be skipped, default is 0. Must be 0 if
            headless is True, otherwise an exception will be raised
        skip_rows (int): number of data rows to skip before processing data. This is useful when you want to parse
            data in chunks. Defaults to 0
        num_rows (int): max number of rows to process. This is useful when you want to parse data in chunks.
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
        col_types (Optional[dict[str, DType]]): the data types of some or all columns, matched by name. A name
            that matches no column raises an error. Defaults to None, meaning the types are inferred, except those
            set by a header dict.

    Returns:
        a table

    Raises:
        DHError
    """
    try:
        if isinstance(header, str):
            raise TypeError(
                "header must be a sequence of column names or a dict, not a str"
            )
        if isinstance(header, Mapping):
            if col_types:
                raise ValueError(
                    "a header dict cannot be combined with col_types; pass header as a sequence of names instead"
                )
            _check_parser_types("header", header)
        if col_types:
            _check_parser_types("col_types", col_types)

        csv_specs_builder = _JCsvTools.builder()

        if isinstance(header, Mapping):
            if header:
                csv_specs_builder.headers(_JArrays.asList(list(header.keys())))
                for column_name, column_type in header.items():
                    csv_specs_builder.putParserForName(
                        column_name, _PARSERS[column_type]
                    )
        elif header:
            csv_specs_builder.headers(_JArrays.asList(list(header)))

        if col_types:
            for column_name, column_type in col_types.items():
                csv_specs_builder.putParserForName(column_name, _PARSERS[column_type])
            csv_specs_builder.headerLegalizer(_col_types_legalizer(col_types))

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


def write(table: Table, path: str, cols: Sequence[str] = []) -> None:
    """Write a table to a standard CSV file.

    Args:
        table (Table): the source table
        path (str): the path of the CSV file
        cols (Sequence[str]): the names of the columns to be written out

    Raises:
        DHError
    """
    try:
        _JCsvTools.writeCsv(table.j_table, False, path, *cols)
    except Exception as e:
        raise DHError(message="write csv failed.") from e
