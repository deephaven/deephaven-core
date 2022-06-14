#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module supports reading an external Parquet files into Deephaven tables and writing Deephaven tables out as
Parquet files. """
from dataclasses import dataclass
from typing import List

import jpy

from deephaven import DHError
from deephaven.column import Column
from deephaven.table import Table

_JParquetTools = jpy.get_type("io.deephaven.parquet.table.ParquetTools")
_JFile = jpy.get_type("java.io.File")
_JCompressionCodecName = jpy.get_type("org.apache.parquet.hadoop.metadata.CompressionCodecName")
_JParquetInstructions = jpy.get_type("io.deephaven.parquet.table.ParquetInstructions")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")


@dataclass
class ColumnInstruction:
    """  This class specifies the instructions for reading/writing a Parquet column. """
    column_name: str = None
    parquet_column_name: str = None
    codec_name: str = None
    codec_args: str = None
    use_dictionary: bool = False


def _build_parquet_instructions(col_instructions: List[ColumnInstruction] = None, compression_codec_name: str = None,
                                max_dictionary_keys: int = None, is_legacy_parquet: bool = False,
                                for_read: bool = True):
    if not any([col_instructions, compression_codec_name, max_dictionary_keys, is_legacy_parquet]):
        return None

    builder = _JParquetInstructions.builder()
    if col_instructions is not None:
        for ci in col_instructions:
            if for_read and not ci.parquet_column_name:
                raise ValueError("must specify the parquet column name for read.")
            if not for_read and not ci.column_name:
                raise ValueError("must specify the table column name for write.")

            builder.addColumnNameMapping(ci.parquet_column_name, ci.column_name)
            if ci.column_name:
                if ci.codec_name:
                    builder.addColumnCodec(ci.column_name, ci.codec_name, ci.codec_args)
                builder.useDictionary(ci.column_name, ci.use_dictionary)

    if compression_codec_name:
        builder.setCompressionCodecName(compression_codec_name)

    if max_dictionary_keys is not None:
        builder.setMaximumDictionaryKeys(max_dictionary_keys)

    if is_legacy_parquet:
        builder.setIsLegacyParquet(True)

    return builder.build()


def read(path: str, col_instructions: List[ColumnInstruction] = None, is_legacy_parquet: bool = False) -> Table:
    """ Reads in a table from a single parquet, metadata file, or directory with recognized layout.

    Args:
        path (str): the file or directory to examine
        col_instructions (List[ColumnInstruction]): instructions for customizations while reading
        is_legacy_parquet (bool): if the parquet data is legacy

    Returns:
        a table

    Raises:
        DHError
    """

    try:
        read_instructions = _build_parquet_instructions(col_instructions=col_instructions,
                                                        is_legacy_parquet=is_legacy_parquet,
                                                        for_read=True)

        if read_instructions:
            return Table(j_table=_JParquetTools.readTable(path, read_instructions))
        else:
            return Table(j_table=_JParquetTools.readTable(path))
    except Exception as e:
        raise DHError(e, "failed to read parquet data.") from e


def _j_file_array(paths: List[str]):
    return jpy.array("java.io.File", [_JFile(el) for el in paths])


def delete(path: str) -> None:
    """ Deletes a Parquet table on disk.

    Args:
        path (str): path to delete

    Raises:
        DHError
    """
    try:
        _JParquetTools.deleteTable(_JFile(path))
    except Exception as e:
        raise DHError(e, f"failed to delete a parquet table: {path} on disk.") from e


def write(table: Table, path: str, col_definitions: List[Column] = None,
          col_instructions: List[ColumnInstruction] = None, compression_codec_name: str = None,
          max_dictionary_keys: int = None) -> None:
    """ Write a table to a Parquet file.

    Args:
        table (Table): the source table
        path (str): the destination file path; the file name should end in a ".parquet" extension. If the path
            includes non-existing directories they are created. If there is an error, any intermediate directories
            previously created are removed; note this makes this method unsafe for concurrent use
        col_definitions (List[Column]): the column definitions to use, default is None
        col_instructions (List[ColumnInstruction]): instructions for customizations while writing, default is None
        compression_codec_name (str): the default compression codec to use, if not specified, defaults to SNAPPY
        max_dictionary_keys (int): the maximum dictionary keys allowed, if not specified, defaults to 2^20 (1,048,576)

    Raises:
        DHError
    """
    try:
        write_instructions = _build_parquet_instructions(col_instructions=col_instructions,
                                                         compression_codec_name=compression_codec_name,
                                                         max_dictionary_keys=max_dictionary_keys,
                                                         for_read=False)

        table_definition = None
        if col_definitions is not None:
            table_definition = _JTableDefinition.of([col.j_column_definition for col in col_definitions])

        if table_definition:
            if write_instructions:
                _JParquetTools.writeTable(table.j_table, path, table_definition, write_instructions)
            else:
                _JParquetTools.writeTable(table.j_table, _JFile(path), table_definition)
        else:
            if write_instructions:
                _JParquetTools.writeTable(table.j_table, _JFile(path), write_instructions)
            else:
                _JParquetTools.writeTable(table.j_table, path)
    except Exception as e:
        raise DHError(e, "failed to write to parquet data.") from e


def batch_write(tables: List[Table], paths: List[str], col_definitions: List[Column],
                col_instructions: List[ColumnInstruction] = None, compression_codec_name: str = None,
                max_dictionary_keys: int = None, grouping_cols: List[str] = None):
    """ Writes tables to disk in parquet format to a supplied set of paths.

    If you specify grouping columns, there must already be grouping information for those columns in the sources.
    This can be accomplished with .groupBy(<grouping columns>).ungroup() or .sort(<grouping column>).

    Note that either all the tables are written out successfully or none is.

    Args:
        tables (List[Table]): the source tables
        paths (List[str]): the destinations paths. Any non existing directories in the paths provided are
            created. If there is an error, any intermediate directories previously created are removed; note this makes
            this method unsafe for concurrent use
        col_definitions (List[Column]): the column definitions to use
        col_instructions (List[ColumnInstruction]): instructions for customizations while writing
        compression_codec_name (str): the compression codec to use, if not specified, defaults to SNAPPY
        max_dictionary_keys (int): the maximum dictionary keys allowed, if not specified, defaults to 2^20 (1,048,576)
        grouping_cols (List[str]): the group column names

    Raises:
        DHError
    """
    try:
        write_instructions = _build_parquet_instructions(col_instructions=col_instructions,
                                                         compression_codec_name=compression_codec_name,
                                                         max_dictionary_keys=max_dictionary_keys,
                                                         for_read=False)

        table_definition = _JTableDefinition.of([col.j_column_definition for col in col_definitions])

        if grouping_cols:
            _JParquetTools.writeParquetTables([t.j_table for t in tables], table_definition, write_instructions,
                                              _j_file_array(paths), grouping_cols)
        else:
            _JParquetTools.writeTables([t.j_table for t in tables], table_definition,
                                       _j_file_array(paths))
    except Exception as e:
        raise DHError(e, "write multiple tables to parquet data failed.") from e
