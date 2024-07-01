#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
"""This module supports reading the data in a Deephaven table in a chunked manner."""

from typing import Union, Sequence, Generator, Dict, Optional, Any

import jpy
import numpy as np
from deephaven import update_graph

from deephaven.column import Column
from deephaven.jcompat import to_sequence
from deephaven.numpy import _column_to_numpy_array
from deephaven.table import Table

_JTableUpdateDataReader = jpy.get_type("io.deephaven.integrations.python.PythonTableDataReader")

def _col_defs(table: Table, cols: Union[str, Sequence[str]]) -> Sequence[Column]:
    if not cols:
        col_defs = table.columns
    else:
        cols = to_sequence(cols)
        col_defs = [col for col in table.columns if col.name in cols]

    return col_defs

def _table_reader_rows(table: Table, cols: Optional[Union[str, Sequence[str]]]) -> Generator[Dict[str, Any], None, None]:
    """ A generator that reads one row at a time from a table into a dictionary. The dictionary is a map of column names
    to scalar values of the column data type.

    Args:
        table (Table): The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.

    Returns:
        A generator that yields a dictionary of column names to a value.
    """
    col_defs = _col_defs(table, cols)

    for chunk_dict in  _table_reader_chunks(table, cols=cols, row_set=table.j_table.getRowSet(), chunk_size=4096,
                                            prev=False, to_numpy=False):
        chunk_size = len(chunk_dict[col_defs[0].name])
        for i in range(chunk_size):
            col_dict = {}
            for col_def in col_defs:
                col_dict[col_def.name] = chunk_dict[col_def.name][i]
            yield col_dict

def _table_reader_chunks(table: Table, *, cols: Optional[Union[str, Sequence[str]]], row_set: jpy.JType, chunk_size: Optional[int] = 4096,
                         prev: bool = False, to_numpy: bool = True) -> Generator[Dict[str, np.ndarray], None, None]:
    """ A generator that reads the chunks of rows over the given row set of a table into a dictionary. The dictionary is
    a map of column names to numpy arrays.

    Args:
        table (Table):  The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.
        row_set (jpy.JType): The row set to read.
        chunk_size (Optional[int]): The number of rows to read at a time. Default is 4096.
        prev (bool): If True, read the previous values. Default is False.
        to_numpy (bool): If True, convert the column data to numpy arrays. Default is True.

    Returns:
        A generator that yields a dictionary of column names to numpy arrays.
    """
    col_defs = _col_defs(table, cols)

    row_sequence_iterator = row_set.getRowSequenceIterator()
    col_sources = [table.j_table.getColumnSource(col_def.name) for col_def in col_defs]
    j_reader_context = _JTableUpdateDataReader.makeContext(chunk_size, *col_sources)
    with update_graph.auto_locking_ctx(table):
        try:
            while row_sequence_iterator.hasMore():
                chunk_row_set = row_sequence_iterator.getNextRowSequenceWithLength(chunk_size)
                j_array = _JTableUpdateDataReader.readChunkColumnMajor(j_reader_context, chunk_row_set, col_sources, prev)

                col_dict = {}
                for i, col_def in enumerate(col_defs):
                    col_dict[col_def.name] = _column_to_numpy_array(col_def, j_array[i]) if to_numpy else j_array[i]

                yield col_dict
        finally:
            j_reader_context.close()
            row_sequence_iterator.close()

