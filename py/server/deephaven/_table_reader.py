#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
"""This module supports reading the data in a Deephaven table in a chunked manner."""
from collections import namedtuple
from typing import Union, Sequence, Generator, Dict, Optional, Any, Tuple

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
        if len(col_defs) != len(cols):
            raise ValueError(f"Invalid column names: {set(cols) - {col.name for col in col_defs}}")

    return col_defs

def _table_reader_chunk_dict(table: Table, *, cols: Optional[Union[str, Sequence[str]]] = None, row_set: jpy.JType, chunk_size: int = 4096,
                             prev: bool = False, to_numpy: bool = True) -> Generator[Dict[str, Union[np.ndarray | jpy.JType]], None, None]:
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

    Raises:
        ValueError
    """
    if chunk_size < 0:
        raise ValueError("chunk_size must not be negative.")

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

def _table_reader_dict(table: Table, cols: Optional[Union[str, Sequence[str]]] = None) -> Generator[Dict[str, Any], None, None]:
    """ A generator that reads one row at a time from a table into a dictionary. The dictionary is a map of column names
    to scalar values of the column data type.

    Args:
        table (Table): The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.

    Returns:
        A generator that yields a dictionary of column names to a value.

    Raises:
        ValueError
    """
    col_defs = _col_defs(table, cols)

    for chunk_dict in  _table_reader_chunk_dict(table, cols=cols, row_set=table.j_table.getRowSet(), chunk_size=4096,
                                                prev=False, to_numpy=False):
        chunk_size = len(chunk_dict[col_defs[0].name])
        for i in range(chunk_size):
            col_dict = {}
            for col_def in col_defs:
                col_dict[col_def.name] = chunk_dict[col_def.name][i]
            yield col_dict

def _table_reader_tuple(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, tuple_name: str = 'Deephaven') -> Generator[Tuple[Any, ...], None, None]:
        """ Returns a generator that reads one row at a time from the table into a named tuple. The named tuple is made
        up of fields with their names being the column names and their values being of the column data types.

        If the table is refreshing and no update graph locks are currently being held, the generator will try to acquire
        the shared lock of the update graph before reading the table data. This provides a consistent view of the data.
        The side effect of this is that the table will not be able to refresh while the table is being iterated on.
        Additionally, the generator internally maintains a fill context. The auto acquired shared lock and the fill
        context will be released after the generator is destroyed. That can happen implicitly when the generator
        is used in a for-loop. When the generator is not used in a for-loop, to prevent resource leaks, it must be closed
        after use by either (1) by setting it to None, (2) using the del statement, or (3) calling the close() method on it.

        Args:
            table (Table): The table to read.
            cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read. Default is None.
            tuple_name (str): The name of the named tuple. Default is 'Deephaven'.

        Returns:
            A generator that yields a named tuple for each row in the table

        Raises:
            ValueError
        """
        named_tuple_class = namedtuple(tuple_name, cols or [col.name for col in table.columns], rename=False)

        for row in _table_reader_dict(table, cols):
            yield named_tuple_class(**row)


def _table_reader_chunk_tuple(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, *, chunk_size: int = 4096,
                     tuple_name: str = 'Deephaven')-> Generator[Tuple[np.ndarray, ...], None, None]:
    """ Returns a generator that reads one chunk of rows at a time from the table into a named tuple. The named
    tuple is made up of fields with their names being the column names and their values being numpy arrays of the
    column data types.

    If the table is refreshing and no update graph locks are currently being held, the generator will try to acquire
    the shared lock of the update graph before reading the table data. This provides a consistent view of the data.
    The side effect of this is that the table will not be able to refresh while the table is being iterated on.
    Additionally, the generator internally maintains a fill context. The auto acquired shared lock and the fill
    context will be released after the generator is destroyed. That can happen implicitly when the generator
    is used in a for-loop. When the generator is not used in a for-loop, to prevent resource leaks, it must be closed
    after use by either (1) by setting it to None, (2) using the del statement, or (3) calling the close() method on it.

    Args:
        table (Table): The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.
        chunk_size (int): The number of rows to read at a time. Default is 4096.
        tuple_name (str): The name of the named tuple. Default is 'Deephaven'.

    Returns:
        A generator that yields a named tuple for each row in the table.

    Raises:
        ValueError
    """
    named_tuple_class = namedtuple(tuple_name, cols or [col.name for col in table.columns], rename=False)

    for chunk_dict in _table_reader_chunk_dict(table, cols=cols, row_set=table.j_table.getRowSet(), chunk_size=chunk_size,
                                               prev=False, to_numpy=True):
        yield named_tuple_class(**chunk_dict)


