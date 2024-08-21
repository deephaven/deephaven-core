#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
"""This module supports reading the data in a Deephaven table in a chunked manner."""
from collections import namedtuple
from typing import Union, Sequence, Generator, Dict, Optional, Any, Tuple, Callable, TypeVar, Iterable

import jpy
import numpy as np
from deephaven import update_graph

from deephaven.column import ColumnDefinition
from deephaven.jcompat import to_sequence
from deephaven.numpy import _column_to_numpy_array
from deephaven.table import Table

_JTableUpdateDataReader = jpy.get_type("io.deephaven.integrations.python.PythonTableDataReader")

T = TypeVar('T')

def _col_defs(table: Table, cols: Union[str, Sequence[str]]) -> Sequence[ColumnDefinition]:
    if not cols:
        col_defs = table.columns
    else:
        cols = to_sequence(cols)
        col_defs = [col for col in table.columns if col.name in cols]
        if len(col_defs) != len(cols):
            raise ValueError(f"Invalid column names: {set(cols) - {col.name for col in col_defs} }")

    return col_defs


def _table_reader_all(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, *,
                     emitter: Callable[[Sequence[ColumnDefinition], jpy.JType], T], row_set: jpy.JType,
                     prev: bool = False) -> T:
    """ Reads all the rows in the given row set of a table. The emitter converts the Java data into a desired Python
    object.

    If the table is refreshing and no update graph locks are currently being held, the generator will try to acquire
    the shared lock of the update graph before reading the table data. This provides a consistent view of the data.
    The side effect of this is that the table will not be able to refresh while the table is being iterated on.
    Additionally, the generator internally maintains a fill context. The auto acquired shared lock and the fill
    context will be released after the generator is destroyed. That can happen implicitly when the generator
    is used in a for-loop. When the generator is not used in a for-loop, to prevent resource leaks, it must be closed
    after use by either (1) by setting it to None, (2) using the del statement, or (3) calling the close() method on it.

    Args:
        table (Table):  The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.
        emitter (Callable[[Sequence[Column], jpy.JType], T): The function that takes the column definitions
            and the column data in the form of Java arrays and returns a Python object, usually a collection, such as
            dictionary, tuple.
        row_set (jpy.JType): The row set to read.
        prev (bool): If True, read the previous values. Default is False.

    Returns:
        A Python collection object, usually a dictionary or tuple.

    Raises:
        ValueError
    """
    col_defs = _col_defs(table, cols)

    col_sources = [table.j_table.getColumnSource(col_def.name) for col_def in col_defs]
    j_reader_context = _JTableUpdateDataReader.makeContext(row_set.size(), *col_sources)
    with update_graph.auto_locking_ctx(table):
        try:
            j_array = _JTableUpdateDataReader.readChunkColumnMajor(j_reader_context, row_set, col_sources, prev)
            return emitter(col_defs, j_array)
        finally:
            j_reader_context.close()


def _table_reader_all_dict(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, *, row_set: jpy.JType,
                           prev: bool = False, to_numpy: bool = True) -> Dict[str, Union[np.ndarray, jpy.JType]]:
    """ Reads all the rows in the given row set of a table into a dictionary. The dictionary is a map of column names
    to numpy arrays or Java arrays.

    If the table is refreshing and no update graph locks are currently being held, the generator will try to acquire
    the shared lock of the update graph before reading the table data. This provides a consistent view of the data.
    The side effect of this is that the table will not be able to refresh while the table is being iterated on.
    Additionally, the generator internally maintains a fill context. The auto acquired shared lock and the fill
    context will be released after the generator is destroyed. That can happen implicitly when the generator
    is used in a for-loop. When the generator is not used in a for-loop, to prevent resource leaks, it must be closed
    after use by either (1) by setting it to None, (2) using the del statement, or (3) calling the close() method on it.

    Args:
        table (Table):  The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.
        row_set (jpy.JType): The row set to read.
        prev (bool): If True, read the previous values. Default is False.
        to_numpy (bool): If True, convert the column data to numpy arrays. Default is True.

    Returns:
        A dictionary of column names to numpy arrays or Java arrays.

    Raises:
        ValueError
    """
    _emitter = lambda col_defs, j_array: {col_def.name: _column_to_numpy_array(col_def, j_array[i]) if to_numpy else j_array[i]
            for i, col_def in enumerate(col_defs)}
    return _table_reader_all(table, cols, emitter=_emitter, row_set=row_set, prev=prev)


def _table_reader_chunk(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, *,
                        emitter: Callable[[Sequence[ColumnDefinition], jpy.JType], Iterable[T]], row_set: jpy.JType,
                        chunk_size: int = 2048, prev: bool = False) \
        -> Generator[T, None, None]:
    """ Returns a generator that reads one chunk of rows at a time from the table.  The emitter converts the Java chunk
    into the generator output value.

    If the table is refreshing and no update graph locks are currently being held, the generator will try to acquire
    the shared lock of the update graph before reading the table data. This provides a consistent view of the data.
    The side effect of this is that the table will not be able to refresh while the table is being iterated on.
    Additionally, the generator internally maintains a fill context. The auto acquired shared lock and the fill
    context will be released after the generator is destroyed. That can happen implicitly when the generator
    is used in a for-loop. When the generator is not used in a for-loop, to prevent resource leaks, it must be closed
    after use by either (1) by setting it to None, (2) using the del statement, or (3) calling the close() method on it.

    Args:
        table (Table):  The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.
        emitter (Callable[[Sequence[Column], jpy.JType], Iterable[T]]): The function that takes the column definitions
            and the column data in the form of Java arrays and returns an Iterable.
        row_set (jpy.JType): The row set to read.
        chunk_size (int): The number of rows to read at a time. Default is 2048.
        prev (bool): If True, read the previous values. Default is False.

    Returns:
        A generator that yields the desired Python type of the emitter.

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
                j_array = _JTableUpdateDataReader.readChunkColumnMajor(j_reader_context, chunk_row_set, col_sources,
                                                                       prev)
                yield from emitter(col_defs, j_array)
        finally:
            j_reader_context.close()
            row_sequence_iterator.close()

def _table_reader_chunk_dict(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, *, row_set: jpy.JType,
                                    chunk_size: int = 2048, prev: bool = False) \
        -> Generator[Dict[str, np.ndarray], None, None]:
    """ Returns a generator that reads one chunk of rows at a time from the table into a dictionary. The dictionary is
    a map of column names to numpy arrays or Java arrays.

    If the table is refreshing and no update graph locks are currently being held, the generator will try to acquire
    the shared lock of the update graph before reading the table data. This provides a consistent view of the data.
    The side effect of this is that the table will not be able to refresh while the table is being iterated on.
    Additionally, the generator internally maintains a fill context. The auto acquired shared lock and the fill
    context will be released after the generator is destroyed. That can happen implicitly when the generator
    is used in a for-loop. When the generator is not used in a for-loop, to prevent resource leaks, it must be closed
    after use by either (1) by setting it to None, (2) using the del statement, or (3) calling the close() method on it.

    Args:
        table (Table):  The table to read.
        cols (Optional[Union[str, Sequence[str]]]): The columns to read. If None, all columns are read.
        row_set (jpy.JType): The row set to read.
        chunk_size (int): The number of rows to read at a time. Default is 2048.
        prev (bool): If True, read the previous values. Default is False.

    Returns:
        A generator that yields a dictionary of column names to numpy arrays or Java arrays.

    Raises:
        ValueError
    """
    def _emitter(col_defs: Sequence[ColumnDefinition], j_array: jpy.JType) -> Generator[Dict[str, np.ndarray], None, None]:
        yield {col_def.name: _column_to_numpy_array(col_def, j_array[i]) for i, col_def in enumerate(col_defs)}

    return _table_reader_chunk(table, cols, emitter=_emitter, row_set=row_set, chunk_size=chunk_size, prev=prev)


def _table_reader_chunk_tuple(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, *,
                tuple_name: str = 'Deephaven', chunk_size: int = 2048,) -> Generator[Tuple[np.ndarray, ...], None, None]:
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
        tuple_name (str): The name of the named tuple. Default is 'Deephaven'.
        chunk_size (int): The number of rows to read at a time. Default is 2048.

    Returns:
        A generator that yields a named tuple for each row in the table.

    Raises:
        ValueError
    """
    named_tuple_class = namedtuple(tuple_name, cols or table.column_names, rename=False)

    def _emitter(col_defs: Sequence[ColumnDefinition], j_array: jpy.JType) -> Generator[Tuple[np.ndarray], None, None]:
        yield named_tuple_class._make([_column_to_numpy_array(col_def, j_array[i]) for i, col_def in enumerate(col_defs)])

    return _table_reader_chunk(table, cols, emitter=_emitter, row_set=table.j_table.getRowSet(), chunk_size=chunk_size, prev=False)

def _table_reader_row_dict(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, chunk_size: int = 2048) \
        -> Generator[Dict[str, Any], None, None]:
    """ A generator that reads one row at a time from a table into a dictionary. The dictionary is a map of column names
    to scalar values of the column data type.

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
        chunk_size (int): The number of rows to read at a time internally to reduce the number of Java/Python boundary
            crossings. Default is 2048.

    Returns:
        A generator that yields a dictionary of column names to values.

    Raises:
        ValueError
    """
    def _emitter(col_defs: Sequence[ColumnDefinition], j_array: jpy.JType) -> Iterable[Dict[str, Any]]:
        make_dict = lambda values: {col_def.name: value for col_def, value in zip(col_defs, values)}
        mvs = [memoryview(j_array[i]) if col_def.data_type.is_primitive else j_array[i] for i, col_def in enumerate(col_defs)]
        return map(make_dict, zip(*mvs))

    return _table_reader_chunk(table, cols, emitter=_emitter, row_set=table.j_table.getRowSet(), chunk_size=chunk_size, prev=False)

def _table_reader_row_tuple(table: Table, cols: Optional[Union[str, Sequence[str]]] = None, tuple_name: str = 'Deephaven',
                            chunk_size: int = 2048) -> Generator[Tuple[Any, ...], None, None]:
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
        chunk_size (int): The number of rows to read at a time internally to reduce the number of Java/Python boundary
            crossings. Default is 2048.

    Returns:
        A generator that yields a named tuple for each row in the table

    Raises:
        ValueError
    """
    named_tuple_class = namedtuple(tuple_name, cols or table.column_names, rename=False)

    def _emitter(col_defs: Sequence[ColumnDefinition], j_array: jpy.JType) -> Iterable[Tuple[Any, ...]]:
        mvs = [memoryview(j_array[i]) if col_def.data_type.is_primitive else j_array[i] for i, col_def in enumerate(col_defs)]
        return map(named_tuple_class._make, zip(*mvs))

    return _table_reader_chunk(table, cols, emitter=_emitter, row_set=table.j_table.getRowSet(), chunk_size=chunk_size, prev=False)
