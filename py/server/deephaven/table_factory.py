#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides various ways to make a Deephaven table. """
from typing import List, Dict, Any, Union

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.column import InputColumn
from deephaven.dtypes import DType
from deephaven.jcompat import to_sequence
from deephaven.table import Table
from deephaven.ugp import auto_locking_ctx

_JTableFactory = jpy.get_type("io.deephaven.engine.table.TableFactory")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")
_JDynamicTableWriter = jpy.get_type("io.deephaven.engine.table.impl.util.DynamicTableWriter")


def empty_table(size: int) -> Table:
    """Creates a table with rows but no columns.

    Args:
        size (int): the number of rows

    Returns:
         a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableTools.emptyTable(size))
    except Exception as e:
        raise DHError(e, "failed to create an empty table.") from e


def time_table(period: Union[str, int], start_time: str = None) -> Table:
    """Creates a table that adds a new row on a regular interval.

    Args:
        period (Union[str, int]): time interval between new row additions, can be expressed as an integer in
            nanoseconds or a time interval string, e.g. "00:00:00.001"
        start_time (str): start time for adding new rows

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        if start_time:
            return Table(j_table=_JTableTools.timeTable(start_time, period))
        else:
            return Table(j_table=_JTableTools.timeTable(period))

    except Exception as e:
        raise DHError(e, "failed to create a time table.") from e


def new_table(cols: List[InputColumn]) -> Table:
    """Creates an in-memory table from a list of input columns. Each column must have an equal number of elements.

    Args:
        cols (List[InputColumn]): a list of InputColumn

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableFactory.newTable(*[col.j_column for col in cols]))
    except Exception as e:
        raise DHError(e, "failed to create a new time table.") from e


def merge(tables: List[Table]):
    """Combines two or more tables into one aggregate table. This essentially appends the tables one on top of the
    other. Null tables are ignored.

    Args:
        tables (List[Table]): the source tables

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        with auto_locking_ctx(*tables):
            return Table(j_table=_JTableTools.merge([t.j_table for t in tables]))
    except Exception as e:
        raise DHError(e, "merge tables operation failed.") from e


def merge_sorted(tables: List[Table], order_by: str) -> Table:
    """Combines two or more tables into one sorted, aggregate table. This essentially stacks the tables one on top
    of the other and sorts the result. Null tables are ignored. mergeSorted is more efficient than using merge
    followed by sort.

    Args:
        tables (List[Table]): the source tables
        order_by (str): the name of the key column

    Returns:
         a Table

    Raises:
        DHError
    """
    try:
        with auto_locking_ctx(*tables):
            return Table(j_table=_JTableTools.mergeSorted(order_by, *[t.j_table for t in tables]))
    except Exception as e:
        raise DHError(e, "merge sorted operation failed.") from e


class DynamicTableWriter(JObjectWrapper):
    """The DynamicTableWriter creates a new in-memory table and supports writing data to it.

    This class implements the context manager protocol and thus can be used in with statements.
    """

    j_object_type = _JDynamicTableWriter

    def __init__(self, col_defs: Dict[str, DType]):
        """Initializes the writer and creates a new in-memory table.

        Args:
            col_defs(Dict[str, DTypes]): a map of column names and types of the new table

        Raises:
            DHError
        """
        col_names = list(col_defs.keys())
        col_dtypes = list(col_defs.values())
        try:
            self._j_table_writer = _JDynamicTableWriter(col_names, [t.qst_type for t in col_dtypes])
            self.table = Table(j_table=self._j_table_writer.getTable())
        except Exception as e:
            raise DHError(e, "failed to create a DynamicTableWriter.") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_table_writer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        """Closes the writer.

        Raises:
            DHError
        """
        try:
            self._j_table_writer.close()
        except Exception as e:
            raise DHError(e, "failed to close the writer.") from e

    def write_row(self, *values: Any) -> None:
        """Writes a row to the newly created table.

        The type of a value must be convertible (safely or unsafely, e.g. lose precision, overflow, etc.) to the type
        of the corresponding column.

        Args:
            *values (Any): the values of the new row, the data types of these values must match the column definitions
                of the table

        Raises:
             DHError
        """
        try:
            values = to_sequence(values)
            self._j_table_writer.logRowPermissive(values)
        except Exception as e:
            raise DHError(e, "failed to write a row.") from e
