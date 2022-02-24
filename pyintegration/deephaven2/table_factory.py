#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module provides various ways to make a Deephaven table. """
from typing import List

import jpy

from deephaven2 import DHError
from deephaven2.column import InputColumn
from deephaven2.table import Table

_JTableFactory = jpy.get_type("io.deephaven.engine.table.TableFactory")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


def empty_table(size: int) -> Table:
    """ Creates a table with rows but no columns.

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


def time_table(period: str, start_time: str = None) -> Table:
    """ Creates a table that adds a new row on a regular interval.

    Args:
        period (str): time interval between new row additions
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
    """ Creates an in-memory table from a list of input columns. Each column must have an equal number of elements.

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
    """ Combines two or more tables into one aggregate table. This essentially appends the tables one on top of the
    other. Null tables are ignored.

    Args:
        tables (List[Table]): the source tables

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableTools.merge([t.j_table for t in tables]))
    except Exception as e:
        raise DHError(e, "merge tables operation failed.") from e


def merge_sorted(tables: List[Table], order_by: str) -> Table:
    """ Combines two or more tables into one sorted, aggregate table. This essentially stacks the tables one on top
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
        return Table(j_table=_JTableTools.mergeSorted(order_by, *[t.j_table for t in tables]))
    except Exception as e:
        raise DHError(e, "merge sorted operation failed.") from e
