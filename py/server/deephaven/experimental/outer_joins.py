#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

#
#
"""This module allows users to perform SQL-style left outer and full outer joins on tables."""

from typing import Union, Sequence

from deephaven import DHError
from deephaven.jcompat import to_sequence
from deephaven.table import Table
import jpy

from deephaven.update_graph import auto_locking_ctx

_JOuterJoinTools = jpy.get_type("io.deephaven.engine.util.OuterJoinTools")


def full_outer_join(l_table: Table, r_table: Table, on: Union[str, Sequence[str]] = None,
                    joins: Union[str, Sequence[str]] = None) -> Table:
    """The full_outer_join function creates a new table containing rows that have matching values in both tables.
    If there are multiple matches between a row from the left able and rows from the right table, all matching
    combinations will be included. Additionally, non-matching rows from both tables will also be included in the new
    table. If no columns to match (on) are specified, then every combination of left and right table rows is included.

    Args:
        l_table (Table): the left table
        r_table (Table): the right table
        on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
            i.e. "col_a = col_b" for different column names; default is None
        joins (Union[str, Sequence[str]], optional): the column(s) to be added from right table to the result
            table, can be renaming expressions, i.e. "new_col = col"; default is None, meaning all the columns from
            the right table

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        on = to_sequence(on)
        joins = to_sequence(joins)
        with auto_locking_ctx(l_table, r_table):
            if joins:
                return Table(j_table=_JOuterJoinTools.fullOuterJoin(l_table.j_table, r_table.j_table,
                                                                    ",".join(on), ",".join(joins)))
            else:
                return Table(j_table=_JOuterJoinTools.fullOuterJoin(l_table.j_table, r_table.j_table, ",".join(on)))
    except Exception as e:
        raise DHError(e, message="failed to perform full-outer-join on tables.") from e


def left_outer_join(l_table: Table, r_table: Table, on: Union[str, Sequence[str]] = None,
                    joins: Union[str, Sequence[str]] = None) -> Table:
    """The left_outer_join function creates a new table containing rows that have matching values in both tables.
    If there are multiple matches between a row from the left able and rows from the right table, all matching
    combinations will be included. Additionally, non-matching rows from the left tables will also be included in the new
    table. If no columns to match (on) are specified, then every combination of left and right table rows is included.

    Args:
        l_table (Table): the left table
        r_table (Table): the right table
        on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
            i.e. "col_a = col_b" for different column names
        joins (Union[str, Sequence[str]], optional): the column(s) to be added from right table to the result
            table, can be renaming expressions, i.e. "new_col = col"; default is None, default is None, meaning all
            the columns from the right table

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        on = to_sequence(on)
        joins = to_sequence(joins)
        with auto_locking_ctx(l_table, r_table):
            if joins:
                return Table(j_table=_JOuterJoinTools.leftOuterJoin(l_table.j_table, r_table.j_table,
                                                                    ",".join(on), ",".join(joins)))
            else:
                return Table(j_table=_JOuterJoinTools.leftOuterJoin(l_table.j_table, r_table.j_table, ",".join(on)))
    except Exception as e:
        raise DHError(e, message="failed to perform left-outer-join on tables.") from e
