#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module allows users to perform SQL-style left outer and full outer joins on tables."""

from typing import Union, Sequence

from deephaven import DHError
from deephaven.jcompat import to_sequence
from deephaven.table import Table
import jpy

from deephaven.ugp import auto_locking_ctx

_JOuterJoinTools = jpy.get_type("io.deephaven.engine.util.OuterJoinTools")


def full_outer_join(table1: Table, table2: Table, on: Union[str, Sequence[str]],
                    joins: Union[str, Sequence[str]] = None) -> Table:
    """Returns a table that has one column for each of table1 columns, and one column corresponding to each of table2
    columns listed in 'joins' (or all the columns whose names don't overlap with the name of a column from table1 if
    'joins' is None or empty). The returned table will have one row for each matching set of keys between the first
    and second tables, plus one row for any first table key set that doesn't match the second table and one row for
    each key set from the second table that doesn't match the first table. Columns from either table for which there
    was no match in the other table will have null values.

    Args:
        table1 (Table): the input table 1
        table2 (Table): the input table 2
        on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
            i.e. "col_a = col_b" for different column names
        joins (Union[str, Sequence[str]], optional): the column(s) to be added from table2 to the result
            table, can be renaming expressions, i.e. "new_col = col"; default is None

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        on = to_sequence(on)
        joins = to_sequence(joins)
        with auto_locking_ctx(table1, table2):
            if joins:
                return Table(j_table=_JOuterJoinTools.fullOuterJoin(table1.j_table, table2.j_table,
                                                                    ",".join(on), ",".join(joins)))
            else:
                return Table(j_table=_JOuterJoinTools.fullOuterJoin(table1.j_table, table1.j_table, ",".join(on)))
    except Exception as e:
        raise DHError(e, message="failed to perform full-outer-join on tables.") from e


def left_outer_join(l_table: Table, r_table: Table, on: Union[str, Sequence[str]],
                    joins: Union[str, Sequence[str]] = None) -> Table:
    """Returns a table that has one column for each of the left table columns, and one column corresponding to each
    of the right table columns listed in 'joins' (or all the columns whose names don't overlap with the name of a
    column from the left table if 'joins' is None or empty. The returned table will have one row for each matching
    set of keys between the left table and right table plus one row for any left table key set that doesn't match the
    right table. Columns from the right table for which there was no match will have null values.

    Args:
        l_table (Table): the left table
        r_table (Table): the right table
        on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
            i.e. "col_a = col_b" for different column names
        joins (Union[str, Sequence[str]], optional): the column(s) to be added from table2 to the result
            table, can be renaming expressions, i.e. "new_col = col"; default is None

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
