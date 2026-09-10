#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

#
#
"""This module allows users to perform SQL-style left outer and full outer joins on tables."""

from collections.abc import Sequence
from typing import Optional, Union

import jpy

from deephaven import DHError
from deephaven.jcompat import to_sequence
from deephaven.table import Table, _default_cross_join_reserve_bits
from deephaven.update_graph import auto_locking_ctx

_JOuterJoinTools = jpy.get_type("io.deephaven.engine.util.OuterJoinTools")


def full_outer_join(
    l_table: Table,
    r_table: Table,
    on: Optional[Union[str, Sequence[str]]] = None,
    joins: Optional[Union[str, Sequence[str]]] = None,
    reserve_bits: Optional[int] = None,
) -> Table:
    """The full_outer_join function creates a new table containing rows that have matching values in both tables.
    If there are multiple matches between a row from the left table and rows from the right table, all matching
    combinations will be included. Additionally, non-matching rows from both tables will also be included in the new
    table. If no columns to match (on) are specified, then every combination of left and right table rows is included.

    Args:
        l_table (Table): the left table
        r_table (Table): the right table
        on (Optional[Union[str, Sequence[str]]]): the column(s) to match, can be a common name or an equal expression,
            i.e. "col_a = col_b" for different column names; default is None
        joins (Optional[Union[str, Sequence[str]]]): the column(s) to be added from right table to the result
            table, can be renaming expressions, i.e. "new_col = col"; default is None, meaning all the columns from
            the right table
        reserve_bits (Optional[int]): the number of bits to reserve for the right row; default is None,
            meaning the configured value is used, which is 10 bits by default.

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        on = ",".join(to_sequence(on))
        joins = ",".join(to_sequence(joins))
        reserve_bits = (
            reserve_bits if reserve_bits else _default_cross_join_reserve_bits()
        )
        with auto_locking_ctx(l_table, r_table):
            return Table(
                j_table=_JOuterJoinTools.fullOuterJoin(
                    l_table.j_table, r_table.j_table, on, joins, reserve_bits
                )
            )
    except Exception as e:
        raise DHError(e, message="failed to perform full-outer-join on tables.") from e


def left_outer_join(
    l_table: Table,
    r_table: Table,
    on: Optional[Union[str, Sequence[str]]] = None,
    joins: Optional[Union[str, Sequence[str]]] = None,
    reserve_bits: Optional[int] = None,
) -> Table:
    """The left_outer_join function creates a new table containing rows that have matching values in both tables.
    If there are multiple matches between a row from the left table and rows from the right table, all matching
    combinations will be included. Additionally, non-matching rows from the left tables will also be included in the new
    table. If no columns to match (on) are specified, then every combination of left and right table rows is included.

    Args:
        l_table (Table): the left table
        r_table (Table): the right table
        on (Optional[Union[str, Sequence[str]]]): the column(s) to match, can be a common name or an equal expression,
            i.e. "col_a = col_b" for different column names
        joins (Optional[Union[str, Sequence[str]]]): the column(s) to be added from right table to the result
            table, can be renaming expressions, i.e. "new_col = col"; default is None, meaning all the columns from the right table
        reserve_bits (Optional[int]): the number of bits to reserve for the right row; default is None,
            meaning the configured value is used, which is 10 bits by default.

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        on = ",".join(to_sequence(on))
        joins = ",".join(to_sequence(joins))
        reserve_bits = (
            reserve_bits if reserve_bits else _default_cross_join_reserve_bits()
        )
        with auto_locking_ctx(l_table, r_table):
            return Table(
                j_table=_JOuterJoinTools.leftOuterJoin(
                    l_table.j_table, r_table.j_table, on, joins, reserve_bits
                )
            )
    except Exception as e:
        raise DHError(e, message="failed to perform left-outer-join on tables.") from e
