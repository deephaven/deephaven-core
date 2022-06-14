#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This package is a place for Deephaven experimental features. """

import jpy
from deephaven import DHError
from deephaven.table import Table

_JWindowCheck = jpy.get_type("io.deephaven.engine.util.WindowCheck")


def time_window(table: Table, ts_col: str, window: int, bool_col: str) -> Table:
    """Creates a new table by applying a time window to the source table and adding a new Boolean column.

    The value of the new Boolean column is set to false when the timestamp column value is older than the window from
    now or true otherwise. If the timestamp column value is null, the Boolean column value will be null as well. The
    result table ticks whenever the source table ticks, or modifies a row when it passes out of the window.

    Args:
        table (Table): the source table
        ts_col (str): the timestamp column name
        window (int): the size of the window in nanoseconds
        bool_col (str): the name of the new Boolean column.

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JWindowCheck.addTimeWindow(table.j_table, ts_col, window, bool_col))
    except Exception as e:
        raise DHError(e, "failed to create a time window table.") from e
