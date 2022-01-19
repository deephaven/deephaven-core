#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
import jpy

from deephaven2 import DHError
from deephaven2.table import Table

_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


def to_html(table: Table) -> str:
    """  Returns a printout of a table formatted as HTML. Limit use to small tables to avoid running out of memory.

    Returns:
        a String of the table printout formatted as HTML

    Raises:
        DHError
    """
    try:
        return _JTableTools.html(table.j_table)
    except Exception as e:
        raise DHError(e, "table to_html failed") from e
