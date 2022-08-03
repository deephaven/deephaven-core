#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module supports exporting Deephaven data in the HTML format. """

import jpy

from deephaven import DHError
from deephaven.table import Table

_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


def to_html(table: Table) -> str:
    """  Returns a table formatted as an HTML string. Limit use to small tables to avoid running out of memory.

    Returns:
        a HTML string

    Raises:
        DHError
    """
    try:
        return _JTableTools.html(table.j_table)
    except Exception as e:
        raise DHError(e, "table to_html failed") from e
