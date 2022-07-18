#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Utility module for the stream subpackage. """

import jpy

from deephaven import DHError
from deephaven.table import Table

_JStreamTableTools = jpy.get_type("io.deephaven.engine.table.impl.StreamTableTools")


def stream_to_append_only(table: Table) -> Table:
    """ Creates an 'append only' table from the stream table.

    Args:
        table (Table): a stream table

    Returns:
        an append-only table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JStreamTableTools.streamToAppendOnlyTable(table.j_table))
    except Exception as e:
        raise DHError(e, "failed to create an append-only table.") from e
