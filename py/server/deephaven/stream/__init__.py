#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Utility module for the stream subpackage. """
from warnings import warn

import jpy

from deephaven import DHError
from deephaven.table import Table

_JBlinkTableTools = jpy.get_type("io.deephaven.engine.table.impl.BlinkTableTools")

def blink_to_append_only(table: Table) -> Table:
    """ Creates an 'append only' table from the blink table.

    Args:
        table (Table): a blink table

    Returns:
        an append-only table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JBlinkTableTools.blinkToAppendOnly(table.j_table))
    except Exception as e:
        raise DHError(e, "failed to create an append-only table.") from e

# TODO (https://github.com/deephaven/deephaven-core/issues/3853): Delete this method
def stream_to_append_only(table: Table) -> Table:
    """Deprecated synonym for blink_to_append_only"""
    warn('This function is deprecated, prefer blink_to_append_only', DeprecationWarning, stacklevel=2)
    return blink_to_append_only(table)
