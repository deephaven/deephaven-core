#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

""" Utility module for the stream subpackage. """
from warnings import warn

import jpy

from deephaven import DHError
from deephaven.table import Table

_JBlinkTableTools = jpy.get_type("io.deephaven.engine.table.impl.BlinkTableTools")
_JAddOnlyToBlinkTableAdapter = jpy.get_type("io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter")

def add_only_to_blink(table: Table) -> Table:
    """ Creates a blink table from the given add-only table.  The blink table contains the rows added in the latest
    update cycle.

    Note that the use of this function should be limited to add-only tables that are not fully in-memory, or when
    blink table specific aggregation semantics are desired. If the table is fully in-memory, creating a downstream blink
    table is not recommended because it doesn't achieve the main benefit of blink tables, which is to reduce memory usage
    but instead increases memory usage.

    Args:
        table (Table): the source table

    Returns:
        a blink table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JAddOnlyToBlinkTableAdapter.toBlink(table.j_table))
    except Exception as e:
        raise DHError(e, "failed to create a blink table.") from e

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
