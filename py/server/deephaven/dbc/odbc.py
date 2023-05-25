#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module supports ingesting data from external relational databases into Deephaven via the Python DB-API 2.0
(PEP 249) and the Open Database Connectivity (ODBC) interfaces by using the Turbodbc module.

Turbodbc is DB-API 2.0 compliant, provides access to relational databases via the ODBC interface and more
importantly it has optimized, built-in Apache Arrow support when fetching ODBC result sets. This enables Deephaven to
achieve maximum efficiency when ingesting relational data. """

from deephaven import DHError
from deephaven import arrow as dharrow
from deephaven.table import Table

try:
    import turbodbc.cursor
except ImportError:
    raise DHError(message="import turbodbc failed, please install turbodbc, the ODBC driver manager, and the targeted "
                          "database driver first.")


def read_cursor(cursor: turbodbc.cursor.Cursor) -> Table:
    """Converts the result set of the provided cursor into a Deephaven table.

    Args:
        cursor (turbodbc.cursor.Cursor): a Turbodbc cursor. Prior to it being passed in, its execute() method must be
            called to run a query operation that produces a result set

    Returns:
        a new Table

    Raises:
        DHError, TypeError
    """

    if not isinstance(cursor, turbodbc.cursor.Cursor):
        raise TypeError(f"expect {turbodbc.cursor.Cursor} got {type(cursor)} instead.")
    try:
        pa_table = cursor.fetchallarrow()
    except Exception as e:
        raise DHError(e, message="failed to fetch ODBC result as a Arrow table.") from e

    return dharrow.to_table(pa_table)
