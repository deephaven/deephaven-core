#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""The dbc package includes the modules and functions for using external databases with Deephaven."""

from deephaven import DHError
from deephaven.table import Table
import deephaven.arrow as dharrow


def read_sql(conn: str, query: str) -> Table:
    """Executes the provided SQL query via ConnectorX and returns a Deephaven table.

    Args:
        conn (str): a connection string URI, please refer to https://sfu-db.github.io/connector-x/databases.html for
            database specific format
        query (str): SQL query statement

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        import connectorx as cx
    except ImportError:
        raise DHError(message="import ConnectorX failed, please install it first.")

    try:
        pa_table = cx.read_sql(conn=conn, query=query, return_type="arrow")
    except Exception as e:
        raise DHError(e, message="failed to get a Arrow table from ConnectorX.") from e

    return dharrow.to_table(pa_table)
