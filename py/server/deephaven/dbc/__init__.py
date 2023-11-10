#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""The dbc package includes the modules and functions for using external databases with Deephaven."""
from typing import Any, Literal

import deephaven.arrow as dharrow
from deephaven import DHError
from deephaven.table import Table


def read_sql(conn: Any, query: str, driver: Literal["odbc", "adbc", "connectorx"] = "connectorx") -> Table:
    """Executes the provided SQL query via a supported driver and returns a Deephaven table.

    Args:
        conn (Any): must either be a connection string for the given driver or a Turbodbc/ADBC DBAPI Connection
            object; when it is a Connection object, the driver argument will be ignored.
        query (str): SQL query statement
        driver: (str): the driver to use, supported drivers are "odbc", "adbc", "connectorx", default is "connectorx"

    Returns:
        a new Table

    Raises:
        DHError
    """
    if isinstance(conn, str):
        if driver == "connectorx":
            try:
                import connectorx as cx
            except ImportError:
                raise DHError(message="import connectorx failed, please install it first.")

            try:
                pa_table = cx.read_sql(conn=conn, query=query, return_type="arrow")
                return dharrow.to_table(pa_table)
            except Exception as e:
                raise DHError(e, message="failed to get a Arrow table from ConnectorX.") from e
        elif driver == "odbc":
            from deephaven.dbc.odbc import read_cursor
            import turbodbc
            with turbodbc.connect(connection_string=conn) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return read_cursor(cursor)
        elif driver == "adbc":
            from deephaven.dbc.adbc import read_cursor
            if not conn:
                import adbc_driver_sqlite.dbapi as dbapi
            elif conn.strip().startswith("postgresql:"):
                import adbc_driver_postgresql.dbapi as dbapi
            else:
                raise DHError(message=f"unsupported ADBC connection string {conn}")

            with dbapi.connect(conn) as dbconn:
                with dbconn.cursor() as cursor:
                    cursor.execute(query)
                    return read_cursor(cursor)
        else:
            raise DHError(message=f"unsupported driver {driver}")
    else:
        try:
            import adbc_driver_manager.dbapi as dbapi
            if isinstance(conn, dbapi.Connection):
                from deephaven.dbc.adbc import read_cursor
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return read_cursor(cursor)
        except ImportError:
            pass

        try:
            import turbodbc.connection
            if isinstance(conn, turbodbc.connection.Connection):
                from deephaven.dbc.odbc import read_cursor
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    return read_cursor(cursor)
        except ImportError:
            pass

        raise DHError(message=f"invalid conn argument {conn}")





