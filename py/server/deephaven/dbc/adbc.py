#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module supports ingesting data from external databases (relational and other types) into Deephaven via the
Python DB-API 2.0 (PEP 249) and the Apache Arrow Database Connectivity (ADBC) interfaces. (Please refer to
https://arrow.apache.org/docs/dev/format/ADBC.html for more details on ADBC).

ADBC defines a standard API to fetch data in Arrow format from databases that support Arrow natively as well as
from databases that only support ODBC/JDBC. By relying on ADBC, Deephaven is able to ingest data efficiently from a
wide variety of data sources. """

from deephaven import DHError
from deephaven import arrow as dharrow
from deephaven.table import Table

try:
    import adbc_driver_manager.dbapi
except ImportError:
    raise DHError(message="import ADBC driver manager failed, please install ADBC driver manager and your "
                          "targeted database driver first.")


def read_cursor(cursor: adbc_driver_manager.dbapi.Cursor) -> Table:
    """Converts the result set of the provided cursor into a Deephaven table.

    Args:
        cursor (adbc_driver_manager.dbapi.Cursor): an ADBC DB-API cursor. Prior to it being passed in, its execute()
            method must be called to run a query operation that produces an Arrow table

    Returns:
        a new Table

    Raises:
        DHError, TypeError
    """

    if not isinstance(cursor, adbc_driver_manager.dbapi.Cursor):
        raise TypeError(f"expect {adbc_driver_manager.dbapi.Cursor} got {type(cursor)} instead.")

    try:
        pa_table = cursor.fetch_arrow_table()
    except Exception as e:
        raise DHError(e, message="failed to fetch ADBC result as an Arrow table.") from e

    return dharrow.to_table(pa_table)
