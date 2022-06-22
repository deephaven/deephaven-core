#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from __future__ import annotations

import pyarrow

from pydeephaven.dherror import DHError
from pydeephaven._table_interface import TableInterface


class Table(TableInterface):
    """ A Table object represents a reference to a table on the server. It is the core data structure of
    Deephaven and supports a rich set of operations such as filtering, sorting, aggregating, joining, snapshotting etc.

    Note, an application should never instantiate a Table object directly. Table objects are always provided through
    factory methods such as Session.empty_table(), or import/export methods such as Session.import_table(),
    open_table(), or any of the Table operations.

    Attributes:
        is_closed (bool): check if the table has been closed on the server
    """

    def table_op_handler(self, table_op):
        return self.session.table_service.grpc_table_op(self, table_op)

    def __init__(self, session, ticket, schema_header=b'', size=None, is_static=None, schema=None):
        if not session or not session.is_alive:
            raise DHError("Must be associated with a active session")
        self.session = session
        self.ticket = ticket
        self.schema = schema
        self.is_static = is_static
        self.size = size
        if not schema:
            self._parse_schema(schema_header)

    def __del__(self):
        try:
            # only table objects that are explicitly exported have schema info and only the tickets associated with
            # such tables should be released.
            if self.ticket and self.schema and self.session.is_alive:
                self.session.release(self.ticket)
        except Exception as e:
            # TODO(deephaven-core#1858): Better error handling for pyclient around release #1858
            pass

    @property
    def is_closed(self):
        return not self.ticket

    def close(self) -> None:
        """ Close the table reference on the server.

        Raises:
            DHError
        """
        self.session.release(self.ticket)
        self.ticket = None

    def _parse_schema(self, schema_header):
        if not schema_header:
            return

        reader = pyarrow.ipc.open_stream(schema_header)
        self.schema = reader.schema

    def snapshot(self) -> pyarrow.Table:
        """ Take a snapshot of the table and return a pyarrow Table.

        Returns:
            a pyarrow.Table

        Raises:
            DHError
        """
        return self.session.flight_service.snapshot_table(self)
