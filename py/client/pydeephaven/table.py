#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module implements the Table and InputTable classes which are the main instruments to work with Deephaven
data."""

from __future__ import annotations

from typing import List

import pyarrow as pa

from pydeephaven._table_ops import MetaTableOp
from pydeephaven.dherror import DHError
from pydeephaven._table_interface import TableInterface


class Table(TableInterface):
    """A Table object represents a reference to a table on the server. It is the core data structure of
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
        self._meta_table = None

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
    def is_refreshing(self):
        """Whether this table is refreshing."""
        return not self.is_static

    @property
    def is_closed(self):
        """Whether this table is closed on the server."""
        return not self.ticket

    def close(self) -> None:
        """Close the table reference on the server.

        Raises:
            DHError
        """
        self.session.release(self.ticket)
        self.ticket = None

    def _parse_schema(self, schema_header):
        if not schema_header:
            return

        reader = pa.ipc.open_stream(schema_header)
        self.schema = reader.schema

    @property
    def meta_table(self) -> Table:
        """The column definitions of the table in a Table form."""
        if self._meta_table is None:
            table_op = MetaTableOp()
            self._meta_table = self.session.table_service.grpc_table_op(self, table_op)
        return self._meta_table

    def to_arrow(self) -> pa.Table:
        """Takes a snapshot of the table and returns a pyarrow Table.

        Returns:
            a pyarrow.Table

        Raises:
            DHError
        """
        return self.session.flight_service.do_get_table(self)


class InputTable(Table):
    """InputTable is a subclass of Table that allows the users to dynamically add/delete/modify data in it. There are
    two types of InputTable - append-only and keyed.

    The append-only input table is not keyed, all rows are added to the end of the table, and deletions and edits are
    not permitted.

    The keyed input tablet has keys for each row and supports addition/deletion/modification of rows by the keys.
    """

    def __init__(self, session, ticket, schema_header=b'', size=None, is_static=None, schema=None):
        super().__init__(session=session, ticket=ticket, schema_header=schema_header, size=size,
                         is_static=is_static, schema=schema)
        self.key_cols: List[str] = None

    def add(self, table: Table) -> None:
        """Writes rows from the provided table to this input table. If this is a keyed input table, added rows with keys
        that match existing rows will replace those rows.

        Args:
            table (Table): the table that provides the rows to write

        Raises:
            DHError
        """
        try:
            self.session.input_table_service.add(self, table)
        except Exception as e:
            raise DHError("add to InputTable failed.") from e

    def delete(self, table: Table) -> None:
        """Deletes the keys contained in the provided table from this keyed input table. If this method is called on an
        append-only input table, a PermissionError will be raised.

        Args:
            table (Table): the table with the keys to delete

        Raises:
            DHError, PermissionError
        """
        if not self.key_cols:
            raise PermissionError("deletion on an append-only input table is not allowed.")
        try:
            self.session.input_table_service.delete(self, table)
        except Exception as e:
            raise DHError("delete data in the InputTable failed.") from e
