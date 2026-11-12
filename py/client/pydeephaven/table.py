#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""This module implements the Table and InputTable classes which are the main instruments to work with Deephaven
data."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

import pyarrow as pa

from pydeephaven._table_interface import TableInterface
from pydeephaven._table_ops import (
    MetaTableOp,
    MultijoinTablesOp,
    SortDirection,
    TableOp,
)
from pydeephaven._utils import to_list
from pydeephaven.dherror import DHError
from pydeephaven.ticket import ExportTicket, ServerObject, Ticket

if TYPE_CHECKING:
    from pydeephaven.session import Session


class Table(TableInterface["Table"], ServerObject):
    """A Table object represents a reference to a table on the server. It is the core data structure of
    Deephaven and supports a rich set of operations such as filtering, sorting, aggregating, joining, snapshotting etc.

    Note, an application should never instantiate a Table object directly. Table objects are always provided through
    factory methods such as Session.empty_table(), or import/export methods such as Session.import_table(),
    open_table(), or any of the Table operations.

    Attributes:
        is_closed (bool): check if the table has been closed on the server
    """

    _doc_vars = {"returns": "a Table object", "raises": "Raises:\n    DHError"}

    def table_op_handler(self, table_op: TableOp) -> Table:
        return self.session.table_service.grpc_table_op(self, table_op)

    def __init__(
        self,
        session: Session,
        ticket: Ticket,
        schema_header: bytes = b"",
        size: int | None = None,
        is_static: bool | None = None,
        schema: pa.Schema | None = None,
    ):
        ServerObject.__init__(self, type="Table", ticket=ticket)
        if not session or not session.is_alive:
            raise DHError("Must be associated with a active session")
        self.session = session
        self.schema = schema
        self.is_static = is_static
        self.size = size
        if not schema:
            self._parse_schema(schema_header)
        self._meta_table: Table | None = None
        self._closed: bool = False

    def __del__(self) -> None:
        # Must not make an RPC: it can run inside a gRPC call that holds the channel lock, and would deadlock. So the
        # session releases the ticket on its own thread, and this checks is_connected, not is_alive. Only an export
        # with a schema is owned by this wrapper. A wrapper built from another table's ticket has none, and releasing
        # it would break that table.
        try:
            if (
                not self._closed
                and isinstance(self.ticket, ExportTicket)
                and self.schema is not None
                and self.session.is_connected
            ):
                self.session._queue_release(self.ticket)
                self._closed = True
        except Exception:
            # TODO(deephaven-core#1858): Better error handling for pyclient around release #1858
            pass

    @property
    def is_refreshing(self) -> bool:
        """Whether this table is refreshing."""
        return not self.is_static

    @property
    def is_closed(self) -> bool:
        """Whether this table is closed."""
        return self._closed

    def close(self) -> None:
        """Close the table reference on the server.

        Raises:
            DHError
        """
        self.session.release(self.ticket)
        self._closed = True

    def _parse_schema(self, schema_header: bytes) -> None:
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

    def sort_descending(self, order_by: str | list[str]) -> Table:
        """The sort_descending method creates a new table where rows in a table are sorted in descending
        order based on the order_by column(s).

        Args:
            order_by (Union[str, list[str]]): the column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        order_by = to_list(order_by)
        order = [SortDirection.DESCENDING] * len(order_by)
        return super().sort(order_by, order)


class InputTable(Table):
    """InputTable is a subclass of Table that allows the users to dynamically add/delete/modify data in it. There are
    two types of InputTable - append-only and keyed.

    The append-only input table is not keyed, all rows are added to the end of the table, and deletions and edits are
    not permitted.

    The keyed input tablet has keys for each row and supports addition/deletion/modification of rows by the keys.
    """

    key_cols: list[str] | None

    def __init__(
        self,
        session: Session,
        ticket: Ticket,
        schema_header: bytes = b"",
        size: int | None = None,
        is_static: bool | None = None,
        schema: pa.Schema | None = None,
    ):
        super().__init__(
            session=session,
            ticket=ticket,
            schema_header=schema_header,
            size=size,
            is_static=is_static,
            schema=schema,
        )

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
            raise PermissionError(
                "deletion on an append-only input table is not allowed."
            )
        try:
            self.session.input_table_service.delete(self, table)
        except Exception as e:
            raise DHError("delete data in the InputTable failed.") from e


class MultiJoinTable:
    """A MultiJoinTable is an object that contains the result of a multi-table natural join. To retrieve the underlying
    result Table, use the :attr:`.table` property."""

    def __init__(self, table: Table):
        self._table = table

    @property
    def table(self) -> Table:
        """Returns the Table containing the multi-table natural join output."""
        return self._table


class MultiJoinInput:
    """A MultiJoinInput represents the input tables, key columns and additional columns to be used in the multi-table
    natural join.
    """

    table: Table
    on: str | Sequence[str]
    joins: str | Sequence[str] | None = None

    def __init__(
        self,
        table: Table,
        on: str | Sequence[str],
        joins: str | Sequence[str] | None = None,
    ):
        """Initializes a MultiJoinInput object.

        Args:
            table (Table): the right table to include in the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equality expression that
                matches every input table, i.e. "col_a = col_b" to rename output column names.
            joins (Optional[Union[str, Sequence[str]]]): the column(s) to be added from the table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
        """
        self.table = table
        self.on = to_list(on)
        self.joins = to_list(joins)


def multi_join(
    input: Table | Sequence[Table] | MultiJoinInput | Sequence[MultiJoinInput],
    on: str | Sequence[str] | None = None,
) -> MultiJoinTable:
    """The multi_join method creates a new table by performing a multi-table natural join on the input tables. The
    result consists of the set of distinct keys from the input tables natural joined to each input table. Input
    tables need not have a matching row for each key, but they may not have multiple matching rows for a given key.

    Args:
        input (Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]): the input objects specifying the
            tables and columns to include in the join.
        on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equality expression
            that matches every input table, i.e. "col_a = col_b" to rename output column names. Note: When
            MultiJoinInput objects are supplied, this parameter must be omitted.

    Returns:
        MultiJoinTable: the result of the multi-table natural join operation.  To access the underlying Table, use the
            :attr:`~MultiJoinTable.table` property.

    Raises:
        DHError
    """
    if isinstance(input, Table) or (
        isinstance(input, Sequence) and all(isinstance(t, Table) for t in input)
    ):
        tables = to_list(input)
        session = tables[0].session
        if not all([t.session == session for t in tables]):
            raise DHError(message="all tables must be from the same session.")
        if on is None:
            raise DHError(
                message="on parameter is required when Table objects are provided."
            )
        multi_join_inputs = [MultiJoinInput(table=t, on=on) for t in tables]
    elif isinstance(input, MultiJoinInput) or (
        isinstance(input, Sequence)
        and all(isinstance(ji, MultiJoinInput) for ji in input)
    ):
        if on is not None:
            raise DHError(
                message="on parameter is not permitted when MultiJoinInput objects are provided."
            )
        multi_join_inputs = to_list(input)
        session = multi_join_inputs[0].table.session
        if not all([mji.table.session == session for mji in multi_join_inputs]):
            raise DHError(message="all tables must be from the same session.")
    else:
        raise DHError(
            message="input must be a Table, a sequence of Tables, a MultiJoinInput, or a sequence of MultiJoinInputs."
        )

    table_op = MultijoinTablesOp(multi_join_inputs=multi_join_inputs)
    return MultiJoinTable(
        table=session.table_service.grpc_table_op(None, table_op, table_class=Table)
    )
