#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""This module implements the Query class that can be used to execute a chained set of Table operations in one
batch."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydeephaven._table_interface import TableInterface
from pydeephaven._table_ops import NoneOp, TableOp
from pydeephaven.dherror import DHError
from pydeephaven.table import Table

if TYPE_CHECKING:
    from pydeephaven.session import Session


class Query(TableInterface["Query"]):
    """A Query object is used to define and execute a sequence of Deephaven table operations on the server.

    When the query is executed, the table operations specified for the Query object are batched together and sent to the
    server in a single request, thus avoiding multiple round trips between the client and the server. The result of
    executing the query is a new Deephaven table.

    Note, an application should always use the factory method on the Session object to create a Query instance as the
    constructor is subject to future changes to support more advanced features already planned.
    """

    _ops: list[TableOp]

    _doc_vars = {
        "returns": "self",
        "intro": "Adds this operation to the query.",
    }

    def table_op_handler(self, table_op: TableOp) -> Query:
        self._ops.append(table_op)
        return self

    def __init__(self, session: Session, table: Table):
        self.session = session
        if not self.session or not table:
            raise DHError("invalid session or table value.")
        self._ops = [NoneOp(table=table)]

    def exec(self) -> Table:
        """Executes the query on the server and return the result table.

        Returns:
            a Table object

        Raises:
            DHError
        """
        # The first op is the NoneOp that holds the source table. It cannot be sent as a request on its own.
        if len(self._ops) < 2:
            raise DHError("the query has no table operations to execute.")
        return self.session.table_service.batch(self._ops)
