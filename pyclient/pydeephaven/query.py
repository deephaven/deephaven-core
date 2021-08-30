#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from pydeephaven._table_ops import *
from pydeephaven.dherror import DHError
from pydeephaven._table_interface import TableInterface


class Query(TableInterface):
    """ A Query object is used to define and exec a sequence of Deephaven table operations on the server.

    When the query is executed, the table operations specified for the Query object are batched together and sent to the
    server in a single request, thus avoiding multiple round trips between the client and the server. The result of
    executing the query is a new Deephaven table.

    Note, an application should always use the factory method on the Session object to create a Query instance as the
    constructor is subject to future changes to support more advanced features already planned.
    """

    def table_op_handler(self, table_op):
        self._ops.append(table_op)
        return self

    def __init__(self, session, table):
        self.session = session
        if not self.session or not table:
            raise DHError("invalid session or table value.")
        self._ops = [NoneOp(table=table)]

    def exec(self):
        """ Execute the query on the server and return the result table.

        Args:

        Returns:
            self

        Raises:
            

        """
        return self.session.table_service.batch(self._ops)
