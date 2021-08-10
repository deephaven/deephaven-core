#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from typing import List

from pydeephaven import Table
from pydeephaven._query_ops import *
from pydeephaven.dherror import DHError


class Query:
    """ A Query object is used to define and exec a sequence of Deephaven table operations on the server.

    When the query is executed, the table operations specified for the Query object are batched together and sent to the
    server in a single request,  thus avoiding multiple round trips between the client and the server. The result of
    executing the query is a new Deephaven table.

    Note, an application should always use the factory method on the Session object to create a Query instance as the
    constructor is subject to future changes to support more advanced features already planned.
    """

    def __init__(self, session, table):
        self.session = session
        if not self.session or not table:
            raise DHError("invalid session or table value.")
        self._ops = self._last_op = NoneOp(table=table)

    def exec(self) -> Table:
        """ Execute the query on the server and returns the result table.

        Args:

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.batch(self._ops)

    def drop_columns(self, column_names: List[str]):
        """ Chain a drop-columns operation into the query.

        Args:
            column_names (List[str]: a list of column names

        Returns:
            self

        Raises:

        """
        self._last_op = DropColumnsOp(parent=self._last_op, column_names=column_names)
        return self

    def update(self, column_specs: List[str]):
        """ Chain a update operation into the query.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            self

        Raises:

        """
        self._last_op = UpdateOp(parent=self._last_op, column_specs=column_specs)
        return self

    def lazy_update(self, column_specs):
        """ Chain a lazy update operation into the query.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            self

        Raises:

        """
        self._last_op = LazyUpdateOp(parent=self._last_op, column_specs=column_specs)
        return self

    def view(self, column_specs):
        """ Chain a view operation into the query.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            self

        Raises:

        """
        self._last_op = ViewOp(parent=self._last_op, column_specs=column_specs)
        return self

    def update_view(self, column_specs):
        """ Chain a update-view operation into the query.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            self

        Raises:

        """
        self._last_op = UpdateViewOp(parent=self._last_op, column_specs=column_specs)
        return self

    def select(self, column_specs):
        """ Chain a select operation into the query.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            self

        Raises:

        """
        self._last_op = SelectOp(parent=self._last_op, column_specs=column_specs)
        return self

    def tail(self, num_rows: int):
        """ Chain a tail operation into the query

        Args:
            num_rows (int): the number of rows to return

        Returns:
            self

        Raises:

        """
        self._last_op = TailOp(parent=self._last_op, num_rows=num_rows)
        return self

    def head(self, num_rows):
        """ Chain a head operation into the query

        Args:
            num_rows (int): the number of rows to return

        Returns:
            self

        Raises:

        """
        self._last_op = HeadOp(parent=self._last_op, num_rows=num_rows)
        return self

    # def sort(self, columns: List[str], directions: List[SortDirection] = []):
    #     self._last_op = SortOp(parent=self._last_op, columns=columns, directions=directions)
    #     return self
