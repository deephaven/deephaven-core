#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module implements the Table class and functions that work with Tables. """
from typing import List

from deephaven2 import DHError
from deephaven2.column import Column


class Table:
    """ A Table represents a Deephaven table. It allows applications to perform powerful Deephaven table operations


    Note: A client should not instantiate Table directly. Tables are mostly created by factory methods, data ingress
    operations, queries, aggregations, joins, etc.
    """

    def __init__(self, db_table):
        self._db_table = db_table
        self._definition = self._db_table.getDefinition()
        self._schema = None

    # to make the table visible to DH script session
    def get_dh_table(self):
        return self._db_table

    @property
    def columns(self):
        """ The column definitions of the table. """
        if self._schema:
            return self._schema

        self._schema = []
        j_col_list = self._definition.getColumnList()
        for i in range(j_col_list.size()):
            j_col = j_col_list.get(i)
            self._schema.append(Column(j_col.getName(),
                                       j_col.getDataType(),
                                       j_col.getComponentType(),
                                       j_col.getColumnType()))

        return self._schema

    def update(self, formulas: List[str]):
        """ The update method creates a new table containing a new, in-memory column for each formula.

        Args:
            formulas (List[str]): TODO

        Returns:
            A new table

        Raises:
            DHError
        """
        try:
            return Table(db_table=self._db_table.update(formulas))
        except Exception as e:
            raise DHError(e, "table.update failed") from e
