#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from typing import List

from deephaven2 import DHError
from deephaven2.column import Column


class Table:
    """ A Table object represents a Deephaven table. """

    def __init__(self, db_table):
        self._db_table = db_table
        self._definition = self._db_table.getDefinition()
        self._schema = None

    # to make the table visible to DH script session
    def get_dh_table(self):
        return self._db_table
    
    @property
    def db_table(self):
        """ The underlying Table object in Java. """
        return self._db_table

    @property
    def columns(self):
        """ Returns the column info of the table.

        Returns:
            a list of column definitions

        Raises:
            DHError
        """
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
        """ Perform an update operation on the table and return the result table.

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
