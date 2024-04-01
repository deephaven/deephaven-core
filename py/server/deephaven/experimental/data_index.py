#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
"""This module provides the ability to create, check, and retrieve DataIndex objects from Deephaven tables."""

from typing import List, Optional

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import j_list_to_list
from deephaven.table import Table

_JDataIndexer = jpy.get_type("io.deephaven.engine.table.impl.indexer.DataIndexer")
_JDataIndex = jpy.get_type("io.deephaven.engine.table.DataIndex")

class DataIndex(JObjectWrapper):
    """A DataIndex is an index used to improve the speed of data access operations for a Deephaven table.  The index applies to one or more indexed (key) column(s) of a Deephaven table."""
    j_object_type = _JDataIndex

    def __init__(self, j_data_index: jpy.JType):
        self._j_data_index = j_data_index

    @property
    def j_object(self) -> jpy.JType:
        return self._j_data_index

    @property
    def keys(self) -> List[str]:
        """Returns the names of the columns indexed by the DataIndex.

        Returns:
            the key columns
        """
        return j_list_to_list(self._j_data_index.keyColumnNames())

    @property
    def table(self) -> Table:
        """the backing table of the DataIndex.

        Returns:
            the backing table
        """
        return Table(self._j_data_index.table())



def has_data_index(table: Table, key_cols: List[str]) -> bool:
    """Checks if a table currently has a DataIndex for the given key columns.

    Args:
        table (Table): the table to check
        key_cols (List[str]): the key columns to check

    Returns:
        bool: True if the table has a DataIndex, False otherwise
    """
    return _JDataIndexer.hasDataIndex(table.j_table, key_cols)

def get_data_index(table: Table, key_cols: List[str]) -> Optional[DataIndex]:
    """Gets a DataIndex for the given key columns. Returns None if the DataIndex does not exist.
    Note that the returned DataIndex will be managed with the enclosing liveness scope, which means it is users'
    responsibility to keep the DataIndex live and reachable by following the rules of liveness scope.

    Args:
        table (Table): the table to get the DataIndex from
        key_cols (List[str]): the key columns

    Returns:
        DataIndex or None
    """
    j_di = _JDataIndexer.getDataIndex(table.j_table, key_cols)
    return DataIndex(j_di) if j_di else None

def create_data_index(table: Table, key_cols: List[str]) -> DataIndex:
    """Creates a DataIndex for the given key columns on the provided table. If the DataIndex already exists, it is returned without recomputation.
    
    Note that the returned DataIndex will be managed with the enclosing liveness scope, which means it is users'
    responsibility to keep the DataIndex live and reachable by following the rules of liveness scope.

    Args:
        table (Table): the table to index
        key_cols (List[str]): the key columns

    Returns:
        DataIndex: the DataIndex

    Raises:
        DHError: if the DataIndex cannot be created
    """
    try:
        return DataIndex(_JDataIndexer.getOrCreateDataIndex(table.j_table, key_cols))
    except Exception as e:
        raise DHError(e, "failed to create DataIndex.") from e