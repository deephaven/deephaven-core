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
    """A DataIndex represents a Deephaven data index object which is built on one or more indexed (key) column(s) of
    A Deephaven table."""
    j_object_type = _JDataIndex

    def __init__(self, j_data_index: jpy.JType):
        self._j_data_indexer = j_data_index

    @property
    def j_object(self) -> jpy.JType:
        return self._j_data_indexer

    @property
    def keys(self) -> List[str]:
        """the key column names of the DataIndex.

        Returns:
            the key columns
        """
        return j_list_to_list(self._j_data_indexer.keyColumnNames())

    @property
    def table(self) -> Table:
        """the backing table of the DataIndex.

        Returns:
            the backing table
        """
        return Table(self._j_data_indexer.table())



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

    Args:
        table (Table): the table to get the DataIndex from
        key_cols (List[str]): the key columns

    Returns:
        DataIndex: the DataIndex
    """
    j_di = _JDataIndexer.getDataIndex(table.j_table, key_cols)
    return DataIndex(j_di) if j_di else None

def create_data_index(table: Table, key_cols: List[str]) -> DataIndex:
    """Creates a DataIndex for the given key columns. If the DataIndex already exists, returns it.

    Args:
        table (Table): the table to create the DataIndex for
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