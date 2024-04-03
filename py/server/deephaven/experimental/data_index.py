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
    """A DataIndex is an index used to improve the speed of data access operations for a Deephaven table.  The index
    applies to one or more indexed (key) column(s) of a Deephaven table.

    Note that a DataIndex itself is backed by a table."""

    j_object_type = _JDataIndex

    def __init__(self, j_data_index: jpy.JType):
        self._j_data_index = j_data_index

    @property
    def j_object(self) -> jpy.JType:
        return self._j_data_index

    @property
    def keys(self) -> List[str]:
        """The names of the columns indexed by the DataIndex. """
        return j_list_to_list(self._j_data_index.keyColumnNames())

    @property
    def table(self) -> Table:
        """The backing table of the DataIndex."""
        return Table(self._j_data_index.table())


def has_data_index(table: Table, key_cols: List[str]) -> bool:
    """Checks if a table currently has a DataIndex for the given key columns.

    Args:
        table (Table): the table to check
        key_cols (List[str]): the names of the key columns indexed

    Returns:
        bool: True if the table has a DataIndex, False otherwise
    """
    return _JDataIndexer.hasDataIndex(table.j_table, key_cols)


def _get_data_index(table: Table, key_cols: List[str]) -> Optional[DataIndex]:
    """Gets a DataIndex for the given key columns. Returns None if the DataIndex does not exist.

    Args:
        table (Table): the table to get the DataIndex from
        key_cols (List[str]): the names of the key columns indexed

    Returns:
        a DataIndex or None
    """
    j_di = _JDataIndexer.getDataIndex(table.j_table, key_cols)
    return DataIndex(j_di) if j_di else None


def data_index(table: Table, key_cols: List[str], create_if_absent: bool = True) -> Optional[DataIndex]:
    """Gets the DataIndex for the given key columns on the provided table. When the DataIndex already exists, returns it.
    When the DataIndex doesn't already exist, if create_if_absent is True, creates the DataIndex first then returns it;
    otherwise returns None.

    Args:
        table (Table): the table to index
        key_cols (List[str]): the names of the key columns to index
        create_if_absent (bool): if True, create the DataIndex if it does not already exist, default is True

    Returns:
        a DataIndex or None

    Raises:
        DHError
    """
    try:
        if not create_if_absent:
            return _get_data_index(table, key_cols)
        return DataIndex(_JDataIndexer.getOrCreateDataIndex(table.j_table, key_cols))
    except Exception as e:
        raise DHError(e, "failed to create DataIndex.") from e
