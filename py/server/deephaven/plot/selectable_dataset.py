#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines the SelectableDateSet which is used to provides a view of a selectable subset of a table.
For example, in some selectable data sets, a GUI click can be used to select a portion of a table. """

from typing import List

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.table import Table, PartitionedTable

_JSelectableDataSet = jpy.get_type("io.deephaven.plot.filters.SelectableDataSet")
_JSelectables = jpy.get_type("io.deephaven.plot.filters.Selectables")


class SelectableDataSet(JObjectWrapper):
    """ A SelectableDataSet provides a view of a selectable subset of a table.  For example, in some selectable data
    sets, a GUI click can be used to select a portion of a table. """

    j_object_type = _JSelectableDataSet

    def __init__(self, j_sds: jpy.JType):
        self.j_sds = j_sds

    @property
    def j_object(self) -> jpy.JType:
        return self.j_sds


def one_click(t: Table, by: List[str] = None, require_all_filters: bool = False) -> SelectableDataSet:
    """ Creates a SelectableDataSet with the specified columns from a table.

    Args:
        t (Table): the source table
        by (List[str]): the selected columns
        require_all_filters (bool): false to display data when not all oneclicks are selected; true to only
            display data when appropriate oneclicks are selected

    Returns:
        a SelectableDataSet

    Raises:
        DHError
    """
    if not by:
        by = []
    try:
        return SelectableDataSet(j_sds=_JSelectables.oneClick(t.j_table, require_all_filters, *by))
    except Exception as e:
        raise DHError(e, "failed in one_click.") from e


def one_click_partitioned_table(pt: PartitionedTable, require_all_filters: bool = False) -> SelectableDataSet:
    """ Creates a SelectableDataSet with the specified columns from the table map.

    Args:
        pt (PartitionedTable): the source partitioned table
        require_all_filters (bool): false to display data when not all oneclicks are selected; true to only
            display data when appropriate oneclicks are selected

    Returns:
        a SelectableDataSet

    Raises:
        DHError
    """
    try:
        return SelectableDataSet(j_sds=_JSelectables.oneClick(pt.j_partitioned_table, require_all_filters))
    except Exception as e:
        raise DHError(e, "failed in one_click.") from e
