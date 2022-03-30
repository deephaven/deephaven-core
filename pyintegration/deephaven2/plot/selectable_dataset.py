#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module defines the SelectableDateSet which is used to provides a view of a selectable subset of a table.
For example, in some selectable data sets, a GUI click can be used to select a portion of a table. """

from typing import List

import jpy

from deephaven2 import DHError
from deephaven2._wrapper import JObjectWrapper
from deephaven2.table import Table

_JSelectableDataSet = jpy.get_type("io.deephaven.plot.filters.SelectableDataSet")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")
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


def one_click(t: Table, by: List[str] = None, require_all_filters_to_display: bool = False) -> SelectableDataSet:
    """ Creates a SelectableDataSet with the specified columns from a table.

    Args:
        t (Table): the source table
        by (List[str]): the selected columns
        require_all_filters_to_display (bool): false to display data when not all oneclicks are selected; true to only
            display data when appropriate oneclicks are selected

    Returns:
        a SelectableDataSet

    Raises:
        DHError
    """
    if not by:
        by = []
    try:
        return SelectableDataSet(j_sds=_JSelectables.oneClick(t.j_table, require_all_filters_to_display, *by))
    except Exception as e:
        raise DHError(e, "failed in one_click.") from e


def one_click_table_map(tm: jpy.JType, t: Table, by: List[str] = None,
                        require_all_filters_to_display: bool = False) -> SelectableDataSet:
    """ Creates a SelectableDataSet with the specified columns from the table map.

    Args:
        tm (jpy.JType): the source table map
        t (Table): the source table
        by (List[str]): the selected columns
        require_all_filters_to_display (bool): false to display data when not all oneclicks are selected; true to only
            display data when appropriate oneclicks are selected

    Returns:
        a SelectableDataSet

    Raises:
        DHError
    """
    if not by:
        by = []
    try:
        return SelectableDataSet(j_sds=_JSelectables.oneClick(tm, t.j_table, require_all_filters_to_display, *by))
    except Exception as e:
        raise DHError(e, "failed in one_click.") from e
