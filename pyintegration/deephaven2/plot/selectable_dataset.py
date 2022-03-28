#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module defines the SelectableDateSet class and provides two concrete implementations. """

import jpy

from deephaven2._wrapper_abc import JObjectWrapper

_JSelectableDataSet = jpy.get_type("io.deephaven.plot.filters.SelectableDataSet")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")


class SelectableDataSet(JObjectWrapper):
    """ A SelectableDataSet object provides a view of a table on a selectable subset. """
    j_object_type = _JSelectableDataSet

    def __init__(self, j_sds):
        self.j_sds = j_sds

    @property
    def j_object(self) -> jpy.JType:
        return self.j_sds


class SelectableDataSetOneClick(SelectableDataSet):
    """ TODO This class relies on TableMap which is being reworked right now. """


class SelectableDataSetSwappableTable(SelectableDataSet):
    """ TODO This class relies on TableMap which is being reworked right now. """
