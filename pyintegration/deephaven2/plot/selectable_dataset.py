#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module defines the SelectableDateSet which is used to provides a view of a selectable subset of a table.
For example, in some selectable data sets, a GUI click can be used to select a portion of a table. """

import jpy

from deephaven2._wrapper import JObjectWrapper

_JSelectableDataSet = jpy.get_type("io.deephaven.plot.filters.SelectableDataSet")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")


class SelectableDataSet(JObjectWrapper):
    """ A SelectableDataSet provides a view of a selectable subset of a table.  For example, in some selectable data
    sets, a GUI click can be used to select a portion of a table. """

    j_object_type = _JSelectableDataSet

    def __init__(self, j_sds: jpy.JType):
        self.j_sds = j_sds

    @property
    def j_object(self) -> jpy.JType:
        return self.j_sds
