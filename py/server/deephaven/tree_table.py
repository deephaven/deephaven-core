#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module defines the TreeTable and the operations that can be performed on instances of TreeTable."""

from __future__ import annotations

from typing import Sequence, Union

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.filters import Filter
from deephaven.jcompat import to_sequence, j_array_list
from deephaven.rollup_table import FormatOperationsRecorder, SortOperationsRecorder, FilterOperationsRecorder

_JTreeTable = jpy.get_type("io.deephaven.engine.table.hierarchical.TreeTable")
_JNodeOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.TreeTable$NodeOperationsRecorder")


class TreeNodeOperationsRecorder(JObjectWrapper, FormatOperationsRecorder,
                                 SortOperationsRecorder, FilterOperationsRecorder):
    """Recorder for node-level operations to be applied when gathering snapshots of TreeTable. Supported operations
    include column formatting, sorting, and filtering.

    Note: It should not be instantiated directly. User code must call :meth:`~TreeTable.node_operation_recorder` to
    create an instance of the recorder.
    """

    j_object_type = _JNodeOperationsRecorder

    @property
    def j_object(self) -> jpy.JType:
        return self.j_node_ops_recorder

    def __init__(self, j_node_ops_recorder: jpy.JType):
        self.j_node_ops_recorder = j_node_ops_recorder


class TreeTable(JObjectWrapper):
    """ A TreeTable is generated as a result of applying the :meth:`~Table.tree` method on a
    :class:`~deephaven.table.Table`.

    Note: TreeTable should not be instantiated directly by user code.
    """
    j_object_type = _JTreeTable

    @property
    def j_object(self) -> jpy.JType:
        return self.j_tree_table

    def __init__(self, j_tree_table: jpy.JType, id_col: str, parent_col: str):
        self.j_tree_table = j_tree_table
        self.id_col = id_col
        self.parent_col = parent_col

    def node_operation_recorder(self) -> TreeNodeOperationsRecorder:
        """Creates a TreepNodeOperationsRecorder for per-node operations to apply during snapshotting of this TreeTable.

        Returns:
            a TreeNodeOperationsRecorder
        """
        return TreeNodeOperationsRecorder(j_node_ops_recorder=self.j_tree_table.makeNodeOperationsRecorder())

    def with_node_operations(self, node_ops: TreeNodeOperationsRecorder) -> TreeTable:
        """Returns a new TreeTable that will apply the recorded node operations to nodes when gathering snapshots.

        Args:
            node_ops (TreeNodeOperationsRecorder): the TreeNodeOperationsRecorder containing the node operations to be
                applied

        Returns:
            a new TreeTable

        Raises:
            DHError
        """

        try:
            return TreeTable(
                j_tree_table=self.j_tree_table.withNodeOperations(node_ops.j_node_ops_recorder),
                id_col=self.id_col, parent_col=self.parent_col)
        except Exception as e:
            raise DHError(e, "with_node_operations on TreeTable failed.") from e

    def with_filters(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> TreeTable:
        """Applies a set of filters to the columns of this TreeTable to produce and return a new TreeTable

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
                expression(s) or Filter object(s)

        Returns:
            a new TreeTable

        Raises:
            DHError
        """

        try:
            filters = to_sequence(filters)
            if filters and isinstance(filters[0], str):
                filters = Filter.from_(filters)
            filters = j_array_list(filters)

            return TreeTable(j_tree_table=self.j_tree_table.withFilters(filters), id_col=self.id_col,
                             parent_col=self.parent_col)
        except Exception as e:
            raise DHError(e, "with_filters operation on TreeTable failed.") from e
