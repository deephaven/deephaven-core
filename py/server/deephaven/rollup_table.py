#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module defines the RollupTable and the operations that can be performed on instances of RollupTable."""

from __future__ import annotations

from enum import Enum
from typing import Sequence, List, Union, Protocol

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.agg import Aggregation
from deephaven.filters import Filter
from deephaven.jcompat import to_sequence, j_array_list

_JRollupTable = jpy.get_type("io.deephaven.engine.table.hierarchical.RollupTable")
_JNodeOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.RollupTable$NodeOperationsRecorder")
_JNodeType = jpy.get_type("io.deephaven.engine.table.hierarchical.RollupTable$NodeType")
_JFormatOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.FormatOperationsRecorder")
_JSortOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.SortOperationsRecorder")
_JFilterOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.FilterOperationsRecorder")


class NodeType(Enum):
    """An enum of node types for RollupTable"""
    AGGREGATED = _JNodeType.Aggregated
    """Nodes at one of the aggregated (rolled up) levels in the RollupTable. These nodes have column names and types 
    that result from applying aggregations on the source table of the RollupTable."""
    CONSTITUENT = _JNodeType.Constituent
    """Nodes at the leaf level when meth:`~deephaven.table.Table.rollup` method is called with 
    include_constituent=True. These nodes have column names and types from the source table of the RollupTable."""


class FormatOperationsRecorder(Protocol):
    """A mixin for creating format operations to be applied to individual nodes of either RollupTable or TreeTable."""

    def format_column(self, col_formats: Union[str, List[str]]):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.format_columns` operation applied to nodes."""
        col_formats = to_sequence(col_formats)
        j_format_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JFormatOperationsRecorder)
        return self.__class__(j_format_ops_recorder.formatColumns(col_formats))

    def format_row_where(self, cond: str, formula: str):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.format_row_where` operation applied to
        nodes."""
        j_format_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JFormatOperationsRecorder)
        return self.__class__(j_format_ops_recorder.formatRowWhere(cond, formula))

    def format_column_where(self, col: str, cond: str, formula: str):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.format_column_where` operation applied to
        nodes."""
        j_format_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JFormatOperationsRecorder)
        return self.__class__(j_format_ops_recorder.formatColumnWhere(col, cond, formula))


class SortOperationsRecorder(Protocol):
    """A mixin for creating sort operations to be applied to individual nodes of either RollupTable or
    TreeTable."""

    def sort(self, order_by: Union[str, Sequence[str]]):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.sort` operation applied to nodes."""
        order_by = to_sequence(order_by)
        j_sort_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JSortOperationsRecorder)
        return self.__class__(j_sort_ops_recorder.sort(order_by))

    def sort_descending(self, order_by: Union[str, Sequence[str]]):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.sort_descending` applied to nodes."""
        order_by = to_sequence(order_by)
        j_sort_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JSortOperationsRecorder)
        return self.__class__(j_sort_ops_recorder.sortDescending(order_by))


class FilterOperationsRecorder(Protocol):
    """A mixin for creating filter operations to be applied to individual nodes of either RollupTable or
    TreeTable."""

    def where(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.where` operation applied to nodes."""
        filters = to_sequence(filters)
        j_filter_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JFilterOperationsRecorder)
        return self.__class__(j_filter_ops_recorder.where(filters))


class RollupNodeOperationsRecorder(JObjectWrapper, FormatOperationsRecorder,
                                   SortOperationsRecorder):
    """Recorder for node-level operations to be applied when gathering snapshots of RollupTable. Supported operations
    include column formatting and sorting.

    Note: It should not be instantiated directly. User code must call :meth:`~RollupTable.node_operation_recorder` to
    create an instance of the recorder.
    """

    j_object_type = _JNodeOperationsRecorder

    @property
    def j_object(self) -> jpy.JType:
        return self.j_node_ops_recorder

    def __init__(self, j_node_ops_recorder: jpy.JType):
        self.j_node_ops_recorder = j_node_ops_recorder


class RollupTable(JObjectWrapper):
    """ A RollupTable is generated as a result of applying the :meth:`~deephaven.table.Table.rollup` operation on a
    :class:`~deephaven.table.Table`.

    Note: RollupTable should not be instantiated directly by user code.
    """
    j_object_type = _JRollupTable

    @property
    def j_object(self) -> jpy.JType:
        return self.j_rollup_table

    def __init__(self, j_rollup_table: jpy.JType, aggs: Sequence[Aggregation], include_constituents: bool,
                 by: Sequence[str]):
        self.j_rollup_table = j_rollup_table
        self.aggs = aggs
        self.include_constituents = include_constituents
        self.by = by

    def node_operation_recorder(self, node_type: NodeType) -> RollupNodeOperationsRecorder:
        """Creates a RollupNodeOperationsRecorder for per-node operations to apply during snapshotting of this
        RollupTable. The recorded node operations will be applied only to the node of the provided NodeType. See
        :class:`NodeType` for details.

        Args:
            node_type (NodeType): the type of node tables that the recorded operations will be applied to; if it is
                :attr:`NodeType.CONSTITUENT`, the RollupTable must be created with include_constituents=True.

        Returns:
            a RollupNodeOperationsRecorder

        Raises:
            DHError
        """
        try:
            return RollupNodeOperationsRecorder(j_node_ops_recorder=self.j_rollup_table.makeNodeOperationsRecorder(
                node_type.value))
        except Exception as e:
            raise DHError(e, "failed to create a RollupNodeOperationsRecorder.") from e

    def with_node_operations(self, node_ops_recorders: List[RollupNodeOperationsRecorder]) -> RollupTable:
        """Returns a new RollupTable that will apply the recorded node operations to nodes when gathering
        snapshots.
        
        Args:
            node_ops_recorders (List[RollupNodeOperationsRecorder]): a list of RollupNodeOperationsRecorder containing
                the node operations to be applied

        Returns:
            a new RollupTable

        Raises:
            DHError
        """
        try:
            return RollupTable(
                j_rollup_table=self.j_rollup_table.withNodeOperations(
                    [op.j_node_ops_recorder for op in node_ops_recorders]),
                include_constituents=self.include_constituents, aggs=self.aggs, by=self.by)
        except Exception as e:
            raise DHError(e, "with_node_operations on RollupTable failed.") from e

    def with_filters(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> RollupTable:
        """Applies a set of filters to the group-by columns of this RollupTable to produce and return a new
        RollupTable

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
                expression(s) or Filter object(s)

        Returns:
            a new RollupTable

        Raises:
            DHError
        """
        try:
            filters = to_sequence(filters)
            if filters and isinstance(filters[0], str):
                filters = Filter.from_(filters)
            filters = j_array_list(filters)

            return RollupTable(j_rollup_table=self.j_rollup_table.withFilters(filters),
                               include_constituents=self.include_constituents, aggs=self.aggs, by=self.by)
        except Exception as e:
            raise DHError(e, "with_filters operation on RollupTable failed.") from e
