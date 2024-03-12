#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implements the Table, PartitionedTable and PartitionedTableProxy classes which are the main
instruments for working with Deephaven refreshing and static data. """

from __future__ import annotations

import contextlib
import inspect
from enum import Enum
from enum import auto
from typing import Any, Optional, Callable, Dict
from typing import Sequence, List, Union, Protocol

import jpy

from deephaven import DHError
from deephaven import dtypes
from deephaven._jpy import strict_cast
from deephaven._wrapper import JObjectWrapper
from deephaven._wrapper import unwrap
from deephaven.agg import Aggregation
from deephaven.column import Column, ColumnType
from deephaven.filters import Filter, and_, or_
from deephaven.jcompat import j_unary_operator, j_binary_operator, j_map_to_dict, j_hashmap
from deephaven.jcompat import to_sequence, j_array_list
from deephaven.update_graph import auto_locking_ctx, UpdateGraph
from deephaven.updateby import UpdateByOperation

# Table
_JTable = jpy.get_type("io.deephaven.engine.table.Table")
_JAttributeMap = jpy.get_type("io.deephaven.engine.table.AttributeMap")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")
_JColumnName = jpy.get_type("io.deephaven.api.ColumnName")
_JSortColumn = jpy.get_type("io.deephaven.api.SortColumn")
_JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
_JFilterOr = jpy.get_type("io.deephaven.api.filter.FilterOr")
_JPair = jpy.get_type("io.deephaven.api.Pair")
_JLayoutHintBuilder = jpy.get_type("io.deephaven.engine.util.LayoutHintBuilder")
_JSearchDisplayMode = jpy.get_type("io.deephaven.engine.util.LayoutHintBuilder$SearchDisplayModes")
_JSnapshotWhenOptions = jpy.get_type("io.deephaven.api.snapshot.SnapshotWhenOptions")
_JBlinkTableTools = jpy.get_type("io.deephaven.engine.table.impl.BlinkTableTools")

# PartitionedTable
_JPartitionedTable = jpy.get_type("io.deephaven.engine.table.PartitionedTable")
_JPartitionedTableFactory = jpy.get_type("io.deephaven.engine.table.PartitionedTableFactory")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")
_JPartitionedTableProxy = jpy.get_type("io.deephaven.engine.table.PartitionedTable$Proxy")
_JJoinMatch = jpy.get_type("io.deephaven.api.JoinMatch")
_JJoinAddition = jpy.get_type("io.deephaven.api.JoinAddition")
_JAsOfJoinRule = jpy.get_type("io.deephaven.api.AsOfJoinRule")
_JTableOperations = jpy.get_type("io.deephaven.api.TableOperations")

# Dynamic Query Scope
_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
_JScriptSessionQueryScope = jpy.get_type("io.deephaven.engine.util.AbstractScriptSession$ScriptSessionQueryScope")
_JPythonScriptSession = jpy.get_type("io.deephaven.integrations.python.PythonDeephavenSession")

# Rollup Table and Tree Table
_JRollupTable = jpy.get_type("io.deephaven.engine.table.hierarchical.RollupTable")
_JTreeTable = jpy.get_type("io.deephaven.engine.table.hierarchical.TreeTable")
_JRollupTableNodeOperationsRecorder = jpy.get_type(
    "io.deephaven.engine.table.hierarchical.RollupTable$NodeOperationsRecorder")
_JTreeTableNodeOperationsRecorder = jpy.get_type(
    "io.deephaven.engine.table.hierarchical.TreeTable$NodeOperationsRecorder")
_JNodeType = jpy.get_type("io.deephaven.engine.table.hierarchical.RollupTable$NodeType")
_JFormatOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.FormatOperationsRecorder")
_JSortOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.SortOperationsRecorder")
_JFilterOperationsRecorder = jpy.get_type("io.deephaven.engine.table.hierarchical.FilterOperationsRecorder")

# MultiJoin Table and input
_JMultiJoinInput = jpy.get_type("io.deephaven.engine.table.MultiJoinInput")
_JMultiJoinTable = jpy.get_type("io.deephaven.engine.table.MultiJoinTable")
_JMultiJoinFactory = jpy.get_type("io.deephaven.engine.table.MultiJoinFactory")


class NodeType(Enum):
    """An enum of node types for RollupTable"""
    AGGREGATED = _JNodeType.Aggregated
    """Nodes at an aggregated (rolled up) level in the RollupTable. An aggregated level is above the constituent (
    leaf) level. These nodes have column names and types that result from applying aggregations on the source table
    of the RollupTable. """
    CONSTITUENT = _JNodeType.Constituent
    """Nodes at the leaf level when :meth:`~deephaven.table.Table.rollup` method is called with
    include_constituent=True. The constituent level is the lowest in a rollup table. These nodes have column names
    and types from the source table of the RollupTable. """


class SearchDisplayMode(Enum):
    """An enum of search display modes for layout hints"""
    DEFAULT = _JSearchDisplayMode.Default
    """Use the system default. This may depend on your user and/or system settings."""
    SHOW = _JSearchDisplayMode.Show
    """Permit the search bar to be displayed, regardless of user or system settings."""
    HIDE = _JSearchDisplayMode.Hide
    """Hide the search bar, regardless of user or system settings."""


class _FormatOperationsRecorder(Protocol):
    """A mixin for creating format operations to be applied to individual nodes of either RollupTable or TreeTable."""

    def format_column(self, formulas: Union[str, List[str]]):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.format_columns` operation applied to nodes."""
        formulas = to_sequence(formulas)
        j_format_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JFormatOperationsRecorder)
        return self.__class__(j_format_ops_recorder.formatColumns(formulas))

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


class _SortOperationsRecorder(Protocol):
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


class _FilterOperationsRecorder(Protocol):
    """A mixin for creating filter operations to be applied to individual nodes of either RollupTable or
    TreeTable."""

    def where(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]):
        """Returns a new recorder with the :meth:`~deephaven.table.Table.where` operation applied to nodes."""
        j_filter_ops_recorder = jpy.cast(self.j_node_ops_recorder, _JFilterOperationsRecorder)
        return self.__class__(j_filter_ops_recorder.where(and_(filters).j_filter))


class RollupNodeOperationsRecorder(JObjectWrapper, _FormatOperationsRecorder,
                                   _SortOperationsRecorder):
    """Recorder for node-level operations to be applied when gathering snapshots of RollupTable. Supported operations
    include column formatting and sorting.

    Note: It should not be instantiated directly. User code must call :meth:`~RollupTable.node_operation_recorder` to
    create an instance of the recorder.
    """

    j_object_type = _JRollupTableNodeOperationsRecorder

    @property
    def j_object(self) -> jpy.JType:
        return self.j_node_ops_recorder

    def __init__(self, j_node_ops_recorder: jpy.JType):
        self.j_node_ops_recorder = j_node_ops_recorder


class RollupTable(JObjectWrapper):
    """ A RollupTable is generated as a result of applying the :meth:`~deephaven.table.Table.rollup` operation on a
    :class:`~deephaven.table.Table`.

    A RollupTable aggregates by the grouping columns, and then creates a hierarchical table which re-aggregates
    using one less grouping column on each level.

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
        """Creates a RollupNodeOperationsRecorder for per-node operations to apply during Deephaven UI driven
        snapshotting of this RollupTable. The recorded node operations will be applied only to the node of the
        provided NodeType. See :class:`NodeType` for details.

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

    def with_node_operations(self, recorders: List[RollupNodeOperationsRecorder]) -> RollupTable:
        """Returns a new RollupTable that will apply the recorded node operations to nodes when gathering
        snapshots requested by the Deephaven UI.

        Args:
            recorders (List[RollupNodeOperationsRecorder]): a list of RollupNodeOperationsRecorder containing
                the node operations to be applied, they must be ones created by calling the 'node_operation_recorder'
                method on the same table.

        Returns:
            a new RollupTable

        Raises:
            DHError
        """
        try:
            return RollupTable(
                j_rollup_table=self.j_rollup_table.withNodeOperations(
                    [op.j_node_ops_recorder for op in recorders]),
                include_constituents=self.include_constituents, aggs=self.aggs, by=self.by)
        except Exception as e:
            raise DHError(e, "with_node_operations on RollupTable failed.") from e

    def with_filters(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> RollupTable:
        """Returns a new RollupTable by applying the given set of filters to the group-by columns of this RollupTable.

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
                expression(s) or Filter object(s)

        Returns:
            a new RollupTable

        Raises:
            DHError
        """
        try:
            return RollupTable(j_rollup_table=self.j_rollup_table.withFilter(and_(filters).j_filter),
                               include_constituents=self.include_constituents, aggs=self.aggs, by=self.by)
        except Exception as e:
            raise DHError(e, "with_filters operation on RollupTable failed.") from e


class TreeNodeOperationsRecorder(JObjectWrapper, _FormatOperationsRecorder,
                                 _SortOperationsRecorder, _FilterOperationsRecorder):
    """Recorder for node-level operations to be applied when gathering snapshots of TreeTable. Supported operations
    include column formatting, sorting, and filtering.

    Note: It should not be instantiated directly. User code must call :meth:`~TreeTable.node_operation_recorder` to
    create an instance of the recorder.
    """

    j_object_type = _JTreeTableNodeOperationsRecorder

    @property
    def j_object(self) -> jpy.JType:
        return self.j_node_ops_recorder

    def __init__(self, j_node_ops_recorder: jpy.JType):
        self.j_node_ops_recorder = j_node_ops_recorder


class TreeTable(JObjectWrapper):
    """ A TreeTable is generated as a result of applying the :meth:`~Table.tree` method on a
    :class:`~deephaven.table.Table`.

    A TreeTable presents a hierarchically structured  "tree" view of a table where parent-child relationships are expressed
    by an "id" and a "parent" column. The id column should represent a unique identifier for a given row, and the parent
    column indicates which row is the parent for a given row.

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
        """Creates a TreepNodeOperationsRecorder for per-node operations to apply during Deephaven UI driven
        snapshotting of this TreeTable.

        Returns:
            a TreeNodeOperationsRecorder
        """
        return TreeNodeOperationsRecorder(j_node_ops_recorder=self.j_tree_table.makeNodeOperationsRecorder())

    def with_node_operations(self, recorder: TreeNodeOperationsRecorder) -> TreeTable:
        """Returns a new TreeTable that will apply the recorded node operations to nodes when gathering snapshots
        requested by the Deephaven UI.

        Args:
            recorder (TreeNodeOperationsRecorder): the TreeNodeOperationsRecorder containing the node operations to be
                applied, it must be created by calling the 'node_operation_recorder' method on the same table.

        Returns:
            a new TreeTable

        Raises:
            DHError
        """

        try:
            return TreeTable(
                j_tree_table=self.j_tree_table.withNodeOperations(recorder.j_node_ops_recorder),
                id_col=self.id_col, parent_col=self.parent_col)
        except Exception as e:
            raise DHError(e, "with_node_operations on TreeTable failed.") from e

    def with_filters(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> TreeTable:
        """Returns a new TreeTable by applying the given set of filters to the columns of this TreeTable.

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
                expression(s) or Filter object(s)

        Returns:
            a new TreeTable

        Raises:
            DHError
        """

        try:
            return TreeTable(j_tree_table=self.j_tree_table.withFilter(and_(filters).j_filter), id_col=self.id_col,
                             parent_col=self.parent_col)
        except Exception as e:
            raise DHError(e, "with_filters operation on TreeTable failed.") from e


def _j_py_script_session() -> _JPythonScriptSession:
    j_execution_context = _JExecutionContext.getContext()
    j_query_scope = j_execution_context.getQueryScope()
    try:
        j_script_session_query_scope = strict_cast(j_query_scope, _JScriptSessionQueryScope)
        return strict_cast(j_script_session_query_scope.scriptSession(), _JPythonScriptSession)
    except DHError:
        return None


@contextlib.contextmanager
def _query_scope_ctx():
    """A context manager to set/unset query scope based on the scope of the most immediate caller code that invokes
    Table operations."""

    # locate the innermost Deephaven frame (i.e. any of the table operation methods that use this context manager)
    outer_frames = inspect.getouterframes(inspect.currentframe())[1:]
    for i, (frame, filename, *_) in enumerate(outer_frames):
        if filename and filename == __file__:
            break

    # combine the immediate caller's globals and locals into a single dict and use it as the query scope
    caller_frame = outer_frames[i + 1].frame
    function = outer_frames[i + 1].function
    j_py_script_session = _j_py_script_session()
    if j_py_script_session and (len(outer_frames) > i + 2 or function != "<module>"):
        scope_dict = caller_frame.f_globals.copy()
        scope_dict.update(caller_frame.f_locals)
        j_py_script_session.pushScope(scope_dict)
        try:
            yield
        finally:
            j_py_script_session.popScope()
    else:
        # in the __main__ module, use the default main global scope
        yield


def _query_scope_agg_ctx(aggs: Sequence[Aggregation]) -> contextlib.AbstractContextManager:
    has_agg_formula = any([agg.is_formula for agg in aggs])
    if has_agg_formula:
        cm = _query_scope_ctx()
    else:
        cm = contextlib.nullcontext()
    return cm


class SortDirection(Enum):
    """An enum defining the sorting orders."""
    DESCENDING = auto()
    """"""
    ASCENDING = auto()
    """"""


def _sort_column(col, dir_):
    return (_JSortColumn.desc(_JColumnName.of(col)) if dir_ == SortDirection.DESCENDING else _JSortColumn.asc(
        _JColumnName.of(col)))


def _td_to_columns(table_definition):
    cols = []
    j_cols = table_definition.getColumnsArray()
    for j_col in j_cols:
        cols.append(
            Column(
                name=j_col.getName(),
                data_type=dtypes.from_jtype(j_col.getDataType()),
                component_type=dtypes.from_jtype(j_col.getComponentType()),
                column_type=ColumnType(j_col.getColumnType()),
            )
        )
    return cols


class Table(JObjectWrapper):
    """A Table represents a Deephaven table. It allows applications to perform powerful Deephaven table operations.

    Note: It should not be instantiated directly by user code. Tables are mostly created by factory methods,
    data ingestion operations, queries, aggregations, joins, etc.

    """
    j_object_type = _JTable

    def __init__(self, j_table: jpy.JType):
        self.j_table = jpy.cast(j_table, self.j_object_type)
        if self.j_table is None:
            raise DHError("j_table type is not io.deephaven.engine.table.Table")
        self._definition = self.j_table.getDefinition()
        self._schema = None
        self._is_refreshing = None
        self._update_graph = None
        self._is_flat = None

    def __repr__(self):
        default_repr = super().__repr__()
        # default_repr is in a format like so:
        # deephaven.table.Table(io.deephaven.engine.table.Table(objectRef=0x7f07e4890518))
        # We take the last two brackets off, add a few more details about the table, then add the necessary brackets back
        column_dict = {col.name: col.data_type for col in self.columns[:10]}
        repr_str = (
            f"{default_repr[:-2]}, num_rows = {self.size}, columns = {column_dict}"
        )
        # We need to add the two brackets back, and also truncate it to be 120 char total (118 + two brackets)
        repr_str = repr_str[:114] + "...}))" if len(repr_str) > 118 else repr_str + "))"
        return repr_str

    def __str__(self):
        return repr(self)

    @property
    def size(self) -> int:
        """The current number of rows in the table."""
        return self.j_table.size()

    @property
    def is_refreshing(self) -> bool:
        """Whether this table is refreshing."""
        if self._is_refreshing is None:
            self._is_refreshing = self.j_table.isRefreshing()
        return self._is_refreshing

    @property
    def is_blink(self) -> bool:
        """Whether this table is a blink table."""
        return _JBlinkTableTools.isBlink(self.j_table)

    @property
    def update_graph(self) -> UpdateGraph:
        """The update graph of the table."""
        if self._update_graph is None:
            self._update_graph = UpdateGraph(self.j_table.getUpdateGraph())
        return self._update_graph

    @property
    def is_flat(self) -> bool:
        """Whether this table is guaranteed to be flat, i.e. its row set will be from 0 to number of rows - 1."""
        if self._is_flat is None:
            self._is_flat = self.j_table.isFlat()
        return self._is_flat

    @property
    def columns(self) -> List[Column]:
        """The column definitions of the table."""
        if self._schema:
            return self._schema

        self._schema = _td_to_columns(self._definition)
        return self._schema

    @property
    def meta_table(self) -> Table:
        """The column definitions of the table in a Table form. """
        return Table(j_table=self.j_table.meta())

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table

    def has_columns(self, cols: Union[str, Sequence[str]]):
        """Whether this table contains a column for each of the provided names, return False if any of the columns is
        not in the table.

        Args:
            cols (Union[str, Sequence[str]]): the column name(s)

        Returns:
            bool
        """
        cols = to_sequence(cols)
        return self.j_table.hasColumns(cols)

    def attributes(self) -> Dict[str, Any]:
        """Returns all the attributes defined on the table."""
        j_map = jpy.cast(self.j_table, _JAttributeMap).getAttributes()
        return j_map_to_dict(j_map)

    def with_attributes(self, attrs: Dict[str, Any]) -> Table:
        """Returns a new Table that has the provided attributes defined on it and shares the underlying data and schema
        with this table.

        Note, the table attributes are immutable once defined, and are mostly used internally by the Deephaven
        engine. For advanced users, certain predefined plug-in attributes provide a way to extend Deephaven with
        custom-built plug-ins.

        Args:
            attrs (Dict[str, Any]): a dict of table attribute names and their values

        Returns:
            a new Table

        Raises:
            DHError
        """
        try:
            j_map = j_hashmap(attrs)
            return Table(j_table=jpy.cast(self.j_table, _JAttributeMap).withAttributes(j_map))
        except Exception as e:
            raise DHError(e, "failed to create a table with attributes.") from e

    def without_attributes(self, attrs: Union[str, Sequence[str]]) -> Table:
        """Returns a new Table that shares the underlying data and schema with this table but with the specified
        attributes removed.

        Args:
            attrs (Union[str, Sequence[str]]): the attribute name(s) to be removed

        Returns:
            a new Table

        Raises:
            DHError
        """
        try:
            attrs = j_array_list(to_sequence(attrs))
            return Table(j_table=jpy.cast(self.j_table, _JAttributeMap).withoutAttributes(attrs))
        except Exception as e:
            raise DHError(e, "failed to create a table without attributes.") from e

    def to_string(self, num_rows: int = 10, cols: Union[str, Sequence[str]] = None) -> str:
        """Returns the first few rows of a table as a pipe-delimited string.

        Args:
            num_rows (int): the number of rows at the beginning of the table
            cols (Union[str, Sequence[str]]): the column name(s), default is None

        Returns:
            string

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return _JTableTools.string(self.j_table, num_rows, *cols)
        except Exception as e:
            raise DHError(e, "table to_string failed") from e

    def coalesce(self) -> Table:
        """Returns a coalesced child table."""
        return Table(j_table=self.j_table.coalesce())

    def flatten(self) -> Table:
        """Returns a new version of this table with a flat row set, i.e. from 0 to number of rows - 1."""
        return Table(j_table=self.j_table.flatten())

    def snapshot(self) -> Table:
        """Returns a static snapshot table.

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(self):
                return Table(j_table=self.j_table.snapshot())
        except Exception as e:
            raise DHError(message="failed to create a snapshot.") from e

    def snapshot_when(self, trigger_table: Table, stamp_cols: Union[str, List[str]] = None, initial: bool = False,
                      incremental: bool = False, history: bool = False) -> Table:
        """Returns a table that captures a snapshot of this table whenever trigger_table updates.

        When trigger_table updates, a snapshot of this table and the "stamp key" from trigger_table form the resulting
        table. The "stamp key" is the last row of the trigger_table, limited by the stamp_cols. If trigger_table is
        empty, the "stamp key" will be represented by NULL values.

        Args:
            trigger_table (Table): the trigger table
            stamp_cols (Union[str, Sequence[str]): The columns from trigger_table that form the "stamp key", may be
                renames. None, or empty, means that all columns from trigger_table form the "stamp key".
            initial (bool): Whether to take an initial snapshot upon construction, default is False. When False, the
                resulting table will remain empty until trigger_table first updates.
            incremental (bool): Whether the resulting table should be incremental, default is False. When False, all
                rows of this table will have the latest "stamp key". When True, only the rows of this table that have
                been added or updated will have the latest "stamp key".
            history (bool): Whether the resulting table should keep history, default is False. A history table appends a
                full snapshot of this table and the "stamp key" as opposed to updating existing rows. The history flag
                is currently incompatible with initial and incremental: when history is True, incremental and initial
                must be False.

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            options = _JSnapshotWhenOptions.of(initial, incremental, history, to_sequence(stamp_cols))
            with auto_locking_ctx(self, trigger_table):
                return Table(j_table=self.j_table.snapshotWhen(trigger_table.j_table, options))
        except Exception as e:
            raise DHError(message="failed to create a snapshot_when table.") from e

    #
    # Table operation category: Select
    #
    # region Select
    def drop_columns(self, cols: Union[str, Sequence[str]]) -> Table:
        """The drop_columns method creates a new table with the same size as this table but omits any of specified
        columns.

        Args:
            cols (Union[str, Sequence[str]): the column name(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return Table(j_table=self.j_table.dropColumns(*cols))
        except Exception as e:
            raise DHError(e, "table drop_columns operation failed.") from e

    def move_columns(self, idx: int, cols: Union[str, Sequence[str]]) -> Table:
        """The move_columns method creates a new table with specified columns moved to a specific column index value.
        Columns may be renamed with the same semantics as rename_columns. The renames are simultaneous and unordered,
        enabling direct swaps between column names. Specifying a source or destination more than once is prohibited.

        Args:
            idx (int): the column index where the specified columns will be moved in the new table.
            cols (Union[str, Sequence[str]]) : the column name(s) or the column rename expr(s) as "X = Y"

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return Table(j_table=self.j_table.moveColumns(idx, *cols))
        except Exception as e:
            raise DHError(e, "table move_columns operation failed.") from e

    def move_columns_down(self, cols: Union[str, Sequence[str]]) -> Table:
        """The move_columns_down method creates a new table with specified columns appearing last in order, to the far
        right. Columns may be renamed with the same semantics as rename_columns. The renames are simultaneous and
        unordered, enabling direct swaps between column names. Specifying a source or destination more than once is
        prohibited.

        Args:
            cols (Union[str, Sequence[str]]) : the column name(s) or the column rename expr(s) as "X = Y"

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return Table(j_table=self.j_table.moveColumnsDown(*cols))
        except Exception as e:
            raise DHError(e, "table move_columns_down operation failed.") from e

    def move_columns_up(self, cols: Union[str, Sequence[str]]) -> Table:
        """The move_columns_up method creates a new table with specified columns appearing first in order, to the far
        left. Columns may be renamed with the same semantics as rename_columns. The renames are simultaneous and
        unordered, enabling direct swaps between column names. Specifying a source or destination more than once is
        prohibited.

        Args:
            cols (Union[str, Sequence[str]]) : the column name(s) or the column rename expr(s) as "X = Y"

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return Table(j_table=self.j_table.moveColumnsUp(*cols))
        except Exception as e:
            raise DHError(e, "table move_columns_up operation failed.") from e

    def rename_columns(self, cols: Union[str, Sequence[str]]) -> Table:
        """The rename_columns method creates a new table with the specified columns renamed. The renames are
        simultaneous and unordered, enabling direct swaps between column names. Specifying a source or
         destination more than once is prohibited.

        Args:
            cols (Union[str, Sequence[str]]) : the column rename expr(s) as "X = Y"

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return Table(j_table=self.j_table.renameColumns(*cols))
        except Exception as e:
            raise DHError(e, "table rename_columns operation failed.") from e

    def update(self, formulas: Union[str, Sequence[str]]) -> Table:
        """The update method creates a new table containing a new, in-memory column for each formula.

        Args:
            formulas (Union[str, Sequence[str]]): the column formula(s)

        Returns:
            A new table

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return Table(j_table=self.j_table.update(*formulas))
        except Exception as e:
            raise DHError(e, "table update operation failed.") from e

    def lazy_update(self, formulas: Union[str, Sequence[str]]) -> Table:
        """The lazy_update method creates a new table containing a new, cached, formula column for each formula.

        Args:
            formulas (Union[str, Sequence[str]]): the column formula(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return Table(j_table=self.j_table.lazyUpdate(*formulas))
        except Exception as e:
            raise DHError(e, "table lazy_update operation failed.") from e

    def view(self, formulas: Union[str, Sequence[str]]) -> Table:
        """The view method creates a new formula table that includes one column for each formula.

        Args:
            formulas (Union[str, Sequence[str]]): the column formula(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx():
                return Table(j_table=self.j_table.view(*formulas))
        except Exception as e:
            raise DHError(e, "table view operation failed.") from e

    def update_view(self, formulas: Union[str, Sequence[str]]) -> Table:
        """The update_view method creates a new table containing a new, formula column for each formula.

        Args:
            formulas (Union[str, Sequence[str]]): the column formula(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx():
                return Table(j_table=self.j_table.updateView(*formulas))
        except Exception as e:
            raise DHError(e, "table update_view operation failed.") from e

    def select(self, formulas: Union[str, Sequence[str]] = None) -> Table:
        """The select method creates a new in-memory table that includes one column for each formula. If no formula
        is specified, all columns will be included.

        Args:
            formulas (Union[str, Sequence[str]], optional): the column formula(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            with _query_scope_ctx(), auto_locking_ctx(self):
                if not formulas:
                    return Table(j_table=self.j_table.select())
                formulas = to_sequence(formulas)
                return Table(j_table=self.j_table.select(*formulas))
        except Exception as e:
            raise DHError(e, "table select operation failed.") from e

    def select_distinct(self, formulas: Union[str, Sequence[str]] = None) -> Table:
        """The select_distinct method creates a new table containing all the unique values for a set of key
        columns. When the selectDistinct method is used on multiple columns, it looks for distinct sets of values in
        the selected columns.

        Args:
            formulas (Union[str, Sequence[str]], optional): the column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx():
                return Table(j_table=self.j_table.selectDistinct(*formulas))
        except Exception as e:
            raise DHError(e, "table select_distinct operation failed.") from e

    # endregion

    #
    # Table operation category: Filter
    #
    # region Filter

    def where(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]] = None) -> Table:
        """The where method creates a new table with only the rows meeting the filter criteria in the column(s) of
        the table.

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
                expression(s) or Filter object(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            filters = to_sequence(filters)
            with _query_scope_ctx():
                return Table(j_table=self.j_table.where(and_(filters).j_filter))
        except Exception as e:
            raise DHError(e, "table where operation failed.") from e

    def where_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> Table:
        """The where_in method creates a new table containing rows from the source table, where the rows match
        values in the filter table. The filter is updated whenever either table changes.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, Sequence[str]]): the column name(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            with auto_locking_ctx(self, filter_table):
                return Table(j_table=self.j_table.whereIn(filter_table.j_table, *cols))
        except Exception as e:
            raise DHError(e, "table where_in operation failed.") from e

    def where_not_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> Table:
        """The where_not_in method creates a new table containing rows from the source table, where the rows do not
        match values in the filter table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, Sequence[str]]): the column name(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            with auto_locking_ctx(self, filter_table):
                return Table(j_table=self.j_table.whereNotIn(filter_table.j_table, *cols))
        except Exception as e:
            raise DHError(e, "table where_not_in operation failed.") from e

    def where_one_of(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]] = None) -> Table:
        """The where_one_of method creates a new table containing rows from the source table, where the rows match at
        least one filter.

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition expression(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            filters = to_sequence(filters)
            with _query_scope_ctx():
                return Table(j_table=self.j_table.where(or_(filters).j_filter))
        except Exception as e:
            raise DHError(e, "table where_one_of operation failed.") from e

    def head(self, num_rows: int) -> Table:
        """The head method creates a new table with a specific number of rows from the beginning of the table.

        Args:
            num_rows (int): the number of rows at the head of table

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.head(num_rows))
        except Exception as e:
            raise DHError(e, "table head operation failed.") from e

    def head_pct(self, pct: float) -> Table:
        """The head_pct method creates a new table with a specific percentage of rows from the beginning of the table.

        Args:
            pct (float): the percentage of rows to return as a value from 0 (0%) to 1 (100%).

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.headPct(pct))
        except Exception as e:
            raise DHError(e, "table head_pct operation failed.") from e

    def tail(self, num_rows: int) -> Table:
        """The tail method creates a new table with a specific number of rows from the end of the table.

        Args:
            num_rows (int): the number of rows at the end of table

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.tail(num_rows))
        except Exception as e:
            raise DHError(e, "table tail operation failed.") from e

    def tail_pct(self, pct: float) -> Table:
        """The tail_pct method creates a new table with a specific percentage of rows from the end of the table.

        Args:
            pct (float): the percentage of rows to return as a value from 0 (0%) to 1 (100%).

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.tailPct(pct))
        except Exception as e:
            raise DHError(e, "table tail_pct operation failed.") from e

    # endregion

    #
    # Table operation category: Sort
    #
    # region Sort
    def restrict_sort_to(self, cols: Union[str, Sequence[str]]):
        """The restrict_sort_to method adjusts the input table to produce an output table that only allows sorting on
        specified table columns. This can be useful to prevent users from accidentally performing expensive sort
        operations as they interact with tables in the UI.

        Args:
            cols (Union[str, Sequence[str]]): the column name(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return Table(self.j_table.restrictSortTo(*cols))
        except Exception as e:
            raise DHError(e, "table restrict_sort_to operation failed.") from e

    def sort_descending(self, order_by: Union[str, Sequence[str]]) -> Table:
        """The sort_descending method creates a new table where rows in a table are sorted in descending order based on
        the order_by column(s).

        Args:
            order_by (Union[str, Sequence[str]], optional): the column name(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            order_by = to_sequence(order_by)
            return Table(j_table=self.j_table.sortDescending(*order_by))
        except Exception as e:
            raise DHError(e, "table sort_descending operation failed.") from e

    def reverse(self) -> Table:
        """The reverse method creates a new table with all of the rows from this table in reverse order.

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.reverse())
        except Exception as e:
            raise DHError(e, "table reverse operation failed.") from e

    def sort(self, order_by: Union[str, Sequence[str]],
             order: Union[SortDirection, Sequence[SortDirection]] = None) -> Table:
        """The sort method creates a new table where the rows are ordered based on values in a specified set of columns.

        Args:
            order_by (Union[str, Sequence[str]]): the column(s) to be sorted on
            order (Union[SortDirection, Sequence[SortDirection], optional): the corresponding sort directions for
                each sort column, default is None, meaning ascending order for all the sort columns.

        Returns:
            a new table

        Raises:
            DHError
        """

        try:
            order_by = to_sequence(order_by)
            if not order:
                order = (SortDirection.ASCENDING,) * len(order_by)
            order = to_sequence(order)
            if len(order_by) != len(order):
                raise DHError(message="The number of sort columns must be the same as the number of sort directions.")

            sort_columns = [_sort_column(col, dir_) for col, dir_ in zip(order_by, order)]
            j_sc_list = j_array_list(sort_columns)
            return Table(j_table=self.j_table.sort(j_sc_list))
        except Exception as e:
            raise DHError(e, "table sort operation failed.") from e

    # endregion

    #
    # Table operation category: Join
    #
    # region Join

    def natural_join(self, table: Table, on: Union[str, Sequence[str]],
                     joins: Union[str, Sequence[str]] = None) -> Table:
        """The natural_join method creates a new table containing all the rows and columns of this table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the key values in the left and right tables are
        equal. If there is no matching key in the right table, appended row values are NULL.

        Args:
            table (Table): the right-table of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            with auto_locking_ctx(self, table):
                if joins:
                    return Table(
                        j_table=self.j_table.naturalJoin(
                            table.j_table, ",".join(on), ",".join(joins)
                        )
                    )
                else:
                    return Table(
                        j_table=self.j_table.naturalJoin(table.j_table, ",".join(on))
                    )
        except Exception as e:
            raise DHError(e, "table natural_join operation failed.") from e

    def exact_join(self, table: Table, on: Union[str, Sequence[str]], joins: Union[str, Sequence[str]] = None) -> Table:
        """The exact_join method creates a new table containing all the rows and columns of this table plus
        additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the key values in the left and right tables are
        equal.

        Args:
            table (Table): the right-table of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            with auto_locking_ctx(self, table):
                if joins:
                    return Table(
                        j_table=self.j_table.exactJoin(
                            table.j_table, ",".join(on), ",".join(joins)
                        )
                    )
                else:
                    return Table(
                        j_table=self.j_table.exactJoin(table.j_table, ",".join(on))
                    )
        except Exception as e:
            raise DHError(e, "table exact_join operation failed.") from e

    def join(self, table: Table, on: Union[str, Sequence[str]] = None,
             joins: Union[str, Sequence[str]] = None) -> Table:
        """The join method creates a new table containing rows that have matching values in both tables. Rows that
        do not have matching criteria will not be included in the result. If there are multiple matches between a row
        from the left table and rows from the right table, all matching combinations will be included. If no columns
        to match (on) are specified, every combination of left and right table rows is included.

        Args:
            table (Table): the right-table of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names; default is None
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            with auto_locking_ctx(self, table):
                if joins:
                    return Table(
                        j_table=self.j_table.join(
                            table.j_table, ",".join(on), ",".join(joins)
                        )
                    )
                else:
                    return Table(j_table=self.j_table.join(table.j_table, ",".join(on)))
        except Exception as e:
            raise DHError(e, "table join operation failed.") from e

    def aj(self, table: Table, on: Union[str, Sequence[str]], joins: Union[str, Sequence[str]] = None) -> Table:
        """The aj (as-of join) method creates a new table containing all the rows and columns of the left table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the keys from the left table most closely match
        the keys from the right table without going over. If there is no matching key in the right table, appended row
        values are NULL.

        Args:
            table (Table): the right-table of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '>' or '>='.  If a common name is used for the inexact match,
                '>=' is used for the comparison.
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = ",".join(to_sequence(on))
            joins = ",".join(to_sequence(joins))
            table_op = jpy.cast(self.j_object, _JTableOperations)
            with auto_locking_ctx(self, table):
                return Table(j_table=table_op.aj(table.j_table, on, joins))
        except Exception as e:
            raise DHError(e, "table as-of join operation failed.") from e

    def raj(self, table: Table, on: Union[str, Sequence[str]], joins: Union[str, Sequence[str]] = None) -> Table:
        """The reverse-as-of join method creates a new table containing all the rows and columns of the left table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the keys from the left table most closely match
        the keys from the right table without going under. If there is no matching key in the right table, appended row
        values are NULL.

        Args:
            table (Table): the right-table of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '<' or '<='.  If a common name is used for the inexact match,
                '<=' is used for the comparison.
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = ",".join(to_sequence(on))
            joins = ",".join(to_sequence(joins))
            table_op = jpy.cast(self.j_object, _JTableOperations)
            with auto_locking_ctx(self, table):
                return Table(j_table=table_op.raj(table.j_table, on, joins))
        except Exception as e:
            raise DHError(e, "table reverse-as-of join operation failed.") from e

    def range_join(self, table: Table, on: Union[str, List[str]], aggs: Union[Aggregation, List[Aggregation]]) -> Table:
        """The range_join method creates a new table containing all the rows and columns of the left table,
        plus additional columns containing aggregated data from the right table. For columns appended to the
        left table (joins), cell values equal aggregations over vectors of values from the right table.
        These vectors are formed from all values in the right table where the right table keys fall within the
        ranges of keys defined by the left table (responsive ranges).

        range_join is a join plus aggregation that (1) joins arrays of data from the right table onto the left table,
        and then (2) aggregates over the joined data. Oftentimes this is used to join data for a particular time range
        from the right table onto the left table.

        Rows from the right table with null or NaN key values are discarded; that is, they are never included in the
        vectors used for aggregation.  For all rows that are not discarded, the right table must be sorted according
        to the right range column for all rows within a group.

        Join key ranges, specified by the 'on' argument, are defined by zero-or-more exact join matches and a single
        range join match. The range join match must be the last match in the list.

        The exact match expressions are parsed as in other join operations. That is, they are either a column name
        common to both tables or a column name from the left table followed by an equals sign followed by a column
        name from the right table.
        Examples:
            Match on the same column name in both tables:
                "common_column"
            Match on different column names in each table:
                "left_column = right_column"
                or
                "left_column == right_column"

        The range match expression is expressed as a ternary logical expression, expressing the relationship between
        the left start column, the right range column, and the left end column. Each column name pair is separated by
        a logical operator, either < or <=. Additionally, the entire expression may be preceded by a left arrow <-
        and/or followed by a right arrow ->.  The arrows indicate that range match can 'allow preceding' or 'allow
        following' to match values outside the explicit range. 'Allow preceding' means that if no matching right
        range column value is equal to the left start column value, the immediately preceding matching right row
        should be included in the aggregation if such a row exists. 'Allow following' means that if no matching right
        range column value is equal to the left end column value, the immediately following matching right row should
        be included in the aggregation if such a row exists.
        Examples:
            For less than paired with greater than:
               "left_start_column < right_range_column < left_end_column"
            For less than or equal paired with greater than or equal:
               "left_start_column <= right_range_column <= left_end_column"
            For less than or equal (allow preceding) paired with greater than or equal (allow following):
               "<- left_start_column <= right_range_column <= left_end_column ->"

        Special Cases
            In order to produce aggregated output, range match expressions must define a range of values to aggregate
            over. There are a few noteworthy special cases of ranges.

            Empty Range
            An empty range occurs for any left row with no matching right rows. That is, no non-null, non-NaN right
            rows were found using the exact join matches, or none were in range according to the range join match.

            Single-value Ranges
            A single-value range is a range where the left row's values for the left start column and left end
            column are equal and both relative matches are inclusive (<= and >=, respectively). For a single-value
            range, only rows within the bucket where the right range column matches the single value are included in
            the output aggregations.

            Invalid Ranges
            An invalid range occurs in two scenarios:
                (1) When the range is inverted, i.e., when the value of the left start column is greater than the value
                    of the left end column.
                (2) When either relative-match is exclusive (< or >) and the value in the left start column is equal to
                    the value in the left end column.
            For invalid ranges, the result row will be null for all aggregation output columns.

            Undefined Ranges
            An undefined range occurs when either the left start column or the left end column is NaN. For rows with an
            undefined range, the corresponding output values will be null (as with invalid ranges).

            Unbounded Ranges
            A partially or fully unbounded range occurs when either the left start column or the left end column is
            null. If the left start column value is null and the left end column value is non-null, the range is
            unbounded at the beginning, and only the left end column subexpression will be used for the match. If the
            left start column value is non-null and the left end column value is null, the range is unbounded at the
            end, and only the left start column subexpression will be used for the match. If the left start column
            and left end column values are null, the range is unbounded, and all rows will be included.

        Note: At this time, implementations only support static tables. This operation remains under active development.

        Args:
            table (Table): the right table of the join
            on (Union[str, List[str]]): the match expression(s) that must include zero-or-more exact match expression,
                and exactly one range match expression as described above
            aggs (Union[Aggregation, List[Aggregation]]): the aggregation(s) to perform over the responsive ranges from
                the right table for each row from this Table

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            aggs = to_sequence(aggs)
            j_agg_list = j_array_list([agg.j_aggregation for agg in aggs])
            return Table(j_table=self.j_table.rangeJoin(table.j_table, j_array_list(on), j_agg_list))
        except Exception as e:
            raise DHError(e, message="table range_join operation failed.") from e

    # endregion

    #
    # Table operation category: Aggregation
    # region Aggregation

    def head_by(self, num_rows: int, by: Union[str, Sequence[str]] = None) -> Table:
        """The head_by method creates a new table containing the first number of rows for each group.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            by (Union[str, Sequence[str]]): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                return Table(j_table=self.j_table.headBy(num_rows, *by))
        except Exception as e:
            raise DHError(e, "table head_by operation failed.") from e

    def tail_by(self, num_rows: int, by: Union[str, Sequence[str]] = None) -> Table:
        """The tail_by method creates a new table containing the last number of rows for each group.

        Args:
            num_rows (int): the number of rows at the end of each group
            by (Union[str, Sequence[str]]): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                return Table(j_table=self.j_table.tailBy(num_rows, *by))
        except Exception as e:
            raise DHError(e, "table tail_by operation failed.") from e

    def group_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The group_by method creates a new table containing grouping columns and grouped data, column content is
        grouped into vectors.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.groupBy(*by))
            else:
                return Table(j_table=self.j_table.groupBy())
        except Exception as e:
            raise DHError(e, "table group-by operation failed.") from e

    def ungroup(self, cols: Union[str, Sequence[str]] = None) -> Table:
        """The ungroup method creates a new table in which array columns from the source table are unwrapped into
        separate rows.

        Args:
            cols (Union[str, Sequence[str]], optional): the name(s) of the array column(s), if None, all array columns
                will be ungrouped, default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            with auto_locking_ctx(self):
                if cols:
                    return Table(j_table=self.j_table.ungroup(*cols))
                else:
                    return Table(j_table=self.j_table.ungroup())
        except Exception as e:
            raise DHError(e, "table ungroup operation failed.") from e

    def first_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The first_by method creates a new table containing the first row for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.firstBy(*by))
            else:
                return Table(j_table=self.j_table.firstBy())
        except Exception as e:
            raise DHError(e, "table first_by operation failed.") from e

    def last_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The last_by method creates a new table containing the last row for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.lastBy(*by))
            else:
                return Table(j_table=self.j_table.lastBy())
        except Exception as e:
            raise DHError(e, "table last_by operation failed.") from e

    def sum_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The sum_by method creates a new table containing the sum for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.sumBy(*by))
            else:
                return Table(j_table=self.j_table.sumBy())
        except Exception as e:
            raise DHError(e, "table sum_by operation failed.") from e

    def abs_sum_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The abs_sum_by method creates a new table containing the absolute sum for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.absSumBy(*by))
            else:
                return Table(j_table=self.j_table.absSumBy())
        except Exception as e:
            raise DHError(e, "table asb_sum_by operation failed.") from e

    def weighted_sum_by(self, wcol: str, by: Union[str, Sequence[str]] = None) -> Table:
        """The weighted_sum_by method creates a new table containing the weighted sum for each group.

        Args:
            wcol (str): the name of the weight column
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.wsumBy(wcol, *by))
            else:
                return Table(j_table=self.j_table.wsumBy(wcol))
        except Exception as e:
            raise DHError(e, "table weighted_sum_by operation failed.") from e

    def avg_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The avg_by method creates a new table containing the average for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.avgBy(*by))
            else:
                return Table(j_table=self.j_table.avgBy())
        except Exception as e:
            raise DHError(e, "table avg_by operation failed.") from e

    def weighted_avg_by(self, wcol: str, by: Union[str, Sequence[str]] = None) -> Table:
        """The weighted_avg_by method creates a new table containing the weighted average for each group.

        Args:
            wcol (str): the name of the weight column
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.wavgBy(wcol, *by))
            else:
                return Table(j_table=self.j_table.wavgBy(wcol))
        except Exception as e:
            raise DHError(e, "table avg_by operation failed.") from e

    def std_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The std_by method creates a new table containing the sample standard deviation for each group.

        Sample standard deviation is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
        which ensures that the sample variance will be an unbiased estimator of population variance.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.stdBy(*by))
            else:
                return Table(j_table=self.j_table.stdBy())
        except Exception as e:
            raise DHError(e, "table std_by operation failed.") from e

    def var_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The var_by method creates a new table containing the sample variance for each group.

        Sample variance is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
        which ensures that the sample variance will be an unbiased estimator of population variance.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.varBy(*by))
            else:
                return Table(j_table=self.j_table.varBy())
        except Exception as e:
            raise DHError(e, "table var_by operation failed.") from e

    def median_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The median_by method creates a new table containing the median for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.medianBy(*by))
            else:
                return Table(j_table=self.j_table.medianBy())
        except Exception as e:
            raise DHError(e, "table median_by operation failed.") from e

    def min_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The min_by method creates a new table containing the minimum value for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.minBy(*by))
            else:
                return Table(j_table=self.j_table.minBy())
        except Exception as e:
            raise DHError(e, "table min_by operation failed.") from e

    def max_by(self, by: Union[str, Sequence[str]] = None) -> Table:
        """The max_by method creates a new table containing the maximum value for each group.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.maxBy(*by))
            else:
                return Table(j_table=self.j_table.maxBy())
        except Exception as e:
            raise DHError(e, "table max_by operation failed.") from e

    def count_by(self, col: str, by: Union[str, Sequence[str]] = None) -> Table:
        """The count_by method creates a new table containing the number of rows for each group.

        Args:
            col (str): the name of the column to store the counts
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            if by:
                return Table(j_table=self.j_table.countBy(col, *by))
            else:
                return Table(j_table=self.j_table.countBy(col))
        except Exception as e:
            raise DHError(e, "table count_by operation failed.") from e

    def agg_by(self, aggs: Union[Aggregation, Sequence[Aggregation]], by: Union[str, Sequence[str]] = None,
               preserve_empty: bool = False, initial_groups: Table = None) -> Table:
        """The agg_by method creates a new table containing grouping columns and grouped data. The resulting
        grouped data is defined by the aggregations specified.

        Args:
            aggs (Union[Aggregation, Sequence[Aggregation]]): the aggregation(s)
            by (Union[str, Sequence[str]]): the group-by column name(s), if not provided, all rows from this table are
                grouped into a single group of rows before the aggregations are applied to the result, default is None.
            preserve_empty (bool): whether to keep result rows for groups that are initially empty or become empty as
                a result of updates. Each aggregation operator defines its own value for empty groups. Default is False.
            initial_groups (Table): a table whose distinct combinations of values for the group-by column(s)
                should be used to create an initial set of aggregation groups. All other columns are ignored. This is
                useful in combination with preserve_empty=True to ensure that particular groups appear in the result
                table, or with preserve_empty=False to control the encounter order for a collection of groups and
                thus their relative order in the result. Changes to this table are not expected or handled; if this
                table is a refreshing table, only its contents at instantiation time will be used. Default is None,
                the result will be the same as if a table is provided but no rows were supplied. When it is provided,
                the 'by' argument must be provided to explicitly specify the grouping columns.
        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            aggs = to_sequence(aggs)
            by = to_sequence(by)
            if not by and initial_groups:
                raise ValueError("missing group-by column names when initial_groups is provided.")
            j_agg_list = j_array_list([agg.j_aggregation for agg in aggs])

            cm = _query_scope_agg_ctx(aggs)
            with cm:
                if not by:
                    return Table(j_table=self.j_table.aggBy(j_agg_list, preserve_empty))
                else:
                    j_column_name_list = j_array_list([_JColumnName.of(col) for col in by])
                    initial_groups = unwrap(initial_groups)
                    return Table(
                        j_table=self.j_table.aggBy(j_agg_list, preserve_empty, initial_groups, j_column_name_list))
        except Exception as e:
            raise DHError(e, "table agg_by operation failed.") from e

    def partitioned_agg_by(self, aggs: Union[Aggregation, Sequence[Aggregation]],
                           by: Union[str, Sequence[str]] = None, preserve_empty: bool = False,
                           initial_groups: Table = None) -> PartitionedTable:
        """The partitioned_agg_by method is a convenience method that performs an agg_by operation on this table and
        wraps the result in a PartitionedTable. If the argument 'aggs' does not include a partition aggregation
        created by calling :py:func:`agg.partition`, one will be added automatically with the default constituent column
        name __CONSTITUENT__.

        Args:
            aggs (Union[Aggregation, Sequence[Aggregation]]): the aggregation(s)
            by (Union[str, Sequence[str]]): the group-by column name(s), default is None
            preserve_empty (bool): whether to keep result rows for groups that are initially empty or become empty as
                a result of updates. Each aggregation operator defines its own value for empty groups. Default is False.
            initial_groups (Table): a table whose distinct combinations of values for the group-by column(s)
                should be used to create an initial set of aggregation groups. All other columns are ignored. This is
                useful in combination with preserve_empty=True to ensure that particular groups appear in the result
                table, or with preserve_empty=False to control the encounter order for a collection of groups and
                thus their relative order in the result. Changes to this table are not expected or handled; if this
                table is a refreshing table, only its contents at instantiation time will be used. Default is None,
                the result will be the same as if a table is provided but no rows were supplied. When it is provided,
                the 'by' argument must be provided to explicitly specify the grouping columns.

        Returns:
            a PartitionedTable

        Raises:
            DHError
        """
        try:
            aggs = to_sequence(aggs)
            by = to_sequence(by)
            j_agg_list = j_array_list([agg.j_aggregation for agg in aggs])
            initial_groups = unwrap(initial_groups)

            cm = _query_scope_agg_ctx(aggs)
            with cm:
                return PartitionedTable(
                    j_partitioned_table=self.j_table.partitionedAggBy(j_agg_list, preserve_empty, initial_groups, *by))
        except Exception as e:
            raise DHError(e, "table partitioned_agg_by operation failed.") from e

    def agg_all_by(self, agg: Aggregation, by: Union[str, Sequence[str]] = None) -> Table:
        """The agg_all_by method creates a new table containing grouping columns and grouped data. The resulting
        grouped data is defined by the aggregation specified.

        Note, because agg_all_by applies the aggregation to all the columns of the table, it will ignore
        any column names specified for the aggregation.

        Args:
            agg (Aggregation): the aggregation
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            cm = _query_scope_agg_ctx([agg])
            with cm:
                return Table(j_table=self.j_table.aggAllBy(agg.j_agg_spec, *by))
        except Exception as e:
            raise DHError(e, "table agg_all_by operation failed.") from e

    # endregion

    def format_columns(self, formulas: Union[str, List[str]]) -> Table:
        """ Applies color formatting to the columns of the table.

        Args:
            formulas (Union[str, List[str]]): formatting string(s) in the form of "column=color_expression"
                where color_expression can be a color name or a Java ternary expression that results in a color.

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            return Table(j_table=self.j_table.formatColumns(formulas))
        except Exception as e:
            raise DHError(e, "failed to color format columns.") from e

    def format_column_where(self, col: str, cond: str, formula: str) -> Table:
        """ Applies color formatting to a column of the table conditionally.

        Args:
            col (str): the column name
            cond (str): the condition expression
            formula (str): the formatting string in the form of assignment expression "column=color expression"
                where color_expression can be a color name or a Java ternary expression that results in a color.

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.formatColumnWhere(col, cond, formula))
        except Exception as e:
            raise DHError(e, "failed to color format column conditionally.") from e

    def format_row_where(self, cond: str, formula: str) -> Table:
        """ Applies color formatting to rows of the table conditionally.

        Args:
            cond (str): the condition expression
            formula (str): the formatting string in the form of assignment expression "column=color expression"
                where color_expression can be a color name or a Java ternary expression that results in a color.

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.formatRowWhere(cond, formula))
        except Exception as e:
            raise DHError(e, "failed to color format rows conditionally.") from e

    def layout_hints(self, front: Union[str, List[str]] = None, back: Union[str, List[str]] = None,
                     freeze: Union[str, List[str]] = None, hide: Union[str, List[str]] = None,
                     column_groups: List[dict] = None, search_display_mode: SearchDisplayMode = None) -> Table:
        """ Sets layout hints on the Table

        Args:
            front (Union[str, List[str]]): the columns to show at the front.
            back (Union[str, List[str]]): the columns to show at the back.
            freeze (Union[str, List[str]]): the columns to freeze to the front.
                These will not be affected by horizontal scrolling.
            hide (Union[str, List[str]]): the columns to hide.
            column_groups (List[Dict]): A list of dicts specifying which columns should be grouped in the UI.
                The dicts can specify the following:

                * name (str): The group name
                * children (List[str]): The column names in the group
                * color (Optional[str]): The hex color string or Deephaven color name
            search_display_mode (SearchDisplayMode): set the search bar to explicitly be accessible or inaccessible,
                or use the system default. :attr:`SearchDisplayMode.SHOW` will show the search bar,
                :attr:`SearchDisplayMode.HIDE` will hide the search bar, and :attr:`SearchDisplayMode.DEFAULT` will
                use the default value configured by the user and system settings.

        Returns:
            a new table with the layout hints set

        Raises:
            DHError
        """
        try:
            _j_layout_hint_builder = _JLayoutHintBuilder.get()

            if front is not None:
                _j_layout_hint_builder.atFront(to_sequence(front))

            if back is not None:
                _j_layout_hint_builder.atBack(to_sequence(back))

            if freeze is not None:
                _j_layout_hint_builder.freeze(to_sequence(freeze))

            if hide is not None:
                _j_layout_hint_builder.hide(to_sequence(hide))

            if column_groups is not None:
                for group in column_groups:
                    _j_layout_hint_builder.columnGroup(group.get("name"), j_array_list(group.get("children")),
                                                       group.get("color", ""))

            if search_display_mode is not None:
                _j_layout_hint_builder.setSearchBarAccess(search_display_mode.value)

        except Exception as e:
            raise DHError(e, "failed to create layout hints") from e

        try:
            return Table(j_table=self.j_table.setLayoutHints(_j_layout_hint_builder.build()))
        except Exception as e:
            raise DHError(e, "failed to set layout hints on table") from e

    def partition_by(self, by: Union[str, Sequence[str]], drop_keys: bool = False) -> PartitionedTable:
        """ Creates a PartitionedTable from this table, partitioned according to the specified key columns.

        Args:
            by (Union[str, Sequence[str]]): the column(s) by which to group data
            drop_keys (bool): whether to drop key columns in the constituent tables, default is False

        Returns:
            A PartitionedTable containing a sub-table for each group

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            return PartitionedTable(j_partitioned_table=self.j_table.partitionBy(drop_keys, *by))
        except Exception as e:
            raise DHError(e, "failed to create a partitioned table.") from e

    def update_by(self, ops: Union[UpdateByOperation, List[UpdateByOperation]],
                  by: Union[str, List[str]] = None) -> Table:
        """Creates a table with additional columns calculated from window-based aggregations of columns in this table.
        The aggregations are defined by the provided operations, which support incremental aggregations over the
        corresponding rows in the table. The aggregations will apply position or time-based windowing and
        compute the results over the entire table or each row group as identified by the provided key columns.

        Args:
            ops (Union[UpdateByOperation, List[UpdateByOperation]]): the update-by operation definition(s)
            by (Union[str, List[str]]): the key column name(s) to group the rows of the table

        Returns:
            a new Table

        Raises:
            DHError
        """
        try:
            ops = to_sequence(ops)
            by = to_sequence(by)
            with auto_locking_ctx(self):
                return Table(j_table=self.j_table.updateBy(j_array_list(ops), *by))
        except Exception as e:
            raise DHError(e, "table update-by operation failed.") from e

    def slice(self, start: int, stop: int) -> Table:
        """Extracts a subset of a table by row positions into a new Table.

        If both the start and the stop are positive, then both are counted from the beginning of the table.
        The start is inclusive, and the stop is exclusive. slice(0, N) is equivalent to :meth:`~Table.head` (N)
        The start must be less than or equal to the stop.

        If the start is positive and the stop is negative, then the start is counted from the beginning of the
        table, inclusively. The stop is counted from the end of the table. For example, slice(1, -1) includes all
        rows but the first and last. If the stop is before the start, the result is an empty table.

        If the start is negative, and the stop is zero, then the start is counted from the end of the table,
        and the end of the slice is the size of the table. slice(-N, 0) is equivalent to :meth:`~Table.tail` (N).

        If the start is negative and the stop is negative, they are both counted from the end of the
        table. For example, slice(-2, -1) returns the second to last row of the table.

        Args:
            start (int): the first row position to include in the result
            stop (int): the last row position to include in the result

        Returns:
            a new Table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.slice(start, stop))
        except Exception as e:
            raise DHError(e, "table slice operation failed.") from e

    def slice_pct(self, start_pct: float, end_pct: float) -> Table:
        """Extracts a subset of a table by row percentages.

        Returns a subset of table in the range [floor(start_pct * size_of_table), floor(end_pct * size_of_table)).
        For example, for a table of size 10, slice_pct(0.1, 0.7) will return a subset from the second row to the seventh
        row. Similarly, slice_pct(0, 1) would return the entire table (because row positions run from 0 to size - 1).
        The percentage arguments must be in range [0, 1], otherwise the function returns an error.

        Args:
            start_pct (float): the starting percentage point (inclusive) for rows to include in the result, range [0, 1]
            end_pct (float): the ending percentage point (exclusive) for rows to include in the result, range [0, 1]

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.slicePct(start_pct, end_pct))
        except Exception as e:
            raise DHError(e, "table slice_pct operation failed.") from e

    def rollup(self, aggs: Union[Aggregation, Sequence[Aggregation]], by: Union[str, Sequence[str]] = None,
               include_constituents: bool = False) -> RollupTable:
        """Creates a rollup table.

        A rollup table aggregates by the specified columns, and then creates a hierarchical table which re-aggregates
        using one less by column on each level. The column that is no longer part of the aggregation key is
        replaced with null on each level.

        Note some aggregations can not be used in creating a rollup tables, these include: group, partition, median,
        pct, weighted_avg

        Args:
            aggs (Union[Aggregation, Sequence[Aggregation]]): the aggregation(s)
            by (Union[str, Sequence[str]]): the group-by column name(s), default is None
            include_constituents (bool): whether to include the constituent rows at the leaf level, default is False

        Returns:
            a new RollupTable

        Raises:
            DHError
        """
        try:
            aggs = to_sequence(aggs)
            by = to_sequence(by)
            j_agg_list = j_array_list([agg.j_aggregation for agg in aggs])

            cm = _query_scope_agg_ctx(aggs)
            with cm:
                if not by:
                    return RollupTable(j_rollup_table=self.j_table.rollup(j_agg_list, include_constituents), aggs=aggs,
                                       include_constituents=include_constituents, by=by)
                else:
                    return RollupTable(j_rollup_table=self.j_table.rollup(j_agg_list, include_constituents, by),
                                       aggs=aggs, include_constituents=include_constituents, by=by)
        except Exception as e:
            raise DHError(e, "table rollup operation failed.") from e

    def tree(self, id_col: str, parent_col: str, promote_orphans: bool = False) -> TreeTable:
        """Creates a hierarchical tree table.

        The structure of the table is encoded by an "id" and a "parent" column. The id column should represent a unique
        identifier for a given row, and the parent column indicates which row is the parent for a given row. Rows that
        have a None parent are part of the "root" table.

        It is possible for rows to be "orphaned" if their parent is non-None and does not exist in the table. These
        rows will not be present in the resulting tree. If this is not desirable, they could be promoted to become
        children of the root table by setting 'promote_orphans' argument to True.

        Args:
            id_col (str): the name of a column containing a unique identifier for a particular row in the table
            parent_col (str): the name of a column containing the parent's identifier, {@code null} for rows that are
                part of the root table
            promote_orphans (bool): whether to promote node tables whose parents don't exist to be children of the
                root node, default is False

        Returns:
            a new TreeTable organized according to the parent-child relationships expressed by id_col and parent_col

        Raises:
            DHError
        """
        try:
            if promote_orphans:
                with auto_locking_ctx(self):
                    j_table = _JTreeTable.promoteOrphans(self.j_table, id_col, parent_col)
            else:
                j_table = self.j_table

            return TreeTable(j_tree_table=j_table.tree(id_col, parent_col), id_col=id_col, parent_col=parent_col)
        except Exception as e:
            raise DHError(e, "table tree operation failed.") from e

    def await_update(self, timeout: int = None) -> bool:
        """Waits until either this refreshing Table is updated or the timeout elapses if provided.

        Args:
            timeout (int): the maximum time to wait in milliseconds, default is None, meaning no timeout

        Returns:
            True when the table is updated or False when the timeout has been reached.

        Raises:
            DHError
        """
        if not self.is_refreshing:
            raise DHError(message="await_update can only be called on refreshing tables.")

        updated = True
        try:
            if timeout is not None:
                updated = self.j_table.awaitUpdate(timeout)
            else:
                self.j_table.awaitUpdate()
        except Exception as e:
            raise DHError(e, "await_update was interrupted.") from e
        else:
            return updated


class PartitionedTable(JObjectWrapper):
    """A partitioned table is a table containing tables, known as constituent tables.
    Each constituent table has the same schema.

    The partitioned table contains:
    1. one column containing constituent tables
    2. key columns (optional)
    3. non-key columns (optional)

    Key values can be used to retrieve constituent tables from the partitioned table and
    can be used to perform operations with other like-keyed partitioned tables.
    """

    j_object_type = _JPartitionedTable

    @property
    def j_object(self) -> jpy.JType:
        return self.j_partitioned_table

    def __init__(self, j_partitioned_table):
        self.j_partitioned_table = j_partitioned_table
        self._schema = None
        self._table = None
        self._key_columns = None
        self._unique_keys = None
        self._constituent_column = None
        self._constituent_changes_permitted = None
        self._is_refreshing = None

    @classmethod
    def from_partitioned_table(cls,
                               table: Table,
                               key_cols: Union[str, List[str]] = None,
                               unique_keys: bool = None,
                               constituent_column: str = None,
                               constituent_table_columns: List[Column] = None,
                               constituent_changes_permitted: bool = None) -> PartitionedTable:
        """Creates a PartitionedTable from the provided underlying partitioned Table.

        Note: key_cols, unique_keys, constituent_column, constituent_table_columns,
        constituent_changes_permitted must either be all None or all have values. When they are None, their values will
        be inferred as follows:

        |    * key_cols: the names of all columns with a non-Table data type
        |    * unique_keys: False
        |    * constituent_column: the name of the first column with a Table data type
        |    * constituent_table_columns: the column definitions of the first cell (constituent table) in the constituent
            column. Consequently, the constituent column can't be empty.
        |    * constituent_changes_permitted: the value of table.is_refreshing


        Args:
            table (Table): the underlying partitioned table
            key_cols (Union[str, List[str]]): the key column name(s) of 'table'
            unique_keys (bool): whether the keys in 'table' are guaranteed to be unique
            constituent_column (str): the constituent column name in 'table'
            constituent_table_columns (List[Column]): the column definitions of the constituent table
            constituent_changes_permitted (bool): whether the values of the constituent column can change

        Returns:
            a PartitionedTable

        Raise:
            DHError
        """
        none_args = [key_cols, unique_keys, constituent_column, constituent_table_columns,
                     constituent_changes_permitted]

        try:
            if all([arg is None for arg in none_args]):
                return PartitionedTable(j_partitioned_table=_JPartitionedTableFactory.of(table.j_table))

            if all([arg is not None for arg in none_args]):
                table_def = _JTableDefinition.of([col.j_column_definition for col in constituent_table_columns])
                j_partitioned_table = _JPartitionedTableFactory.of(table.j_table,
                                                                   j_array_list(to_sequence(key_cols)),
                                                                   unique_keys,
                                                                   constituent_column,
                                                                   table_def,
                                                                   constituent_changes_permitted)
                return PartitionedTable(j_partitioned_table=j_partitioned_table)
        except Exception as e:
            raise DHError(e, "failed to build a PartitionedTable.") from e

        missing_value_args = [arg for arg in none_args if arg is None]
        raise DHError(message=f"invalid argument values, must specify non-None values for {missing_value_args}.")

    @classmethod
    def from_constituent_tables(cls,
                                tables: List[Table],
                                constituent_table_columns: List[Column] = None) -> PartitionedTable:
        """Creates a PartitionedTable with a single column named '__CONSTITUENT__' containing the provided constituent
        tables.

        The result PartitionedTable has no key columns, and both its unique_keys and constituent_changes_permitted
        properties are set to False. When constituent_table_columns isn't provided, it will be set to the column
        definitions of the first table in the provided constituent tables.

        Args:
            tables (List[Table]): the constituent tables
            constituent_table_columns (List[Column]): a list of column definitions compatible with all the constituent
                tables, default is None

        Returns:
            a PartitionedTable

        Raises:
            DHError
        """
        try:
            if not constituent_table_columns:
                return PartitionedTable(j_partitioned_table=_JPartitionedTableFactory.ofTables(to_sequence(tables)))
            else:
                table_def = _JTableDefinition.of([col.j_column_definition for col in constituent_table_columns])
                return PartitionedTable(j_partitioned_table=_JPartitionedTableFactory.ofTables(table_def,
                                                                                               to_sequence(tables)))
        except Exception as e:
            raise DHError(e, "failed to create a PartitionedTable from constituent tables.") from e

    @property
    def table(self) -> Table:
        """The underlying partitioned table."""
        if self._table is None:
            self._table = Table(j_table=self.j_partitioned_table.table())
        return self._table

    @property
    def update_graph(self) -> UpdateGraph:
        """The underlying partitioned table's update graph."""
        return self.table.update_graph

    @property
    def is_refreshing(self) -> bool:
        """Whether the underlying partitioned table is refreshing."""
        if self._is_refreshing is None:
            self._is_refreshing = self.table.is_refreshing
        return self._is_refreshing

    @property
    def key_columns(self) -> List[str]:
        """The partition key column names."""
        if self._key_columns is None:
            self._key_columns = list(self.j_partitioned_table.keyColumnNames().toArray())
        return self._key_columns

    def keys(self) -> Table:
        """Returns a Table containing all the keys of the underlying partitioned table."""
        if self.unique_keys:
            return self.table.view(self.key_columns)
        else:
            return self.table.select_distinct(self.key_columns)

    @property
    def unique_keys(self) -> bool:
        """Whether the keys in the underlying table must always be unique. If keys must be unique, one can expect
        that self.table.select_distinct(self.key_columns) and self.table.view(self.key_columns) operations always
        produce equivalent tables."""
        if self._unique_keys is None:
            self._unique_keys = self.j_partitioned_table.uniqueKeys()
        return self._unique_keys

    @property
    def constituent_column(self) -> str:
        """The name of the column containing constituent tables."""
        if self._constituent_column is None:
            self._constituent_column = self.j_partitioned_table.constituentColumnName()
        return self._constituent_column

    @property
    def constituent_table_columns(self) -> List[Column]:
        """The column definitions for constituent tables. All constituent tables in a partitioned table have the
        same column definitions."""
        if not self._schema:
            self._schema = _td_to_columns(self.j_partitioned_table.constituentDefinition())

        return self._schema

    @property
    def constituent_changes_permitted(self) -> bool:
        """Can the constituents of the underlying partitioned table change?  Specifically, can the values of the
        constituent column change?

        If constituent changes are not permitted, the underlying partitioned table:
        1. has no adds
        2. has no removes
        3. has no shifts
        4. has no modifies that include the constituent column

        Note, it is possible for constituent changes to not be permitted even if constituent tables are refreshing or
        if the underlying partitioned table is refreshing. Also note that the underlying partitioned table must be
        refreshing if it contains any refreshing constituents.
        """
        if self._constituent_changes_permitted is None:
            self._constituent_changes_permitted = self.j_partitioned_table.constituentChangesPermitted()
        return self._constituent_changes_permitted

    def merge(self) -> Table:
        """Makes a new Table that contains all the rows from all the constituent tables. In the merged result,
        data from a constituent table is contiguous, and data from constituent tables appears in the same order the
        constituent table appears in the PartitionedTable. Basically, merge stacks constituent tables on top of each
        other in the same relative order as the partitioned table.

        Returns:
            a Table

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(self):
                return Table(j_table=self.j_partitioned_table.merge())
        except Exception as e:
            raise DHError(e, "failed to merge all the constituent tables.")

    def filter(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]]) -> PartitionedTable:
        """The filter method creates a new partitioned table containing only the rows meeting the filter criteria.
        Filters can not use the constituent column.

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]]): the filter condition  expression(s) or
                Filter object(s)

        Returns:
             a PartitionedTable

        Raises:
            DHError
        """
        filters = to_sequence(filters)
        if isinstance(filters[0], str):
            filters = Filter.from_(filters)
            filters = to_sequence(filters)
        try:
            return PartitionedTable(j_partitioned_table=self.j_partitioned_table.filter(j_array_list(filters)))
        except Exception as e:
            raise DHError(e, "failed to apply filters to the partitioned table.") from e

    def sort(self, order_by: Union[str, Sequence[str]],
             order: Union[SortDirection, Sequence[SortDirection]] = None) -> PartitionedTable:
        """The sort method creates a new partitioned table where the rows are ordered based on values in a specified
        set of columns. Sort can not use the constituent column.

        Args:
            order_by (Union[str, Sequence[str]]): the column(s) to be sorted on.  Can't include the constituent column.
            order (Union[SortDirection, Sequence[SortDirection], optional): the corresponding sort directions for
                each sort column, default is None, meaning ascending order for all the sort columns.

        Returns:
            a new PartitionedTable

        Raises:
            DHError
        """

        try:
            order_by = to_sequence(order_by)
            if not order:
                order = (SortDirection.ASCENDING,) * len(order_by)
            order = to_sequence(order)
            if len(order_by) != len(order):
                raise DHError(message="The number of sort columns must be the same as the number of sort directions.")

            sort_columns = [_sort_column(col, dir_) for col, dir_ in zip(order_by, order)]
            j_sc_list = j_array_list(sort_columns)
            return PartitionedTable(j_partitioned_table=self.j_partitioned_table.sort(j_sc_list))
        except Exception as e:
            raise DHError(e, "failed to sort the partitioned table.") from e

    def get_constituent(self, key_values: Union[Any, Sequence[Any]]) -> Optional[Table]:
        """Gets a single constituent table by its corresponding key column value(s).
        If there are no matching rows, the result is None. If there are multiple matching rows, a DHError is thrown.

        Args:
            key_values (Union[Any, Sequence[Any]]): the value(s) of the key column(s)

        Returns:
            a Table or None

        Raises:
            DHError
        """
        try:
            key_values = to_sequence(key_values)
            j_table = self.j_partitioned_table.constituentFor(key_values)
            if j_table:
                return Table(j_table=j_table)
            else:
                return None
        except Exception as e:
            raise DHError(e, "unable to get constituent table.") from e

    @property
    def constituent_tables(self) -> List[Table]:
        """Returns all the current constituent tables."""
        return list(map(Table, self.j_partitioned_table.constituents()))

    def transform(self, func: Callable[[Table], Table],
                  dependencies: Optional[Sequence[Union[Table, PartitionedTable]]] = None) -> PartitionedTable:
        """Apply the provided function to all constituent Tables and produce a new PartitionedTable with the results
        as its constituents, with the same data for all other columns in the underlying partitioned Table. Note that
        if the Table underlying this PartitionedTable changes, a corresponding change will propagate to the result.

        Args:
            func (Callable[[Table], Table]): a function which takes a Table as input and returns a new Table
            dependencies (Optional[Sequence[Union[Table, PartitionedTable]]]): additional dependencies that must be
                satisfied before applying the provided transform function to added or modified constituents during
                update processing. If the transform function uses any other refreshing Table or refreshing Partitioned
                Table, they must be included in this argument. Defaults to None.

        Returns:
            a PartitionedTable

        Raises:
            DHError
        """
        try:
            j_operator = j_unary_operator(func, dtypes.from_jtype(Table.j_object_type.jclass))
            dependencies = to_sequence(dependencies, wrapped=True)
            j_dependencies = [d.j_table for d in dependencies if isinstance(d, Table) and d.is_refreshing]
            j_dependencies.extend([d.table.j_table for d in dependencies if isinstance(d, PartitionedTable) and d.is_refreshing])
            with auto_locking_ctx(self, *dependencies):
                j_pt = self.j_partitioned_table.transform(j_operator, j_dependencies)
                return PartitionedTable(j_partitioned_table=j_pt)
        except Exception as e:
            raise DHError(e, "failed to transform the PartitionedTable.") from e

    def partitioned_transform(self, other: PartitionedTable, func: Callable[[Table, Table], Table],
                              dependencies: Optional[Sequence[Union[Table, PartitionedTable]]] = None) -> \
            PartitionedTable:
        """Join the underlying partitioned Tables from this PartitionedTable and other on the key columns, then apply
        the provided function to all pairs of constituent Tables with the same keys in order to produce a new
        PartitionedTable with the results as its constituents, with the same data for all other columns in the
        underlying partitioned Table from this.

        Note that if the Tables underlying this PartitionedTable or other change, a corresponding change will propagate
        to the result.

        Args:
            other (PartitionedTable): the other Partitioned table whose constituent tables will be passed in as the 2nd
                argument to the provided function
            func (Callable[[Table, Table], Table]): a function which takes two Tables as input and returns a new Table
            dependencies (Optional[Sequence[Union[Table, PartitionedTable]]]): additional dependencies that must be
                satisfied before applying the provided transform function to added, modified, or newly-matched
                constituents during update processing. If the transform function uses any other refreshing Table or
                refreshing Partitioned Table, they must be included in this argument. Defaults to None.

        Returns:
            a PartitionedTable

        Raises:
            DHError
        """
        try:
            j_operator = j_binary_operator(func, dtypes.from_jtype(Table.j_object_type.jclass))
            dependencies = to_sequence(dependencies, wrapped=True)
            j_dependencies = [d.j_table for d in dependencies if isinstance(d, Table) and d.is_refreshing]
            j_dependencies.extend([d.table.j_table for d in dependencies if isinstance(d, PartitionedTable) and d.is_refreshing])
            with auto_locking_ctx(self, other, *dependencies):
                j_pt = self.j_partitioned_table.partitionedTransform(other.j_partitioned_table, j_operator,
                                                                     j_dependencies)
                return PartitionedTable(j_partitioned_table=j_pt)
        except Exception as e:
            raise DHError(e, "failed to transform the PartitionedTable with another PartitionedTable.") from e

    def proxy(self, require_matching_keys: bool = True, sanity_check_joins: bool = True) -> PartitionedTableProxy:
        """Makes a proxy that allows table operations to be applied to the constituent tables of this
        PartitionedTable.

        Args:
            require_matching_keys (bool): whether to ensure that both partitioned tables have all the same keys
                present when an operation uses this PartitionedTable and another PartitionedTable as inputs for a
                :meth:`~PartitionedTable.partitioned_transform`, default is True
            sanity_check_joins (bool): whether to check that for proxied join operations, a given join key only occurs
                in exactly one constituent table of the underlying partitioned table. If the other table argument is also a
                PartitionedTableProxy, its constituents will also be subjected to this constraint.
        """
        return PartitionedTableProxy(
            j_pt_proxy=self.j_partitioned_table.proxy(require_matching_keys, sanity_check_joins))


class PartitionedTableProxy(JObjectWrapper):
    """A PartitionedTableProxy is a table operation proxy for the underlying partitioned table. It provides methods
    that apply table operations to the constituent tables of the underlying partitioned table, produce a new
    partitioned table from the resulting constituent tables, and return a proxy of it.

    Attributes:
        target (PartitionedTable): the underlying partitioned table of the proxy
        require_matching_keys (bool): whether to ensure that both partitioned tables have all the same keys
            present when an operation uses this PartitionedTable and another PartitionedTable as inputs for a
            :meth:`~PartitionedTable.partitioned_transform`, default is True
        sanity_check_joins (bool): whether to check that for proxied join operations, a given join key only occurs
            in exactly one constituent table of the underlying partitioned table. If the other table argument is also a
            PartitionedTableProxy, its constituents will also be subjected to this constraint.
    """
    j_object_type = _JPartitionedTableProxy

    @property
    def j_object(self) -> jpy.JType:
        return self.j_pt_proxy

    @property
    def is_refreshing(self) -> bool:
        """Whether this proxy represents a refreshing partitioned table."""
        return self.target.is_refreshing

    @property
    def update_graph(self) -> UpdateGraph:
        """The underlying partitioned table proxy's update graph."""
        return self.target.update_graph

    def __init__(self, j_pt_proxy):
        self.j_pt_proxy = jpy.cast(j_pt_proxy, _JPartitionedTableProxy)
        self.require_matching_keys = self.j_pt_proxy.requiresMatchingKeys()
        self.sanity_check_joins = self.j_pt_proxy.sanityChecksJoins()
        self.target = PartitionedTable(j_partitioned_table=self.j_pt_proxy.target())

    def head(self, num_rows: int) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.head` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            num_rows (int): the number of rows at the head of the constituent tables

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.head(num_rows))
        except Exception as e:
            raise DHError(e, "head operation on the PartitionedTableProxy failed.") from e

    def tail(self, num_rows: int) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.tail` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            num_rows (int): the number of rows at the end of the constituent tables

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.tail(num_rows))
        except Exception as e:
            raise DHError(e, "tail operation on the PartitionedTableProxy failed.") from e

    def reverse(self) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.reverse` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.reverse())
        except Exception as e:
            raise DHError(e, "reverse operation on the PartitionedTableProxy failed.") from e

    def snapshot(self) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.snapshot` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.snapshot())
        except Exception as e:
            raise DHError(e, "snapshot operation on the PartitionedTableProxy failed.") from e

    def snapshot_when(self, trigger_table: Union[Table, PartitionedTableProxy],
                      stamp_cols: Union[str, List[str]] = None, initial: bool = False, incremental: bool = False,
                      history: bool = False) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.snapshot_when` table operation to all constituent tables of the underlying
        partitioned table with the provided trigger table or PartitionedTableProxy, and produces a new
        PartitionedTableProxy with the result tables as the constituents of its underlying partitioned table.

        In the case of the trigger table being another PartitionedTableProxy, the :meth:`~Table.snapshot_when` table
        operation is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            trigger_table (Union[Table, PartitionedTableProxy]): the trigger Table or PartitionedTableProxy
            stamp_cols (Union[str, Sequence[str]): The columns from trigger_table that form the "stamp key", may be
                renames. None, or empty, means that all columns from trigger_table form the "stamp key".
            initial (bool): Whether to take an initial snapshot upon construction, default is False. When False, the
                resulting table will remain empty until trigger_table first updates.
            incremental (bool): Whether the resulting table should be incremental, default is False. When False, all
                rows of this table will have the latest "stamp key". When True, only the rows of this table that have
                been added or updated will have the latest "stamp key".
            history (bool): Whether the resulting table should keep history, default is False. A history table appends a
                full snapshot of this table and the "stamp key" as opposed to updating existing rows. The history flag
                is currently incompatible with initial and incremental: when history is True, incremental and initial
                must be False.
        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            options = _JSnapshotWhenOptions.of(initial, incremental, history, to_sequence(stamp_cols))
            with auto_locking_ctx(self, trigger_table):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.snapshotWhen(trigger_table.j_object, options))
        except Exception as e:
            raise DHError(e, "snapshot_when operation on the PartitionedTableProxy failed.") from e

    def sort(self, order_by: Union[str, Sequence[str]],
             order: Union[SortDirection, Sequence[SortDirection]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.sort` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            order_by (Union[str, Sequence[str]]): the column(s) to be sorted on
            order (Union[SortDirection, Sequence[SortDirection], optional): the corresponding sort directions for
                each sort column, default is None, meaning ascending order for all the sort columns.

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            order_by = to_sequence(order_by)
            if not order:
                order = (SortDirection.ASCENDING,) * len(order_by)
            order = to_sequence(order)
            if len(order_by) != len(order):
                raise ValueError("The number of sort columns must be the same as the number of sort directions.")

            sort_columns = [_sort_column(col, dir_) for col, dir_ in zip(order_by, order)]
            j_sc_list = j_array_list(sort_columns)
            with auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.sort(j_sc_list))
        except Exception as e:
            raise DHError(e, "sort operation on the PartitionedTableProxy failed.") from e

    def sort_descending(self, order_by: Union[str, Sequence[str]]) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.sort_descending` table operation to all constituent tables of the underlying
        partitioned table, and produces a new PartitionedTableProxy with the result tables as the constituents of its
        underlying partitioned table.

        Args:
            order_by (Union[str, Sequence[str]]): the column(s) to be sorted on

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            order_by = to_sequence(order_by)
            with auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.sortDescending(*order_by))
        except Exception as e:
            raise DHError(e, "sort_descending operation on the PartitionedTableProxy failed.") from e

    def where(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.where` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
                expression(s) or Filter object(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            filters = to_sequence(filters)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.where(*filters))
        except Exception as e:
            raise DHError(e, "where operation on the PartitionedTableProxy failed.") from e

    def where_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.where_in` table operation to all constituent tables of the underlying
        partitioned table with the provided filter table, and produces a new PartitionedTableProxy with the result
        tables as the constituents of its underlying partitioned table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, Sequence[str]]): the column name(s)

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            with auto_locking_ctx(self, filter_table):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.whereIn(filter_table.j_table, *cols))
        except Exception as e:
            raise DHError(e, "where_in operation on the PartitionedTableProxy failed.") from e

    def where_not_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.where_not_in` table operation to all constituent tables of the underlying
        partitioned table with the provided filter table, and produces a new PartitionedTableProxy with the result
        tables as the constituents of its underlying partitioned table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, Sequence[str]]): the column name(s)

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            with auto_locking_ctx(self, filter_table):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.whereNotIn(filter_table.j_table, *cols))
        except Exception as e:
            raise DHError(e, "where_not_in operation on the PartitionedTableProxy failed.") from e

    def view(self, formulas: Union[str, Sequence[str]]) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.view` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            formulas (Union[str, Sequence[str]]): the column formula(s)

        Returns:
            A new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.view(*formulas))
        except Exception as e:
            raise DHError(e, "view operation on the PartitionedTableProxy failed.") from e

    def update_view(self, formulas: Union[str, Sequence[str]]) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.update_view` table operation to all constituent tables of the underlying
        partitioned table, and produces a new PartitionedTableProxy with the result tables as the constituents of its
        underlying partitioned table.

        Args:
            formulas (Union[str, Sequence[str]]): the column formula(s)

        Returns:
            A new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.updateView(*formulas))
        except Exception as e:
            raise DHError(e, "update_view operation on the PartitionedTableProxy failed.") from e

    def update(self, formulas: Union[str, Sequence[str]]) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.update` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            formulas (Union[str, Sequence[str]]): the column formula(s)

        Returns:
            A new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.update(*formulas))
        except Exception as e:
            raise DHError(e, "update operation on the PartitionedTableProxy failed.") from e

    def select(self, formulas: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.select` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            formulas (Union[str, Sequence[str]], optional): the column formula(s), default is None

        Returns:
            A new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.select(*formulas))
        except Exception as e:
            raise DHError(e, "select operation on the PartitionedTableProxy failed.") from e

    def select_distinct(self, formulas: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.select_distinct` table operation to all constituent tables of the underlying
        partitioned table, and produces a new PartitionedTableProxy with the result tables as the constituents of its
        underlying partitioned table.

        Args:
            formulas (Union[str, Sequence[str]], optional): the column formula(s), default is None

        Returns:
            A new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            formulas = to_sequence(formulas)
            with _query_scope_ctx(), auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.selectDistinct(*formulas))
        except Exception as e:
            raise DHError(e, "select_distinct operation on the PartitionedTableProxy failed.") from e

    def natural_join(self, table: Union[Table, PartitionedTableProxy], on: Union[str, Sequence[str]],
                     joins: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.natural_join` table operation to all constituent tables of the underlying
        partitioned table with the provided right table or PartitionedTableProxy, and produces a new
        PartitionedTableProxy with the result tables as the constituents of its underlying partitioned table.

        In the case of the right table being another PartitionedTableProxy, the :meth:`~Table.natural_join` table
        operation is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            table (Union[Table, PartitionedTableProxy]): the right table or PartitionedTableProxy of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            table_op = jpy.cast(table.j_object, _JTableOperations)
            with auto_locking_ctx(self, table):
                if joins:
                    return PartitionedTableProxy(
                        j_pt_proxy=self.j_pt_proxy.naturalJoin(table_op, ",".join(on), ",".join(joins)))
                else:
                    return PartitionedTableProxy(
                        j_pt_proxy=self.j_pt_proxy.naturalJoin(table_op, ",".join(on)))
        except Exception as e:
            raise DHError(e, "natural_join operation on the PartitionedTableProxy failed.") from e

    def exact_join(self, table: Union[Table, PartitionedTableProxy], on: Union[str, Sequence[str]],
                   joins: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.exact_join` table operation to all constituent tables of the underlying
        partitioned table with the provided right table or PartitionedTableProxy,and produces a new
        PartitionedTableProxy with the result tables as the constituents of its underlying partitioned table.

        In the case of the right table being another PartitionedTableProxy, the :meth:`~Table.exact_join` table
        operation is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            table (Union[Table, PartitionedTableProxy]): the right table or PartitionedTableProxy of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            table_op = jpy.cast(table.j_object, _JTableOperations)
            with auto_locking_ctx(self, table):
                if joins:
                    return PartitionedTableProxy(
                        j_pt_proxy=self.j_pt_proxy.exactJoin(table_op, ",".join(on), ",".join(joins)))
                else:
                    return PartitionedTableProxy(
                        j_pt_proxy=self.j_pt_proxy.exactJoin(table_op, ",".join(on)))
        except Exception as e:
            raise DHError(e, "exact_join operation on the PartitionedTableProxy failed.") from e

    def join(self, table: Union[Table, PartitionedTableProxy], on: Union[str, Sequence[str]] = None,
             joins: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.join` table operation to all constituent tables of the underlying partitioned
        table with the provided right table or PartitionedTableProxy, and produces a new PartitionedTableProxy with
        the result tables as the constituents of its underlying partitioned table.

        In the case of the right table being another PartitionedTableProxy, the :meth:`~Table.join` table operation
        is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            table (Union[Table, PartitionedTableProxy]): the right table or PartitionedTableProxy of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names; default is None
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            table_op = jpy.cast(table.j_object, _JTableOperations)
            with auto_locking_ctx(self, table):
                if joins:
                    return PartitionedTableProxy(
                        j_pt_proxy=self.j_pt_proxy.join(table_op, ",".join(on), ",".join(joins)))
                else:
                    return PartitionedTableProxy(
                        j_pt_proxy=self.j_pt_proxy.join(table_op, ",".join(on)))
        except Exception as e:
            raise DHError(e, "join operation on the PartitionedTableProxy failed.") from e

    def aj(self, table: Union[Table, PartitionedTableProxy], on: Union[str, Sequence[str]],
           joins: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.aj` table operation to all constituent tables of the underlying partitioned
        table with the provided right table or PartitionedTableProxy, and produces a new PartitionedTableProxy with
        the result tables as the constituents of its underlying partitioned table.

        In the case of the right table being another PartitionedTableProxy, the :meth:`~Table.aj` table operation
        is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            table (Union[Table, PartitionedTableProxy]): the right table or PartitionedTableProxy of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '>' or '>='.  If a common name is used for the inexact match,
                '>=' is used for the comparison.
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            on = ",".join(to_sequence(on))
            joins = ",".join(to_sequence(joins))
            table_op = jpy.cast(self.j_object, _JTableOperations)
            r_table_op = jpy.cast(table.j_object, _JTableOperations)

            with auto_locking_ctx(self, table):
                return PartitionedTableProxy(j_pt_proxy=table_op.aj(r_table_op, on, joins))
        except Exception as e:
            raise DHError(e, "as-of join operation on the PartitionedTableProxy failed.") from e

    def raj(self, table: Union[Table, PartitionedTableProxy], on: Union[str, Sequence[str]],
            joins: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.raj` table operation to all constituent tables of the underlying partitioned
        table with the provided right table or PartitionedTableProxy, and produces a new PartitionedTableProxy with
        the result tables as the constituents of its underlying partitioned table.

        In the case of the right table being another PartitionedTableProxy, the :meth:`~Table.raj` table operation
        is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            table (Union[Table, PartitionedTableProxy]): the right table or PartitionedTableProxy of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '<' or '<='.  If a common name is used for the inexact match,
                '<=' is used for the comparison.
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            on = ",".join(to_sequence(on))
            joins = ",".join(to_sequence(joins))
            table_op = jpy.cast(self.j_object, _JTableOperations)
            r_table_op = jpy.cast(table.j_object, _JTableOperations)

            with auto_locking_ctx(self, table):
                return PartitionedTableProxy(j_pt_proxy=table_op.raj(r_table_op, on, joins))
        except Exception as e:
            raise DHError(e, "reverse as-of join operation on the PartitionedTableProxy failed.") from e

    def group_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.group_by` table operation to all constituent tables of the underlying
        partitioned table, and produces a new PartitionedTableProxy with the result tables as the constituents of its
        underlying partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.groupBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.groupBy())
        except Exception as e:
            raise DHError(e, "group-by operation on the PartitionedTableProxy failed.") from e

    def agg_by(self, aggs: Union[Aggregation, Sequence[Aggregation]],
               by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.agg_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            aggs (Union[Aggregation, Sequence[Aggregation]]): the aggregation(s)
            by (Union[str, Sequence[str]]): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            aggs = to_sequence(aggs)
            by = to_sequence(by)
            j_agg_list = j_array_list([agg.j_aggregation for agg in aggs])

            cm = _query_scope_agg_ctx(aggs)
            with cm:
                with auto_locking_ctx(self):
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.aggBy(j_agg_list, *by))
        except Exception as e:
            raise DHError(e, "agg_by operation on the PartitionedTableProxy failed.") from e

    def agg_all_by(self, agg: Aggregation, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.agg_all_by` table operation to all constituent tables of the underlying
        partitioned table, and produces a new PartitionedTableProxy with the result tables as the constituents of its
        underlying partitioned table.

        Note, because agg_all_by applies the aggregation to all the columns of the table, it will ignore
        any column names specified for the aggregation.

        Args:
            agg (Aggregation): the aggregation
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)

            cm = _query_scope_agg_ctx([agg])
            with cm:
                with auto_locking_ctx(self):
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.aggAllBy(agg.j_agg_spec, *by))
        except Exception as e:
            raise DHError(e, "agg_all_by operation on the PartitionedTableProxy failed.") from e

    def count_by(self, col: str, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.count_by` table operation to all constituent tables of the underlying partitioned
        table with the provided source table, and produces a new PartitionedTableProxy with the result tables as the
        constituents of its underlying partitioned table.

        Args:
            col (str): the name of the column to store the counts
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.countBy(col, *by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.countBy(col))
        except Exception as e:
            raise DHError(e, "count_by operation on the PartitionedTableProxy failed.") from e

    def first_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.first_by` table operation to all constituent tables of the underlying
        partitioned table, and produces a new PartitionedTableProxy with the result tables as the constituents of its
        underlying partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.firstBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.firstBy())
        except Exception as e:
            raise DHError(e, "first_by operation on the PartitionedTableProxy failed.") from e

    def last_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.last_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.lastBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.lastBy())
        except Exception as e:
            raise DHError(e, "last_by operation on the PartitionedTableProxy failed.") from e

    def min_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.min_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.minBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.minBy())
        except Exception as e:
            raise DHError(e, "min_by operation on the PartitionedTableProxy failed.") from e

    def max_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.max_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.maxBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.maxBy())
        except Exception as e:
            raise DHError(e, "max_by operation on the PartitionedTableProxy failed.") from e

    def sum_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.sum_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.sumBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.sumBy())
        except Exception as e:
            raise DHError(e, "sum_by operation on the PartitionedTableProxy failed.") from e

    def abs_sum_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.abs_sum_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.absSumBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.absSumBy())
        except Exception as e:
            raise DHError(e, "sum_by operation on the PartitionedTableProxy failed.") from e

    def weighted_sum_by(self, wcol: str, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.weighted_sum_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            wcol (str): the name of the weight column
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.wsumBy(wcol, *by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.wsumBy(wcol))
        except Exception as e:
            raise DHError(e, "sum_by operation on the PartitionedTableProxy failed.") from e

    def avg_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.avg_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.avgBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.avgBy())
        except Exception as e:
            raise DHError(e, "avg_by operation on the PartitionedTableProxy failed.") from e

    def weighted_avg_by(self, wcol: str, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.weighted_avg_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            wcol (str): the name of the weight column
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.wavgBy(wcol, *by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.avgBy(wcol))
        except Exception as e:
            raise DHError(e, "avg_by operation on the PartitionedTableProxy failed.") from e

    def median_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.median_by` table operation to all constituent tables of the underlying
        partitioned table, and produces a new PartitionedTableProxy with the result tables as the constituents of its
        underlying partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.medianBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.medianBy())
        except Exception as e:
            raise DHError(e, "median_by operation on the PartitionedTableProxy failed.") from e

    def std_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.std_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.stdBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.stdBy())
        except Exception as e:
            raise DHError(e, "std_by operation on the PartitionedTableProxy failed.") from e

    def var_by(self, by: Union[str, Sequence[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.var_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            by (Union[str, Sequence[str]], optional): the group-by column name(s), default is None

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            by = to_sequence(by)
            with auto_locking_ctx(self):
                if by:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.varBy(*by))
                else:
                    return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.varBy())
        except Exception as e:
            raise DHError(e, "var_by operation on the PartitionedTableProxy failed.") from e

    def update_by(self, ops: Union[UpdateByOperation, List[UpdateByOperation]],
                  by: Union[str, List[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.update_by` table operation to all constituent tables of the underlying partitioned
        table, and produces a new PartitionedTableProxy with the result tables as the constituents of its underlying
        partitioned table.

        Args:
            ops (Union[UpdateByOperation, List[UpdateByOperation]]): the update-by operation definition(s)
            by (Union[str, List[str]]): the key column name(s) to group the rows of the table

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            ops = to_sequence(ops)
            by = to_sequence(by)
            with auto_locking_ctx(self):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.updateBy(j_array_list(ops), *by))
        except Exception as e:
            raise DHError(e, "update-by operation on the PartitionedTableProxy failed.") from e


class MultiJoinInput(JObjectWrapper):
    """A MultiJoinInput represents the input tables, key columns and additional columns to be used in the multi-table
    natural join. """
    j_object_type = _JMultiJoinInput

    @property
    def j_object(self) -> jpy.JType:
        return self.j_multijoininput

    def __init__(self, table: Table, on: Union[str, Sequence[str]], joins: Union[str, Sequence[str]] = None):
        """Creates a new MultiJoinInput containing the table to include for the join, the key columns from the table to
        match with other table keys plus additional columns containing data from the table. Rows containing unique keys
        will be added to the output table, otherwise the data from these columns will be added to the existing output
        rows.

        Args:
            table (Table): the right table to include in the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the this table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Raises:
            DHError
        """
        try:
            self.table = table
            on = to_sequence(on)
            joins = to_sequence(joins)
            self.j_multijoininput = _JMultiJoinInput.of(table.j_table, on, joins)
        except Exception as e:
            raise DHError(e, "failed to build a MultiJoinInput object.") from e


class MultiJoinTable(JObjectWrapper):
    """A MultiJoinTable is an object that contains the result of a multi-table natural join. To retrieve the underlying
    result Table, use the table() method. """
    j_object_type = _JMultiJoinTable

    @property
    def j_object(self) -> jpy.JType:
        return self.j_multijointable

    def table(self) -> Table:
        """Returns the Table containing the multi-table natural join output. """
        return Table(j_table=self.j_multijointable.table())

    def __init__(self, input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
                 on: Union[str, Sequence[str]] = None):
        """Creates a new MultiJoinTable. The join can be specified in terms of either tables or MultiJoinInputs.

        Args:
            input (Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]): the input objects
                specifying the tables and columns to include in the join.
            on (Union[str, Sequence[str]], optional): the column(s) to match, can be a common name or an equality
                expression that matches every input table, i.e. "col_a = col_b" to rename output column names. Note:
                When MultiJoinInput objects are supplied, this parameter must be omitted.

        Raises:
            DHError
        """
        try:
            if isinstance(input, Table) or (isinstance(input, Sequence) and all(isinstance(t, Table) for t in input)):
                tables = to_sequence(input, wrapped=True)
                with auto_locking_ctx(*tables):
                    j_tables = to_sequence(input)
                    self.j_multijointable = _JMultiJoinFactory.of(on, *j_tables)
            elif isinstance(input, MultiJoinInput) or (
                    isinstance(input, Sequence) and all(isinstance(ji, MultiJoinInput) for ji in input)):
                if on is not None:
                    raise DHError(message="on parameter is not permitted when MultiJoinInput objects are provided.")
                wrapped_input = to_sequence(input, wrapped=True)
                tables = [ji.table for ji in wrapped_input]
                with auto_locking_ctx(*tables):
                    input = to_sequence(input)
                    self.j_multijointable = _JMultiJoinFactory.of(*input)
            else:
                raise DHError(
                    message="input must be a Table, a sequence of Tables, a MultiJoinInput, or a sequence of MultiJoinInputs.")

        except Exception as e:
            raise DHError(e, "failed to build a MultiJoinTable object.") from e


def multi_join(input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
               on: Union[str, Sequence[str]] = None) -> MultiJoinTable:
    """ The multi_join method creates a new table by performing a multi-table natural join on the input tables.  The result
    consists of the set of distinct keys from the input tables natural joined to each input table. Input tables need not
    have a matching row for each key, but they may not have multiple matching rows for a given key.

    Args:
        input (Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]): the input objects specifying the
            tables and columns to include in the join.
        on (Union[str, Sequence[str]], optional): the column(s) to match, can be a common name or an equality expression
            that matches every input table, i.e. "col_a = col_b" to rename output column names. Note: When
            MultiJoinInput objects are supplied, this parameter must be omitted.

    Returns:
        MultiJoinTable: the result of the multi-table natural join operation. To access the underlying Table, use the
            table() method.
    """
    return MultiJoinTable(input, on)
