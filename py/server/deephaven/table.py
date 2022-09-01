#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module implements the Table, PartitionedTable and PartitionedTableProxy classes which are the main
instruments for working with Deephaven refreshing and static data. """

from __future__ import annotations

import contextlib
import inspect
from enum import Enum, auto
from typing import Union, Sequence, List, Any, Optional, Callable

import jpy

from deephaven import DHError, dtypes
from deephaven._wrapper import JObjectWrapper
from deephaven.agg import Aggregation
from deephaven.column import Column, ColumnType
from deephaven.filters import Filter
from deephaven.jcompat import j_array_list, to_sequence, j_unary_operator, j_binary_operator
from deephaven.ugp import auto_locking_ctx
from deephaven.updateby import UpdateByOperation

# Table
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")
_JColumnName = jpy.get_type("io.deephaven.api.ColumnName")
_JSortColumn = jpy.get_type("io.deephaven.api.SortColumn")
_JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
_JFilterOr = jpy.get_type("io.deephaven.api.filter.FilterOr")
_JAsOfMatchRule = jpy.get_type("io.deephaven.engine.table.Table$AsOfMatchRule")
_JPair = jpy.get_type("io.deephaven.api.agg.Pair")
_JMatchPair = jpy.get_type("io.deephaven.engine.table.MatchPair")
_JLayoutHintBuilder = jpy.get_type("io.deephaven.engine.util.LayoutHintBuilder")

# PartitionedTable
_JPartitionedTable = jpy.get_type("io.deephaven.engine.table.PartitionedTable")
_JPartitionedTableFactory = jpy.get_type("io.deephaven.engine.table.PartitionedTableFactory")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")
_JPartitionedTableProxy = jpy.get_type("io.deephaven.engine.table.PartitionedTable$Proxy")
_JJoinMatch = jpy.get_type("io.deephaven.api.JoinMatch")
_JJoinAddition = jpy.get_type("io.deephaven.api.JoinAddition")
_JAsOfJoinRule = jpy.get_type("io.deephaven.api.AsOfJoinRule")
_JReverseAsOfJoinRule = jpy.get_type("io.deephaven.api.ReverseAsOfJoinRule")
_JTableOperations = jpy.get_type("io.deephaven.api.TableOperations")

# Dynamic Query Scope
_JExecutionContext = jpy.get_type("io.deephaven.engine.context.ExecutionContext")
_JQueryScope = jpy.get_type("io.deephaven.engine.context.QueryScope")
_JUnsynchronizedScriptSessionQueryScope = jpy.get_type(
    "io.deephaven.engine.util.AbstractScriptSession$UnsynchronizedScriptSessionQueryScope")
_JPythonScriptSession = jpy.get_type("io.deephaven.integrations.python.PythonDeephavenSession")
_j_script_session = jpy.cast(_JExecutionContext.getContext().getQueryScope(), _JUnsynchronizedScriptSessionQueryScope).scriptSession()
_j_py_script_session = jpy.cast(_j_script_session, _JPythonScriptSession)


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
    if len(outer_frames) > i + 2 or function != "<module>":
        scope_dict = caller_frame.f_globals.copy()
        scope_dict.update(caller_frame.f_locals)
        try:
            _j_py_script_session.pushScope(scope_dict)
            yield
        finally:
            _j_py_script_session.popScope()
    else:
        # in the __main__ module, use the default main global scope
        yield


class SortDirection(Enum):
    """An enum defining the sorting orders."""
    DESCENDING = auto()
    """"""
    ASCENDING = auto()
    """"""


class AsOfMatchRule(Enum):
    """An enum defining matching rules on the final column to match by in as-of join and reverse as-of join
    operation. """
    LESS_THAN_EQUAL = _JAsOfMatchRule.LESS_THAN_EQUAL
    LESS_THAN = _JAsOfMatchRule.LESS_THAN
    GREATER_THAN_EQUAL = _JAsOfMatchRule.GREATER_THAN_EQUAL
    GREATER_THAN = _JAsOfMatchRule.GREATER_THAN


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

    j_object_type = jpy.get_type("io.deephaven.engine.table.Table")

    def __init__(self, j_table: jpy.JType):
        self.j_table = jpy.cast(j_table, self.j_object_type)
        if self.j_table is None:
            raise DHError("j_table type is not io.deephaven.engine.table.Table")
        self._definition = self.j_table.getDefinition()
        self._schema = None
        self._is_refreshing = None

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
    def columns(self) -> List[Column]:
        """The column definitions of the table."""
        if self._schema:
            return self._schema

        self._schema = _td_to_columns(self._definition)
        return self._schema

    @property
    def meta_table(self) -> Table:
        """The column definitions of the table in a Table form. """
        return Table(j_table=self.j_table.getMeta())

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table

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

    def snapshot(self, source_table: Table, do_init: bool = False, cols: Union[str, List[str]] = None) -> Table:
        """Produces an in-memory copy of a source table that refreshes when this table changes.

        Note, this table is often a time table that adds new rows at a regular, user-defined interval.

        Args:
            source_table (Table): the table to be snapshot
            do_init (bool): whether to snapshot when this method is initially called, default is False
            cols (Union[str, List[str]]): names of the columns of this table to be included in the snapshot, default is
                None, meaning all the columns

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            with auto_locking_ctx(self, source_table):
                return Table(j_table=self.j_table.snapshot(source_table.j_table, do_init, *cols))
        except Exception as e:
            raise DHError(message="failed to create a snapshot table.") from e

    def snapshot_history(self, source_table: Table) -> Table:
        """Produces an in-memory history of a source table that adds a new snapshot when this table (trigger table)
        changes.

        The trigger table is often a time table that adds new rows at a regular, user-defined interval.

        Columns from the trigger table appear in the result table. If the trigger and source tables have columns with
        the same name, an error will be raised. To avoid this problem, rename conflicting columns.

        Because snapshot_history stores a copy of the source table for every trigger event, large source tables or
        rapidly changing trigger tables can result in large memory usage.

        Args:
            source_table (Table): the table to be snapshot

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(self, source_table):
                return Table(j_table=self.j_table.snapshotHistory(source_table.j_table))
        except Exception as e:
            raise DHError(message="failed to create a snapshot history table.") from e

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

        Args:
            idx (int): the column index where the specified columns will be moved in the new table.
            cols (Union[str, Sequence[str]]) : the column name(s)

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
        right.

        Args:
            cols (Union[str, Sequence[str]]) : the column name(s)

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
        left.

        Args:
            cols (Union[str, Sequence[str]]) : the column name(s)

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
        """The rename_columns method creates a new table with the specified columns renamed.

        Args:
            cols (Union[str, Sequence[str]]) : the column rename expr(s) as "X = Y"

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            with auto_locking_ctx(self):
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
        """The select_distinct method creates a new table containing all of the unique values for a set of key
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
                return Table(j_table=self.j_table.where(*filters))
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

    def where_one_of(self, filters: Union[str, Sequence[str]] = None) -> Table:
        """The where_one_of method creates a new table containing rows from the source table, where the rows match at
        least one filter.

        Args:
            filters (Union[str, Sequence[str]], optional): the filter condition expression(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            filters = to_sequence(filters)
            with _query_scope_ctx():
                return Table(
                    j_table=self.j_table.where(_JFilterOr.of(_JFilter.from_(*filters)))
                )
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
        """The restrict_sort_to method only allows sorting on specified table columns. This can be useful to prevent
        users from accidentally performing expensive sort operations as they interact with tables in the UI.

        Args:
            cols (Union[str, Sequence[str]]): the column name(s)

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            return self.j_table.restrictSortTo(*cols)
        except Exception as e:
            raise DHError(e, "table restrict_sort_to operation failed.") from e

    def sort_descending(self, order_by: Union[str, Sequence[str]]) -> Table:
        """The sort_descending method creates a new table where rows in a table are sorted in a largest to smallest
        order based on the order_by column(s).

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
        """The natural_join method creates a new table containing all of the rows and columns of this table,
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
        """The exact_join method creates a new table containing all of the rows and columns of this table plus
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

    def aj(self, table: Table, on: Union[str, Sequence[str]], joins: Union[str, Sequence[str]] = None,
           match_rule: AsOfMatchRule = AsOfMatchRule.LESS_THAN_EQUAL) -> Table:
        """The aj (as-of join) method creates a new table containing all of the rows and columns of the left table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the keys from the left table most closely match
        the keys from the right table without going over. If there is no matching key in the right table, appended row
        values are NULL.

        Args:
            table (Table): the right-table of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            match_rule (AsOfMatchRule): the inexact matching rule on the last column to match specified in 'on',
                default is AsOfMatchRule.LESS_THAN_EQUAL. The other valid value is AsOfMatchRule.LESS_THAN.
        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            if on:
                on = [_JMatchPair.of(_JPair.parse(p)) for p in on]
            if joins:
                joins = [_JMatchPair.of(_JPair.parse(p)) for p in joins]
            with auto_locking_ctx(self, table):
                return Table(j_table=self.j_table.aj(table.j_table, on, joins, match_rule.value))
        except Exception as e:
            raise DHError(e, "table as-of join operation failed.") from e

    def raj(self, table: Table, on: Union[str, Sequence[str]], joins: Union[str, Sequence[str]] = None,
            match_rule: AsOfMatchRule = AsOfMatchRule.GREATER_THAN_EQUAL) -> Table:
        """The reverse-as-of join method creates a new table containing all of the rows and columns of the left table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the keys from the left table most closely match
        the keys from the right table without going under. If there is no matching key in the right table, appended row
        values are NULL.

        Args:
            table (Table): the right-table of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            match_rule (AsOfMatchRule): the inexact matching rule on the last column to match specified in 'on',
                default is AsOfMatchRule.GREATER_THAN_EQUAL. The other valid value is AsOfMatchRule.GREATER_THAN.

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            on = to_sequence(on)
            joins = to_sequence(joins)
            if on:
                on = [_JMatchPair.of(_JPair.parse(p)) for p in on]
            if joins:
                joins = [_JMatchPair.of(_JPair.parse(p)) for p in joins]
            with auto_locking_ctx(self, table):
                return Table(j_table=self.j_table.raj(table.j_table, on, joins, match_rule.value))
        except Exception as e:
            raise DHError(e, "table reverse-as-of join operation failed.") from e

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
        grouped into arrays.

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
        """The std_by method creates a new table containing the standard deviation for each group.

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
        """The var_by method creates a new table containing the variance for each group.

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

    def agg_by(self, aggs: Union[Aggregation, Sequence[Aggregation]], by: Union[str, Sequence[str]] = None) -> Table:
        """The agg_by method creates a new table containing grouping columns and grouped data. The resulting
        grouped data is defined by the aggregations specified.

        Args:
            aggs (Union[Aggregation, Sequence[Aggregation]]): the aggregation(s)
            by (Union[str, Sequence[str]]): the group-by column name(s), default is None

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            aggs = to_sequence(aggs)
            by = to_sequence(by)
            j_agg_list = j_array_list([agg.j_aggregation for agg in aggs])
            return Table(j_table=self.j_table.aggBy(j_agg_list, *by))
        except Exception as e:
            raise DHError(e, "table agg_by operation failed.") from e

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
                     column_groups: List[dict] = None) -> Table:
        """ Sets layout hints on the Table

        Args:
            front (Union[str, List[str]]): the columns to show at the front.
            back (Union[str, List[str]]): the columns to show at the back.
            freeze (Union[str, List[str]]): the columns to freeze to the front.
                These will not be affected by horizontal scrolling.
            hide (Union[str, List[str]]): the columns to hide.
            column_groups (List[Dict]): A list of dicts specifying which columns should be grouped in the UI
                The dicts can specify the following:

                name (str): The group name
                children (List[str]): The
                color (Optional[str]): The hex color string or Deephaven color name

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
        corresponding rows in the this table. The aggregations will apply position or time-based windowing and
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
            return Table(j_table=self.j_table.updateBy(j_array_list(ops), *by))
        except Exception as e:
            raise DHError(e, "table update-by operation failed.") from e


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
            key_cols: the names of all columns with a non-Table data type
            unique_keys: False
            constituent_column: the name of the first column with a Table data type
            constituent_table_columns: the column definitions of the first cell (constituent table) in the constituent
                column. Consequently the constituent column can't be empty
            constituent_changes_permitted: the value of table.is_refreshing


        Args:
            table (Table): the underlying partitioned table
            key_cols (Union[str, List[str]]): the key column name(s) of 'table'
            unique_keys (bool): whether the keys in 'table' are guaranteed to be unique
            constituent_column (str): the constituent column name in 'table'
            constituent_table_columns (list[Column]): the column definitions of the constituent table
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
        """The column definitions for constituent tables.  All constituent tables in a partitioned table have the
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

    def transform(self, func: Callable[[Table], Table]) -> PartitionedTable:
        """Apply the provided function to all constituent Tables and produce a new PartitionedTable with the results
        as its constituents, with the same data for all other columns in the underlying partitioned Table. Note that
        if the Table underlying this PartitionedTable changes, a corresponding change will propagate to the result.

        Args:
            func (Callable[[Table], Table]: a function which takes a Table as input and returns a new Table

        Returns:
            a PartitionedTable

        Raises:
            DHError
        """
        try:
            j_operator = j_unary_operator(func, dtypes.from_jtype(Table.j_object_type.jclass))
            with auto_locking_ctx(self):
                j_pt = self.j_partitioned_table.transform(j_operator)
                return PartitionedTable(j_partitioned_table=j_pt)
        except Exception as e:
            raise DHError(e, "failed to transform the PartitionedTable.") from e

    def partitioned_transform(self, other: PartitionedTable, func: Callable[[Table, Table], Table]) -> PartitionedTable:
        """Join the underlying partitioned Tables from this PartitionedTable and other on the key columns, then apply
        the provided function to all pairs of constituent Tables with the same keys in order to produce a new
        PartitionedTable with the results as its constituents, with the same data for all other columns in the
        underlying partitioned Table from this.

        Note that if the Tables underlying this PartitionedTable or other change, a corresponding change will propagate
        to the result.

        Args:
            other (PartitionedTable): the other Partitioned table whose constituent tables will be passed in as the 2nd
                argument to the provided function
            func (Callable[[Table, Table], Table]: a function which takes two Tables as input and returns a new Table

        Returns:
            a PartitionedTable

        Raises:
            DHError
        """
        try:
            j_operator = j_binary_operator(func, dtypes.from_jtype(Table.j_object_type.jclass))
            with auto_locking_ctx(self, other):
                j_pt = self.j_partitioned_table.partitionedTransform(other.j_partitioned_table, j_operator)
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

    def snapshot(self, source_table: Union[Table, PartitionedTableProxy], do_init: bool = False,
                 cols: Union[str, List[str]] = None) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.snapshot` table operation to all constituent tables of the underlying
        partitioned table with the provided source table or PartitionedTableProxy, and produces a new
        PartitionedTableProxy with the result tables as the constituents of its underlying partitioned table.

        In the case of source table being another PartitionedTableProxy, the :meth:`~Table.snapshot` table operation
        is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Note, the constituent tables are often time tables that add new rows at a regular, user-defined interval.

        Args:
            source_table (Union[Table, PartitionedTableProxy]): the table or PartitionedTableProxy to be snapshot
            do_init (bool): whether to snapshot when this method is initially called, default is False
            cols (Union[str, List[str]]): names of the columns of the constituent table to be included in the snapshot,
                default is None, meaning all the columns

        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            cols = to_sequence(cols)
            table_op = jpy.cast(source_table.j_object, _JTableOperations)
            with auto_locking_ctx(self, source_table):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.snapshot(table_op, do_init, *cols))
        except Exception as e:
            raise DHError(e, "snapshot operation on the PartitionedTableProxy failed.") from e

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
           joins: Union[str, Sequence[str]] = None,
           match_rule: AsOfMatchRule = AsOfMatchRule.LESS_THAN_EQUAL) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.aj` table operation to all constituent tables of the underlying partitioned
        table with the provided right table or PartitionedTableProxy, and produces a new PartitionedTableProxy with
        the result tables as the constituents of its underlying partitioned table.

        In the case of the right table being another PartitionedTableProxy, the :meth:`~Table.aj` table operation
        is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            table (Union[Table, PartitionedTableProxy]): the right table or PartitionedTableProxy of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            match_rule (AsOfMatchRule): the inexact matching rule on the last column to match specified in 'on',
                default is AsOfMatchRule.LESS_THAN_EQUAL. The other valid value is AsOfMatchRule.LESS_THAN.
        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            if on:
                on = [_JJoinMatch.parse(p) for p in on]
            if joins:
                joins = [_JJoinAddition.parse(p) for p in joins]

            on = j_array_list(on)
            joins = j_array_list(joins)
            table_op = jpy.cast(table.j_object, _JTableOperations)
            if match_rule is AsOfMatchRule.LESS_THAN_EQUAL:
                match_rule = _JAsOfJoinRule.LESS_THAN_EQUAL
            elif match_rule is AsOfMatchRule.LESS_THAN:
                match_rule = _JAsOfJoinRule.LESS_THAN
            else:
                raise ValueError("invalid match_rule value")

            with auto_locking_ctx(self, table):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.aj(table_op, on, joins, match_rule))
        except Exception as e:
            raise DHError(e, "as-of join operation on the PartitionedTableProxy failed.") from e

    def raj(self, table: Union[Table, PartitionedTableProxy], on: Union[str, Sequence[str]],
            joins: Union[str, Sequence[str]] = None,
            match_rule: AsOfMatchRule = AsOfMatchRule.GREATER_THAN_EQUAL) -> PartitionedTableProxy:
        """Applies the :meth:`~Table.raj` table operation to all constituent tables of the underlying partitioned
        table with the provided right table or PartitionedTableProxy, and produces a new PartitionedTableProxy with
        the result tables as the constituents of its underlying partitioned table.

        In the case of the right table being another PartitionedTableProxy, the :meth:`~Table.raj` table operation
        is applied to the matching pairs of the constituent tables from both underlying partitioned tables.

        Args:
            table (Union[Table, PartitionedTableProxy]): the right table or PartitionedTableProxy of the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            match_rule (AsOfMatchRule): the inexact matching rule on the last column to match specified in 'on',
                default is AsOfMatchRule.GREATER_THAN_EQUAL. The other valid value is AsOfMatchRule.GREATER_THAN.
        Returns:
            a new PartitionedTableProxy

        Raises:
            DHError
        """
        try:
            on = to_sequence(on)
            joins = to_sequence(joins)
            if on:
                on = [_JJoinMatch.parse(p) for p in on]
            if joins:
                joins = [_JJoinAddition.parse(p) for p in joins]

            on = j_array_list(on)
            joins = j_array_list(joins)
            table_op = jpy.cast(table.j_object, _JTableOperations)
            if match_rule is AsOfMatchRule.GREATER_THAN_EQUAL:
                match_rule = _JReverseAsOfJoinRule.GREATER_THAN_EQUAL
            elif match_rule is AsOfMatchRule.GREATER_THAN:
                match_rule = _JReverseAsOfJoinRule.GREATER_THAN
            else:
                raise ValueError("invalid match_rule value")

            with auto_locking_ctx(self, table):
                return PartitionedTableProxy(j_pt_proxy=self.j_pt_proxy.raj(table_op, on, joins, match_rule))
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
