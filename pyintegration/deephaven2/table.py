#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
""" This module implements the Table class and functions that work with Tables. """
from __future__ import annotations

from typing import List

import jpy

from deephaven2 import DHError, dtypes
from deephaven2._jcompat import j_array_list
from deephaven2._wrapper_abc import JObjectWrapper
from deephaven2.agg import Aggregation
from deephaven2.column import Column, ColumnType
from deephaven2.constants import SortDirection

_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")
_JColumnName = jpy.get_type("io.deephaven.api.ColumnName")
_JSortColumn = jpy.get_type("io.deephaven.api.SortColumn")
_JFilter = jpy.get_type("io.deephaven.api.filter.Filter")
_JFilterOr = jpy.get_type("io.deephaven.api.filter.FilterOr")


class Table(JObjectWrapper):
    """A Table represents a Deephaven table. It allows applications to perform powerful Deephaven table operations.

    Note: It should not be instantiated directly by user code. Tables are mostly created by factory methods,
    data ingestion operations, queries, aggregations, joins, etc.

    """

    def __init__(self, j_table):
        self.j_table = j_table
        self._definition = self.j_table.getDefinition()
        self._schema = None

    def __repr__(self):
        default_repr = super().__repr__()
        column_dict = {col.name: col.data_type for col in self.columns[:10]}
        repr_str = (
            f"{default_repr[:-2]}, num_rows = {self.size}, columns = {column_dict}"
        )
        repr_str = repr_str[:115] + "...}>" if len(repr_str) > 120 else repr_str
        return repr_str

    def __eq__(self, table: Table) -> bool:
        try:
            if _JTableTools.diff(self.j_table, table.j_table, 1):
                return False
            else:
                return True
        except Exception as e:
            raise DHError(e, "table equality test failed.") from e

    # to make the table visible to DH script session, internal use only
    def get_dh_table(self):
        return self.j_table

    @property
    def size(self) -> int:
        """The current number of rows in the table."""
        return self.j_table.size()

    @property
    def is_refreshing(self) -> bool:
        """Whether this table is refreshing."""
        return self.j_table.isRefreshing()

    @property
    def columns(self):
        """The column definitions of the table."""
        if self._schema:
            return self._schema

        self._schema = []
        j_col_list = self._definition.getColumnList()
        for i in range(j_col_list.size()):
            j_col = j_col_list.get(i)
            self._schema.append(
                Column(
                    name=j_col.getName(),
                    data_type=dtypes.from_jtype(j_col.getDataType()),
                    component_type=dtypes.from_jtype(j_col.getComponentType()),
                    column_type=ColumnType(j_col.getColumnType()),
                )
            )
        return self._schema

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table

    def to_string(self, num_rows: int = 10, cols: List[str] = []) -> str:
        """Returns the first few rows of a table as a pipe-delimited string.

        Args:
            num_rows (int): the number of rows at the beginning of the table
            cols (List[str]): the list of column names

        Returns:
            string

        Raises:
            DHError
        """
        try:
            return _JTableTools.string(self.j_table, num_rows, *cols)
        except Exception as e:
            raise DHError(e, "table to_string failed") from e

    def coalesce(self) -> Table:
        """Returns a coalesced child table."""
        return Table(j_table=self.j_table.coalesce())

    def snapshot(self, source_table: Table, do_init: bool = False) -> Table:
        """Produces an in-memory copy of a source table that refreshes when this table changes.

        Note, this table is often a time table that adds new rows at a regular, user-defined interval.

        Args:
            do_init (bool): whether to snapshot when this method is initially called, default is False
            source_table (Table): the table to be snapshot

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.snapshot(source_table.j_table, do_init))
        except Exception as e:
            raise DHError(message="failed to take a table snapshot") from e

    #
    # Table operation category: Select
    #
    # region Select
    def drop_columns(self, cols: List[str]) -> Table:
        """The drop_columns method creates a new table with the same size as this table but omits any of specified
        columns.

        Args:
            cols (List[str]): the list of column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.dropColumns(*cols))
        except Exception as e:
            raise DHError(e, "table drop_columns operation failed.") from e

    def move_columns(self, idx: int, cols: List[str]) -> Table:
        """The move_columns method creates a new table with specified columns moved to a specific column index value.

        Args:
            idx (int): the column index where the specified columns will be moved in the new table.
            cols (List[str]) : the list of column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.moveColumns(idx, *cols))
        except Exception as e:
            raise DHError(e, "table move_columns operation failed.") from e

    def move_columns_down(self, cols: List[str]) -> Table:
        """The move_columns_down method creates a new table with specified columns appearing last in order, to the far
        right.

        Args:
            cols (List[str]) : the list of column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.moveColumnsDown(*cols))
        except Exception as e:
            raise DHError(e, "table move_columns_down operation failed.") from e

    def move_columns_up(self, cols: List[str]) -> Table:
        """The move_columns_up method creates a new table with specified columns appearing first in order, to the far
        left.

        Args:
            cols (List[str]) : the list of column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.moveColumnsUp(*cols))
        except Exception as e:
            raise DHError(e, "table move_columns_up operation failed.") from e

    def rename_columns(self, cols: List[str]) -> Table:
        """The rename_columns method creates a new table with the specified columns renamed.

        Args:
            cols (List[str]) : the list of column rename expr as "X = Y"

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.renameColumns(*cols))
        except Exception as e:
            raise DHError(e, "table rename_columns operation failed.") from e

    def update(self, formulas: List[str]) -> Table:
        """The update method creates a new table containing a new, in-memory column for each formula.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            A new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.update(*formulas))
        except Exception as e:
            raise DHError(e, "table update operation failed.") from e

    def lazy_update(self, formulas: List[str]) -> Table:
        """The lazy_update method creates a new table containing a new, cached, formula column for each formula.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.lazyUpdate(*formulas))
        except Exception as e:
            raise DHError(e, "table lazy_update operation failed.") from e

    def view(self, formulas: List[str]) -> Table:
        """The view method creates a new formula table that includes one column for each formula.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.view(*formulas))
        except Exception as e:
            raise DHError(e, "table view operation failed.") from e

    def update_view(self, formulas: List[str]) -> Table:
        """The update_view method creates a new table containing a new, formula column for each formula.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.updateView(*formulas))
        except Exception as e:
            raise DHError(e, "table update_view operation failed.") from e

    def select(self, formulas: List[str] = []) -> Table:
        """The select method creates a new in-memory table that includes one column for each formula. If no formula
        is specified, all columns will be included.

        Args:
            formulas (List[str], optional): the column formulas, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if not formulas:
                return Table(j_table=self.j_table.select())
            return Table(j_table=self.j_table.select(*formulas))
        except Exception as e:
            raise DHError(e, "table select operation failed.") from e

    def select_distinct(self, cols: List[str] = []) -> Table:
        """The select_distinct method creates a new table containing all of the unique values for a set of key
        columns. When the selectDistinct method is used on multiple columns, it looks for distinct sets of values in
        the selected columns.

        Args:
            cols (List[str], optional): the list of column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.selectDistinct(*cols))
        except Exception as e:
            raise DHError(e, "table select_distinct operation failed.") from e

    # endregion

    #
    # Table operation category: Filter
    #
    # region Filter
    def where(self, filters: List[str] = []) -> Table:
        """The where method creates a new table with only the rows meeting the filter criteria in the column(s) of
        the table.

        Args:
            filters (List[str]): a list of filter condition expressions

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.where(*filters))
        except Exception as e:
            raise DHError(e, "table where operation failed.") from e

    def where_in(self, filter_table: Table, cols: List[str]) -> Table:
        """The where_in method creates a new table containing rows from the source table, where the rows match
        values in the filter table. The filter is updated whenever either table changes.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (List[str]): a list of column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.whereIn(filter_table.j_table, *cols))
        except Exception as e:
            raise DHError(e, "table where_in operation failed.") from e

    def where_not_in(self, filter_table: Table, cols: List[str]) -> Table:
        """The where_not_in method creates a new table containing rows from the source table, where the rows do not
        match values in the filter table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (List[str]): a list of column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.whereNotIn(filter_table.j_table, *cols))
        except Exception as e:
            raise DHError(e, "table where_not_in operation failed.") from e

    def where_one_of(self, filters: List[str] = []) -> Table:
        """The where_one_of method creates a new table containing rows from the source table, where the rows match at least
        one filter.

        Args:
            filters (List[str]): a list of filter condition expressions

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
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
    def restrict_sort_to(self, cols: List[str]):
        """The restrict_sort_to method only allows sorting on specified table columns. This can be useful to prevent
        users from accidentally performing expensive sort operations as they interact with tables in the UI.

        Args:
            cols (List[str], optional): the list of column names

        Raises:
            DHError
        """
        try:
            return self.j_table.restrictSortTo(*cols)
        except Exception as e:
            raise DHError(e, "table restrict_sort_to operation failed.") from e

    def sort_descending(self, order_by: List[str] = []) -> Table:
        """The sort_descending method creates a new table where rows in a table are sorted in a largest to smallest
        order based on the order_by column(s).

        Args:
            order_by (List[str], optional): the list of column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
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

    def sort(self, order_by: List[str], order: List[SortDirection] = []) -> Table:
        """The sort method creates a new table where (1) rows are sorted in a smallest to largest order based on the
        order_by column(s) (2) where rows are sorted in the order defined by the order argument.

        Args:
            order_by (List[str]): the names of the columns to be sorted on
            order (List[SortDirection], optional): the corresponding sort directions for each sort column, default
                is empty. In the absence of explicit sort directions, data will be sorted in the ascending order.

        Returns:
            a new table

        Raises:
            DHError
        """

        def sort_column(col, dir_):
            return (
                _JSortColumn.desc(_JColumnName.of(col))
                if dir_ == SortDirection.DESCENDING
                else _JSortColumn.asc(_JColumnName.of(col))
            )

        try:
            if order:
                sort_columns = [
                    sort_column(col, dir_) for col, dir_ in zip(order_by, order)
                ]
                j_sc_list = j_array_list(sort_columns)
                return Table(j_table=self.j_table.sort(j_sc_list))
            else:
                return Table(j_table=self.j_table.sort(*order_by))
        except Exception as e:
            raise DHError(e, "table sort operation failed.") from e

    # endregion

    #
    # Table operation category: Join
    #
    # region Join
    def natural_join(self, table: Table, on: List[str], joins: List[str] = []) -> Table:
        """The natural_join method creates a new table containing all of the rows and columns of this table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the key values in the left and right tables are
        equal. If there is no matching key in the right table, appended row values are NULL.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
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

    def exact_join(self, table: Table, on: List[str], joins: List[str] = []) -> Table:
        """The exact_join method creates a new table containing all of the rows and columns of this table plus
        additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the key values in the left and right tables are
        equal.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
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

    def join(self, table: Table, on: List[str], joins: List[str] = []) -> Table:
        """The join method creates a new table containing rows that have matching values in both tables. Rows that
        do not have matching criteria will not be included in the result. If there are multiple matches between a row
        from the left table and rows from the right table, all matching combinations will be included. If no columns
        to match (on) are specified, every combination of left and right table rows is included.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
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

    def aj(self, table: Table, on: List[str], joins: List[str] = []) -> Table:
        """The aj (as-of join) method creates a new table containing all of the rows and columns of the left table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the keys from the left table most closely match
        the keys from the right table without going over. If there is no matching key in the right table, appended row
        values are NULL.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if joins:
                return Table(
                    j_table=self.j_table.aj(
                        table.j_table, ",".join(on), ",".join(joins)
                    )
                )
            else:
                return Table(j_table=self.j_table.aj(table.j_table, ",".join(on)))
        except Exception as e:
            raise DHError(e, "table as-of join operation failed.") from e

    def raj(self, table: Table, on: List[str], joins: List[str] = []) -> Table:
        """The reverse-as-of join method creates a new table containing all of the rows and columns of the left table,
        plus additional columns containing data from the right table. For columns appended to the left table (joins),
        row values equal the row values from the right table where the keys from the left table most closely match
        the keys from the right table without going under. If there is no matching key in the right table, appended row
        values are NULL.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if joins:
                return Table(
                    j_table=self.j_table.raj(
                        table.j_table, ",".join(on), ",".join(joins)
                    )
                )
            else:
                return Table(j_table=self.j_table.raj(table.j_table, ",".join(on)))
        except Exception as e:
            raise DHError(e, "table reverse-as-of join operation failed.") from e

    # endregion

    #
    # Table operation category: Aggregation
    # region Aggregation
    def head_by(self, num_rows: int, by: List[str]) -> Table:
        """The head_by method creates a new table containing the first number of rows for each group.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            by (List[str]): the group-by column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.headBy(num_rows, *by))
        except Exception as e:
            raise DHError(e, "table head_by operation failed.") from e

    def tail_by(self, num_rows: int, by: List[str]) -> Table:
        """The tail_by method creates a new table containing the last number of rows for each group.

        Args:
            num_rows (int): the number of rows at the end of each group
            by (List[str]): the group-by column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.tailBy(num_rows, *by))
        except Exception as e:
            raise DHError(e, "table tail_by operation failed.") from e

    def group_by(self, by: List[str] = []) -> Table:
        """The group_by method creates a new table containing grouping columns and grouped data, column content is
        grouped into arrays.

        Args:
            by (List[str], optional): the group-by column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.groupBy(*by))
            else:
                return Table(j_table=self.j_table.groupBy())
        except Exception as e:
            raise DHError(e, "table group operation failed.") from e

    def ungroup(self, cols: List[str] = []) -> Table:
        """The ungroup method creates a new table in which array columns from the source table are unwrapped into
        separate rows.

        Args:
            cols (List[str], optional): the names of the array columns, if empty, all array columns will be
                ungrouped, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if cols:
                return Table(j_table=self.j_table.ungroup(*cols))
            else:
                return Table(j_table=self.j_table.ungroup())
        except Exception as e:
            raise DHError(e, "table ungroup operation failed.") from e

    def first_by(self, by: List[str] = []) -> Table:
        """The first_by method creates a new table containing the first row for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.firstBy(*by))
            else:
                return Table(j_table=self.j_table.firstBy())
        except Exception as e:
            raise DHError(e, "table first_by operation failed.") from e

    def last_by(self, by: List[str] = []) -> Table:
        """The last_by method creates a new table containing the last row for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.lastBy(*by))
            else:
                return Table(j_table=self.j_table.lastBy())
        except Exception as e:
            raise DHError(e, "table last_by operation failed.") from e

    def sum_by(self, by: List[str] = []) -> Table:
        """The sum_by method creates a new table containing the sum for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.sumBy(*by))
            else:
                return Table(j_table=self.j_table.sumBy())
        except Exception as e:
            raise DHError(e, "table sum_by operation failed.") from e

    def avg_by(self, by: List[str] = []) -> Table:
        """The avg_by method creates a new table containing the average for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.avgBy(*by))
            else:
                return Table(j_table=self.j_table.avgBy())
        except Exception as e:
            raise DHError(e, "table avg_by operation failed.") from e

    def std_by(self, by: List[str] = []) -> Table:
        """The std_by method creates a new table containing the standard deviation for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.stdBy(*by))
            else:
                return Table(j_table=self.j_table.stdBy())
        except Exception as e:
            raise DHError(e, "table std_by operation failed.") from e

    def var_by(self, by: List[str] = []) -> Table:
        """The var_by method creates a new table containing the variance for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.varBy(*by))
            else:
                return Table(j_table=self.j_table.varBy())
        except Exception as e:
            raise DHError(e, "table var_by operation failed.") from e

    def median_by(self, by: List[str] = []) -> Table:
        """The median_by method creates a new table containing the median for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.medianBy(*by))
            else:
                return Table(j_table=self.j_table.medianBy())
        except Exception as e:
            raise DHError(e, "table median_by operation failed.") from e

    def min_by(self, by: List[str] = []) -> Table:
        """The min_by method creates a new table containing the minimum value for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.minBy(*by))
            else:
                return Table(j_table=self.j_table.minBy())
        except Exception as e:
            raise DHError(e, "table min_by operation failed.") from e

    def max_by(self, by: List[str] = []) -> Table:
        """The max_by method creates a new table containing the maximum value for each group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.maxBy(*by))
            else:
                return Table(j_table=self.j_table.maxBy())
        except Exception as e:
            raise DHError(e, "table max_by operation failed.") from e

    def count_by(self, col: str, by: List[str] = []) -> Table:
        """The count_by method creates a new table containing the number of rows for each group.

        Args:
            col (str): the name of the column to store the counts
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            if by:
                return Table(j_table=self.j_table.countBy(col, *by))
            else:
                return Table(j_table=self.j_table.countBy(col))
        except Exception as e:
            raise DHError(e, "table count_by operation failed.") from e

    def agg_by(self, aggs: List[Aggregation], by: List[str]) -> Table:
        """The agg_by method creates a new table containing grouping columns and grouped data. The resulting
        grouped data is defined by the aggregations specified.

        Args:
            aggs (List[Aggregation]): the list of aggregations
            by (List[str]): the group-by column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            j_agg_list = j_array_list([agg.j_aggregation for agg in aggs])
            return Table(j_table=self.j_table.aggBy(j_agg_list, *by))
        except Exception as e:
            raise DHError(e, "table agg_by operation failed.") from e

    def agg_all_by(self, agg: Aggregation, by: List[str]) -> Table:
        """The agg_all_by method creates a new table containing grouping columns and grouped data. The resulting
        grouped data is defined by the aggregation specified.

        Note, because agg_all_by applies the aggregation to all the columns of the table, it will ignore
        any column names specified for the aggregation.

        Args:
            agg (Aggregation): the aggregation
            by (List[str]): the group-by column names

        Returns:
            a new table

        Raises:
            DHError
        """
        try:
            return Table(j_table=self.j_table.aggAllBy(agg.j_agg_spec, *by))
        except Exception as e:
            raise DHError(e, "table agg_all_by operation failed.") from e

    # endregion
