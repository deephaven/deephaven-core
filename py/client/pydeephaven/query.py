#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module implements the Query class that can be used to execute a chained set of Table operations in one
batch."""
from __future__ import annotations

from typing import Union

from pydeephaven.table import Table
from pydeephaven._table_interface import TableInterface
from pydeephaven._table_ops import *
from pydeephaven.dherror import DHError


class Query(TableInterface):
    """A Query object is used to define and execute a sequence of Deephaven table operations on the server.

    When the query is executed, the table operations specified for the Query object are batched together and sent to the
    server in a single request, thus avoiding multiple round trips between the client and the server. The result of
    executing the query is a new Deephaven table.

    Note, an application should always use the factory method on the Session object to create a Query instance as the
    constructor is subject to future changes to support more advanced features already planned.
    """
    _ops: List[TableOp]

    def table_op_handler(self, table_op: TableOp) -> Query:
        self._ops.append(table_op)
        return self

    def __init__(self, session: Any, table: Table):
        self.session = session
        if not self.session or not table:
            raise DHError("invalid session or table value.")
        self._ops = [NoneOp(table=table)]

    def exec(self) -> Table:
        """Executes the query on the server and return the result table.

        Returns:
            a Table object

        Raises:
            DHError
        """
        return self.session.table_service.batch(self._ops)

    def drop_columns(self, cols: Union[str, List[str]]) -> Query:
        """Adds a drop-columns operation to the query.

        Args:
            cols (Union[str, List[str]]) : the column name(s)

        Returns:
            self
        """
        return super().drop_columns(cols)

    def update(self, formulas: Union[str, List[str]]) -> Query:
        """Adds an update operation to the query.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            self
        """
        return super().update(formulas)

    def lazy_update(self, formulas: Union[str, List[str]]) -> Query:
        """Adds a lazy-update operation to the query.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            self
        """
        return super().lazy_update(formulas)

    def view(self, formulas: Union[str, List[str]]) -> Query:
        """Adds a view operation to the query.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            self
        """
        return super().view(formulas)

    def update_view(self, formulas: Union[str, List[str]]) -> Query:
        """Adds an update-view operation to the query.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            self
        """
        return super().update_view(formulas)

    def select(self, formulas: Union[str, List[str]] = None) -> Query:
        """Adds a select operation to the query.

        Args:
            formulas (Union[str, List[str]], optional): the column formula(s), default is None

        Returns:
            self
        """
        return super().select(formulas)

    def select_distinct(self, cols: Union[str, List[str]] = None) -> Query:
        """Adds a select-distinct operation to the query.

        Args:
            cols (Union[str, List[str]], optional): the column name(s), default is None

        Returns:
            self
        """
        return super().select_distinct(cols)

    def sort(self, order_by: Union[str, List[str]], order: Union[SortDirection, List[SortDirection]] = None) -> Query:
        """Adds sort operation to the query.

        Args:
            order_by (Union[str, List[str]]): the names of the columns to be sorted on
            order (Union[SortDirection, List[SortDirection]], optional): the corresponding sort direction(s) for each
                sort column, default is None. In the absence of explicit sort directions, data will be sorted in 
                the ascending order.

        Returns:
            self
        """
        return super().sort(order_by, order)

    def where(self, filters: Union[str, List[str]]) -> Query:
        """Adds a filter operation to the query.

        Args:
            filters (Union[str, List[str]]): the filter condition expression(s)

        Returns:
            self
        """
        return super().where(filters)

    def head(self, num_rows: int) -> Query:
        """Adds a head operation to the query.

        Args:
            num_rows (int): the number of rows at the head of table

        Returns:
            self
        """
        return super().head(num_rows)

    def tail(self, num_rows: int) -> Query:
        """Adds a tail operation to the query.

        Args:
            num_rows (int): the number of rows at the end of table

        Returns:
            self
        """
        return super().tail(num_rows)

    def natural_join(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Query:
        """Adds a natural-join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            self
        """
        return super().natural_join(table, on, joins)

    def exact_join(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Query:
        """Adds an exact-join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            self
        """
        return super().exact_join(table, on, joins)

    def join(self, table: Any, on: Union[str, List[str]] = None, joins: Union[str, List[str]] = None,
             reserve_bits: int = 10) -> Query:
        """Adds a cross-join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            reserve_bits(int, optional): the number of bits of key-space to initially reserve per group; default is 10

        Returns:
            self
        """
        return super().join(table, on, joins)

    def aj(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Query:
        """Adds a as-of join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '>' or '>='.  If a common name is used for the inexact match,
                '>=' is used for the comparison.
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            self
        """
        return super().aj(table, on, joins)

    def raj(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Query:
        """Adds a reverse as-of join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '<' or '<='.  If a common name is used for the inexact match,
                '<=' is used for the comparison.
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            self
        """
        return super().raj(table, on, joins)

    def head_by(self, num_rows: int, by: Union[str, List[str]]) -> Query:
        """Adds a head-by operation to the query.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            self
        """
        return super().head_by(num_rows, by)

    def tail_by(self, num_rows: int, by: Union[str, List[str]]) -> Query:
        """Adds a tail-by operation to the query.

        Args:
            num_rows (int): the number of rows at the end of each group
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            self
        """
        return super().tail_by(num_rows, by)

    def group_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a group-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s); default is None

        Returns:
            self
        """
        return super().group_by(by)

    def ungroup(self, cols: Union[str, List[str]] = None, null_fill: bool = True) -> Query:
        """Adds an ungroup operation to the query.

        Args:
            cols (Union[str, List[str]], optional): the array column(s), default is None, meaning all array columns will
                be ungrouped
            null_fill (bool, optional): indicates whether null should be used to fill missing cells, default is True

        Returns:
            self
        """
        return super().ungroup(cols, null_fill)

    def first_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a first-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().first_by(by)

    def last_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a last-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().last_by(by)

    def sum_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a sum-by aggregation to the query.

        Args:
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            self
        """
        return super().sum_by(by)

    def avg_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds an avg-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().avg_by(by)

    def std_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a std-by aggregation to the query.

        Args:
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            self
        """
        return super().std_by(by)

    def var_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a var-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().var_by(by)

    def median_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a median-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().median_by(by)

    def min_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a min-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().min_by(by)

    def max_by(self, by: Union[str, List[str]] = None) -> Query:
        """Adds a max-by aggregation to the query.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().max_by(by)

    def count_by(self, col: str, by: Union[str, List[str]] = None) -> Query:
        """Adds a count-by aggregation to the query.

        Args:
            col (str): the name of the column to store the counts
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            self
        """
        return super().count_by(col, by)

    def agg_by(self, aggs: Union[Aggregation, List[Aggregation]], by: Union[str, List[str]]) -> Query:
        """Adds an Aggregate operation to the query.

        Args:
            by (Union[str, List[str]]): the group-by column name(s)
            aggs (Union[Aggregation, List[Aggregation]]): the aggregation(s) to be applied

        Returns:
            self
        """
        return super().agg_by(aggs=aggs, by=by)

    def agg_all_by(self, agg: Aggregation, by: Union[str, List[str]]) -> Query:
        """Adds an AggregateAll operation to the query.

        Args:
            agg (Aggregation): the aggregation to be applied
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            self
        """
        return super().agg_all_by(agg=agg, by=by)

    def update_by(self, ops: Union[UpdateByOperation, List[UpdateByOperation]], by: Union[str, List[str]]) -> Query:
        """Adds an update-by operation to the query.

        Args:
            ops (Union[UpdateByOperation, List[UpdateByOperation]]): the UpdateByOperation(s) to be applied
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            self
        """
        return super().update_by(ops, by)

    def snapshot(self) -> Query:
        """Adds a snapshot operation to the query.

        Returns:
            self
        """
        return super().snapshot()

    def snapshot_when(self, trigger_table: Any, stamp_cols: Union[str, List[str]] = None, initial: bool = False,
                      incremental: bool = False, history: bool = False) -> Query:
        """Adds a snapshot_when operation to the query.

        Args:
            trigger_table (Table): the trigger table
            stamp_cols (Union[str, List[str]]): The column(s) from trigger_table that form the "stamp key", may be
                renames, default is None, meaning that all columns from trigger_table form the "stamp key".
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
            self
        """
        return super().snapshot_when(trigger_table, stamp_cols, initial, incremental, history)

    def where_in(self, filter_table: Any, cols: Union[str, List[str]]) -> Query:
        """Adds a where_in operation to the query.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, List[str]]): the column name(s)

        Returns:
            self
        """
        return super().where_in(filter_table, cols)

    def where_not_in(self, filter_table: Any, cols: Union[str, List[str]]) -> Query:
        """Adds a where_not_in operation to the query.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, List[str]]): the column name(s)

        Returns:
            self
        """
        return super().where_not_in(filter_table, cols)
