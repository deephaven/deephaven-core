#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Any, Union

from pydeephaven import agg
from pydeephaven._table_ops import UpdateOp, LazyUpdateOp, ViewOp, UpdateViewOp, SelectOp, DropColumnsOp, \
    SelectDistinctOp, SortOp, UnstructuredFilterOp, HeadOp, TailOp, HeadByOp, TailByOp, UngroupOp, NaturalJoinOp, \
    ExactJoinOp, CrossJoinOp, AsOfJoinOp, UpdateByOp, SnapshotTableOp, SnapshotWhenTableOp, WhereInTableOp, \
    AggregateAllOp, AggregateOp
from pydeephaven._utils import to_list
from pydeephaven.agg import Aggregation, _AggregationColumns
from pydeephaven.constants import MatchRule, SortDirection
from pydeephaven.dherror import DHError
from pydeephaven.updateby import UpdateByOperation


class TableInterface(ABC):
    """ TableInterface defines and implements a set of Table operations that eventually are performed on tables in
    the Deephaven server. It is inherited by the Table and Query classes for single table operations on a Table or a
    batch of operations that are defined by a Query.
    """

    @abstractmethod
    def table_op_handler(self, table_op):
        ...

    def drop_columns(self, cols: Union[str, List[str]]):
        """ Drop the specified columns from the table and return the result table.

        Args:
            cols (Union[str, List[str]]) : the column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DropColumnsOp(column_names=to_list(cols))
        return self.table_op_handler(table_op)

    def update(self, formulas: Union[str, List[str]]):
        """ Perform an update operation on the table and return the result table.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UpdateOp(column_specs=to_list(formulas))
        return self.table_op_handler(table_op)

    def lazy_update(self, formulas: Union[str, List[str]]):
        """ Perform a lazy-update operation on the table and return the result table.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = LazyUpdateOp(column_specs=to_list(formulas))
        return self.table_op_handler(table_op)

    def view(self, formulas: Union[str, List[str]]):
        """ Perform a view operation on the table and return the result table.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = ViewOp(column_specs=to_list(formulas))
        return self.table_op_handler(table_op)

    def update_view(self, formulas: Union[str, List[str]]):
        """ Perform an update-view operation on the table and return the result table.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UpdateViewOp(column_specs=to_list(formulas))
        return self.table_op_handler(table_op)

    def select(self, formulas: Union[str, List[str]] = None):
        """ Perform a select operation on the table and return the result table.

        Args:
            formulas (Union[str, List[str]], optional): the column formula(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = SelectOp(column_specs=to_list(formulas))
        return self.table_op_handler(table_op)

    def select_distinct(self, cols: Union[str, List[str]] = None):
        """ Perform a select-distinct operation on the table and return the result table.

        Args:
            cols (Union[str, List[str]], optional): the column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = SelectDistinctOp(column_names=to_list(cols))
        return self.table_op_handler(table_op)

    def sort(self, order_by: Union[str, List[str]], order: Union[SortDirection, List[SortDirection]] = None):
        """ Perform a sort operation on the table and return the result table.

        Args:
            order_by (Union[str, List[str]]): the column(s) to be sorted on
            order (Union[SortDirection, List[SortDirection]], optional): the corresponding sort direction(s) for each
                sort column, default is None. In the absence of explicit sort directions, data will be sorted in the
                ascending order.

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = SortOp(column_names=to_list(order_by), directions=to_list(order))
        return self.table_op_handler(table_op)

    def where(self, filters: Union[str, List[str]]):
        """ Perform a filter operation on the table and return the result table.

        Args:
            filters (Union[str, List[str]]): the filter condition expression(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UnstructuredFilterOp(filters=to_list(filters))
        return self.table_op_handler(table_op)

    def head(self, num_rows: int):
        """ Perform a head operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the head of table

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = HeadOp(num_rows=num_rows)
        return self.table_op_handler(table_op)

    def tail(self, num_rows: int):
        """ Perform a tail operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the end of table

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = TailOp(num_rows=num_rows)
        return self.table_op_handler(table_op)

    def natural_join(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None):
        """ Perform a natural-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = NaturalJoinOp(table=table, keys=to_list(on), columns_to_add=to_list(joins))
        return self.table_op_handler(table_op)

    def exact_join(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None):
        """ Perform a exact-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names              
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = ExactJoinOp(table=table, keys=to_list(on), columns_to_add=to_list(joins))
        return self.table_op_handler(table_op)

    def join(self, table: Any, on: Union[str, List[str]] = None, joins: Union[str, List[str]] = None,
             reserve_bits: int = 10):
        """ Perform a cross-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names, default is None
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            reserve_bits(int, optional): the number of bits of key-space to initially reserve per group; default is 10

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = CrossJoinOp(table=table, keys=to_list(on), columns_to_add=to_list(joins), reserve_bits=reserve_bits)
        return self.table_op_handler(table_op)

    def aj(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None,
           match_rule: MatchRule = MatchRule.LESS_THAN_EQUAL):
        """ Perform a as-of join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            match_rule (MatchRule, optional): the match rule for the as-of join, default is LESS_THAN_EQUAL

        Returns:
            a Table object

        Raises:
            DHError
        """
        match_rule = MatchRule.LESS_THAN if match_rule == MatchRule.LESS_THAN else MatchRule.LESS_THAN_EQUAL
        table_op = AsOfJoinOp(table=table, keys=to_list(on), columns_to_add=to_list(joins), match_rule=match_rule)
        return self.table_op_handler(table_op)

    def raj(self, table: Any, on: Union[str, List[str]], joins: Union[str, List[str]] = None,
            match_rule: MatchRule = MatchRule.GREATER_THAN_EQUAL):
        """ Perform a reverse as-of join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None
            match_rule (MatchRule, optional): the match rule for the as-of join, default is GREATER_THAN_EQUAL

        Returns:
            a Table object

        Raises:
            DHError
        """
        match_rule = MatchRule.GREATER_THAN if match_rule == MatchRule.GREATER_THAN else MatchRule.GREATER_THAN_EQUAL
        table_op = AsOfJoinOp(table=table, keys=to_list(on), columns_to_add=to_list(joins), match_rule=match_rule)
        return self.table_op_handler(table_op)

    def head_by(self, num_rows: int, by: Union[str, List[str]]):
        """ Perform a head-by aggregation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        by = to_list(by)
        table_op = HeadByOp(num_rows=num_rows, column_names=to_list(by))
        return self.table_op_handler(table_op)

    def tail_by(self, num_rows: int, by: Union[str, List[str]]):
        """ Perform a tail-by aggregation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the end of each group
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = TailByOp(num_rows=num_rows, column_names=to_list(by))
        return self.table_op_handler(table_op)

    def group_by(self, by: Union[str, List[str]] = None):
        """ Perform a group-by aggregation on the table and return the result table. After the operation,
        the columns not in the group-by columns become array-type.

        If no group-by column is given, the content of each column is grouped into its own array.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.group(), by=to_list(by))
        return self.table_op_handler(table_op)

    def ungroup(self, cols: Union[str, List[str]] = None, null_fill: bool = True):
        """ Perform an ungroup operation on the table and return the result table. The ungroup columns should be of
        array types.

        Args:
            cols (Union[str, List[str]], optional): the array column(s), default is None, meaning all array columns will
                be ungrouped
            null_fill (bool, optional): indicates whether null should be used to fill missing cells, default is True

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UngroupOp(column_names=to_list(cols), null_fill=null_fill)
        return self.table_op_handler(table_op)

    def first_by(self, by: Union[str, List[str]] = None):
        """ Perform First-by aggregation on the table and return the result table which contains the first row of each
        distinct group.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.first(), by=to_list(by))
        return self.table_op_handler(table_op)

    def last_by(self, by: Union[str, List[str]] = None):
        """ Perform last-by aggregation on the table and return the result table which contains the last row of each
        distinct group.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.last(), by=to_list(by))
        return self.table_op_handler(table_op)

    def sum_by(self, by: Union[str, List[str]] = None):
        """ Perform sum-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (Union[str, List[str]]): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.sum_(), by=to_list(by))
        return self.table_op_handler(table_op)

    def avg_by(self, by: Union[str, List[str]] = None):
        """ Perform avg-by aggregation on the table and return the result table. Columns not used in the grouping must
        be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.avg(), by=to_list(by))
        return self.table_op_handler(table_op)

    def std_by(self, by: Union[str, List[str]] = None):
        """ Perform std-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (Union[str, List[str]]): the group-by column names(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.std(), by=to_list(by))
        return self.table_op_handler(table_op)

    def var_by(self, by: Union[str, List[str]] = None):
        """ Perform var-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.var(), by=to_list(by))
        return self.table_op_handler(table_op)

    def median_by(self, by: Union[str, List[str]] = None):
        """ Perform median-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.median(), by=to_list(by))
        return self.table_op_handler(table_op)

    def min_by(self, by: Union[str, List[str]] = None):
        """ Perform min-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.min_(), by=to_list(by))
        return self.table_op_handler(table_op)

    def max_by(self, by: Union[str, List[str]] = None):
        """ Perform max-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateAllOp(agg=agg.max_(), by=to_list(by))
        return self.table_op_handler(table_op)

    def count_by(self, col: str, by: Union[str, List[str]] = None):
        """ Perform count-by aggregation on the table and return the result table. The count of each group is stored in
        a new column named after the 'col' parameter.

        Args:
            col (str): the name of the column to store the counts
            by (Union[str, List[str]], optional): the group-by column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = AggregateOp(aggs=[agg.count_(col=col)], by=to_list(by))
        return self.table_op_handler(table_op)

    def agg_by(self, aggs: Union[Aggregation, List[Aggregation]], by: Union[str, List[str]]):
        """ Create a new table containing grouping columns and grouped data. The resulting grouped data is defined by
        the aggregations specified.

        Args:
            aggs (Union[Aggregation, List[Aggregation]]): the aggregation(s) to be applied
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        aggs = to_list(aggs)
        for agg in aggs:
            if hasattr(agg, 'cols') and not agg.cols:
                raise DHError(message="No columns specified for the aggregation operation {agg}.")

        table_op = AggregateOp(aggs=aggs, by=to_list(by))
        return self.table_op_handler(table_op)

    def agg_all_by(self, agg: Aggregation, by: Union[str, List[str]]):
        """ Create a new table containing grouping columns and grouped data. The resulting grouped data is defined by
        the aggregation specified.

        Note, because agg_all_by applies the aggregation to all the columns of the table, it will ignore
        any column names specified for the aggregation.

        Args:
            agg (Aggregation): the aggregation to be applied
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        if not isinstance(agg, _AggregationColumns):
            raise DHError(f"unsupported aggregation {agg}.")

        table_op = AggregateAllOp(agg=agg, by=to_list(by))
        return self.table_op_handler(table_op)

    def update_by(self, ops: Union[UpdateByOperation, List[UpdateByOperation]], by: Union[str, List[str]]):
        """ Perform an update-by operation on the table and return the result table.

        Args:
            ops (Union[UpdateByOperatoin, List[UpdateByOperation]]): the UpdateByOperation(s) to be applied
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UpdateByOp(operations=to_list(ops), by=to_list(by))
        return self.table_op_handler(table_op)

    def snapshot(self):
        """Returns a static snapshot table.

        Returns:
            a new Table

        Raises:
            DHError
        """
        table_op = SnapshotTableOp()
        return self.table_op_handler(table_op)

    def snapshot_when(self, trigger_table: Any, stamp_cols: Union[str, List[str]] = None, initial: bool = False,
                      incremental: bool = False, history: bool = False):
        """Returns a table that captures a snapshot of this table whenever trigger_table updates.

        When trigger_table updates, a snapshot of this table and the "stamp key" from trigger_table form the resulting
        table. The "stamp key" is the last row of the trigger_table, limited by the stamp_cols. If trigger_table is
        empty, the "stamp key" will be represented by NULL values.

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
            a new Table

        Raises:
            DHError
        """
        table_op = SnapshotWhenTableOp(trigger_table=trigger_table, stamp_cols=to_list(stamp_cols), initial=initial,
                                       incremental=incremental, history=history)
        return self.table_op_handler(table_op)

    def where_in(self, filter_table: Any, cols: Union[str, List[str]]):
        """The where_in method creates a new table containing rows from the source table, where the rows match
        values in the filter table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, List[str]]): the column name(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        table_op = WhereInTableOp(filter_table=filter_table, cols=to_list(cols), inverted=False)
        return self.table_op_handler(table_op)

    def where_not_in(self, filter_table: Any, cols: Union[str, List[str]]):
        """The where_not_in method creates a new table containing rows from the source table, where the rows do not
        match values in the filter table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, List[str]]): the column name(s)

        Returns:
            a new table

        Raises:
            DHError
        """
        table_op = WhereInTableOp(filter_table=filter_table, cols=to_list(cols), inverted=True)
        return self.table_op_handler(table_op)
