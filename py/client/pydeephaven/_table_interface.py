#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Any

from pydeephaven.dherror import DHError
from pydeephaven.combo_agg import ComboAggregation
from pydeephaven._table_ops import UpdateOp, LazyUpdateOp, ViewOp, UpdateViewOp, SelectOp, DropColumnsOp, \
    SelectDistinctOp, SortOp, UnstructuredFilterOp, HeadOp, TailOp, HeadByOp, TailByOp, UngroupOp, NaturalJoinOp, \
    ExactJoinOp, CrossJoinOp, AsOfJoinOp, DedicatedAggOp, ComboAggOp
from pydeephaven.constants import MatchRule, SortDirection
from pydeephaven._constants import AggType


class TableInterface(ABC):
    """ TableInterface defines and implements a set of Table operations that eventually are performed on tables in
    the Deephaven server. It is inherited by the Table and Query classes for single table operations on a Table or a
    batch of operations that are defined by a Query.
    """

    @abstractmethod
    def table_op_handler(self, table_op):
        ...

    def drop_columns(self, cols: List[str]):
        """ Drop the specified columns from the table and return the result table.

        Args:
            cols (List[str]) : the list of column names

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DropColumnsOp(column_names=cols)
        return self.table_op_handler(table_op)

    def update(self, formulas: List[str]):
        """ Perform an update operation on the table and return the result table.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UpdateOp(column_specs=formulas)
        return self.table_op_handler(table_op)

    def lazy_update(self, formulas: List[str]):
        """ Perform a lazy-update operation on the table and return the result table.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = LazyUpdateOp(column_specs=formulas)
        return self.table_op_handler(table_op)

    def view(self, formulas: List[str]):
        """ Perform a view operation on the table and return the result table.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = ViewOp(column_specs=formulas)
        return self.table_op_handler(table_op)

    def update_view(self, formulas: List[str]):
        """ Perform an update-view operation on the table and return the result table.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UpdateViewOp(column_specs=formulas)
        return self.table_op_handler(table_op)

    def select(self, formulas: List[str] = []):
        """ Perform a select operation on the table and return the result table.

        Args:
            formulas (List[str], optional): the column formulas, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = SelectOp(column_specs=formulas)
        return self.table_op_handler(table_op)

    def select_distinct(self, cols: List[str] = []):
        """ Perform a select-distinct operation on the table and return the result table.

        Args:
            cols (List[str], optional): the list of column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = SelectDistinctOp(column_names=cols)
        return self.table_op_handler(table_op)

    def sort(self, order_by: List[str], order: List[SortDirection] = []):
        """ Perform a sort operation on the table and return the result table.

        Args:
            order_by (List[str]): the names of the columns to be sorted on
            order (List[SortDirection], optional): the corresponding sort directions for each sort column, default
                is empty. In the absence of explicit sort directions, data will be sorted in the ascending order.

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = SortOp(column_names=order_by, directions=order)
        return self.table_op_handler(table_op)

    def where(self, filters: List[str]):
        """ Perform a filter operation on the table and return the result table.

        Args:
            filters (List[str]): a list of filter condition expressions

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UnstructuredFilterOp(filters=filters)
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

    def natural_join(self, table: Any, on: List[str], joins: List[str] = []):
        """ Perform a natural-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = NaturalJoinOp(table=table, keys=on, columns_to_add=joins)
        return self.table_op_handler(table_op)

    def exact_join(self, table: Any, on: List[str], joins: List[str] = []):
        """ Perform a exact-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = ExactJoinOp(table=table, keys=on, columns_to_add=joins)
        return self.table_op_handler(table_op)

    def join(self, table: Any, on: List[str] = [], joins: List[str] = [], reserve_bits: int = 10):
        """ Perform a cross-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            reserve_bits(int, optional): the number of bits of key-space to initially reserve per group; default is 10

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = CrossJoinOp(table=table, keys=on, columns_to_add=joins, reserve_bits=reserve_bits)
        return self.table_op_handler(table_op)

    def aj(self, table: Any, on: List[str], joins: List[str] = [], match_rule: MatchRule = MatchRule.LESS_THAN_EQUAL):
        """ Perform a as-of join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            match_rule (MatchRule, optional): the match rule for the as-of join, default is LESS_THAN_EQUAL

        Returns:
            a Table object

        Raises:
            DHError
        """
        match_rule = MatchRule.LESS_THAN if match_rule == MatchRule.LESS_THAN else MatchRule.LESS_THAN_EQUAL
        table_op = AsOfJoinOp(table=table, keys=on, columns_to_add=joins, match_rule=match_rule)
        return self.table_op_handler(table_op)

    def raj(self, table: Any, on: List[str], joins: List[str] = [], match_rule: MatchRule = MatchRule.GREATER_THAN_EQUAL):
        """ Perform a reverse as-of join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            match_rule (MatchRule, optional): the match rule for the as-of join, default is GREATER_THAN_EQUAL

        Returns:
            a Table object

        Raises:
            DHError
        """
        match_rule = MatchRule.GREATER_THAN if match_rule == MatchRule.GREATER_THAN else MatchRule.GREATER_THAN_EQUAL
        table_op = AsOfJoinOp(table=table, keys=on, columns_to_add=joins,
                              match_rule=match_rule)
        return self.table_op_handler(table_op)

    def head_by(self, num_rows: int, by: List[str]):
        """ Perform a head-by aggregation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            by (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = HeadByOp(num_rows=num_rows, column_names=by)
        return self.table_op_handler(table_op)

    def tail_by(self, num_rows: int, by: List[str]):
        """ Perform a tail-by aggregation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the end of each group
            by (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = TailByOp(num_rows=num_rows, column_names=by)
        return self.table_op_handler(table_op)

    def group_by(self, by: List[str] = []):
        """ Perform a group-by aggregation on the table and return the result table. After the operation,
        the columns not in the group-by columns become array-type.

        If no group-by column is given,the content of each column is grouped into its own array.

        Args:
            by (List[str], optional): the group-by column names; default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.GROUP, column_names=by)
        return self.table_op_handler(table_op)

    def ungroup(self, cols: List[str] = [], null_fill: bool = True):
        """ Perform an ungroup operation on the table and return the result table. The ungroup columns should be of
        array types.

        Args:
            cols (List[str], optional): the names of the array columns, if empty, all array columns will be
                ungrouped, default is empty
            null_fill (bool, optional): indicates whether null should be used to fill missing cells, default is True

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = UngroupOp(column_names=cols, null_fill=null_fill)
        return self.table_op_handler(table_op)

    def first_by(self, by: List[str] = []):
        """ Perform First-by aggregation on the table and return the result table which contains the first row of each
        distinct group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.FIRST, column_names=by)
        return self.table_op_handler(table_op)

    def last_by(self, by: List[str] = []):
        """ Perform last-by aggregation on the table and return the result table which contains the last row of each
        distinct group.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.LAST, column_names=by)
        return self.table_op_handler(table_op)

    def sum_by(self, by: List[str] = []):
        """ Perform sum-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.SUM, column_names=by)
        return self.table_op_handler(table_op)

    def avg_by(self, by: List[str] = []):
        """ Perform avg-by aggregation on the table and return the result table. Columns not used in the grouping must
        be of numeric types.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.AVG, column_names=by)
        return self.table_op_handler(table_op)

    def std_by(self, by: List[str] = []):
        """ Perform std-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.STD, column_names=by)
        return self.table_op_handler(table_op)

    def var_by(self, by: List[str] = []):
        """ Perform var-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.VAR, column_names=by)
        return self.table_op_handler(table_op)

    def median_by(self, by: List[str] = []):
        """ Perform median-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.MEDIAN, column_names=by)
        return self.table_op_handler(table_op)

    def min_by(self, by: List[str] = []):
        """ Perform min-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.MIN, column_names=by)
        return self.table_op_handler(table_op)

    def max_by(self, by: List[str] = []):
        """ Perform max-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.MAX, column_names=by)
        return self.table_op_handler(table_op)

    def count_by(self, col: str, by: List[str] = []):
        """ Perform count-by aggregation on the table and return the result table. The count of each group is stored in
        a new column named after the 'col' parameter.

        Args:
            col (str): the name of the column to store the counts
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.COUNT, column_names=by, count_column=col)
        return self.table_op_handler(table_op)

    def count(self, col: str):
        """ Count the number of values in the specified column on the table and return the result in a table with one row
        and one column.

        Args:
            col (str): the name of the column whose values to be counted

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = DedicatedAggOp(AggType.COUNT, count_column=col)
        return self.table_op_handler(table_op)

    def agg_by(self, agg: ComboAggregation, by: List[str]):
        """ Perform a Combined Aggregation operation on the table and return the result table.

        Args:
            agg (ComboAggregation): the combined aggregation definition
            by (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = ComboAggOp(column_names=by, combo_aggregation=agg)
        return self.table_op_handler(table_op)
