#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Any

from pydeephaven._combo_aggs import ComboAggregation
from pydeephaven._table_ops import UpdateOp, LazyUpdateOp, ViewOp, UpdateViewOp, SelectOp, DropColumnsOp, \
    SelectDistinctOp, SortOp, UnstructuredFilterOp, HeadOp, TailOp, HeadByOp, TailByOp, UngroupOp, NaturalJoinOp, \
    ExactJoinOp, LeftJoinOp, CrossJoinOp, AsOfJoinOp, DedicatedAggOp, ComboAggOp
from pydeephaven.constants import MatchRule, AggType, SortDirection


class TableInterface(ABC):
    """ TableInterface defines and implements a set of Table operations that eventually are performed on tables in
    the Deephaven server. It is inherited by the Table and Query classes for single table operations on a Table or a
    batch of operations that are defined by a Query.
    """

    @abstractmethod
    def table_op_handler(self, table_op):
        ...

    def drop_columns(self, column_names: List[str]):
        """ Drop the specified columns from the table and return the result table.

        Args:
            column_names (List[str]) : the list of column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DropColumnsOp(column_names=column_names)
        return self.table_op_handler(table_op)

    def update(self, column_specs: List[str]):
        """ Perform an update operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = UpdateOp(column_specs=column_specs)
        return self.table_op_handler(table_op)

    def lazy_update(self, column_specs: List[str]):
        """ Perform a lazy-update operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = LazyUpdateOp(column_specs=column_specs)
        return self.table_op_handler(table_op)

    def view(self, column_specs: List[str]):
        """ Perform a view operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = ViewOp(column_specs=column_specs)
        return self.table_op_handler(table_op)

    def update_view(self, column_specs: List[str]):
        """ Perform a update-view operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = UpdateViewOp(column_specs=column_specs)
        return self.table_op_handler(table_op)

    def select(self, column_specs: List[str] = []):
        """ Perform a select operation on the table and return the result table.

        Args:
            column_specs (List[str], optional): the column spec formulas, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = SelectOp(column_specs=column_specs)
        return self.table_op_handler(table_op)

    def select_distinct(self, column_names: List[str] = []):
        """ Perform a select-distinct operation on the table and return the result table.

        Args:
            column_names (List[str], optional): the list of column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = SelectDistinctOp(column_names=column_names)
        return self.table_op_handler(table_op)

    def sort(self, column_names: List[str], directions: List[SortDirection] = []):
        """ Perform a sort operation on the table and return the result table.

        Args:
            column_names (List[str]): the names of the columns to be sorted on
            directions (List[SortDirection], optional): the corresponding sort directions for each sort column, default
                is empty. In the absence of explicit sort directions, data will be sorted in the ascending order.

        Returns:
            a Table object

        Raises:
            DHError


        """
        table_op = SortOp(column_names=column_names, directions=directions)
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

    def natural_join(self, table: Any, keys: List[str], columns_to_add: List[str] = []):
        """ Perform a natural-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            keys (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            columns_to_add (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = NaturalJoinOp(table=table, keys=keys, columns_to_add=columns_to_add)
        return self.table_op_handler(table_op)

    def exact_join(self, table: Any, keys: List[str], columns_to_add: List[str] = []):
        """ Perform a exact-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            keys (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            columns_to_add (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = ExactJoinOp(table=table, keys=keys, columns_to_add=columns_to_add)
        return self.table_op_handler(table_op)

    def left_join(self, table: Any, keys: List[str], columns_to_add: List[str] = []):
        """ Perform a left-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            keys (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            columns_to_add (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = LeftJoinOp(table=table, keys=keys, columns_to_add=columns_to_add)
        return self.table_op_handler(table_op)

    def join(self, table: Any, keys: List[str] = [], columns_to_add: List[str] = [], reserve_bits: int = 10):
        """ Perform a cross-join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            keys (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            columns_to_add (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            reserve_bits(int, optional): the number of bits of key-space to initially reserve per group; default is 10

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = CrossJoinOp(table=table, keys=keys, columns_to_add=columns_to_add, reserve_bits=reserve_bits)
        return self.table_op_handler(table_op)

    def aj(self, table: Any, keys: List[str], columns_to_add: List[str] = [], match_rule: MatchRule = MatchRule.LESS_THAN_EQUAL):
        """ Perform a as-of join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            keys (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            columns_to_add (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            match_rule (MatchRule, optional): the match rule for the as-of join, default is LESS_THAN_EQUAL

        Returns:
            a Table object

        Raises:
            DHError

        """
        match_rule = MatchRule.LESS_THAN if match_rule == MatchRule.LESS_THAN else MatchRule.LESS_THAN_EQUAL
        table_op = AsOfJoinOp(table=table, keys=keys, columns_to_add=columns_to_add, match_rule=match_rule)
        return self.table_op_handler(table_op)

    def raj(self, table: Any, keys: List[str], columns_to_add: List[str] = [], match_rule: MatchRule = MatchRule.GREATER_THAN_EQUAL):
        """ Perform a reverse as-of join between this table as the left table and another table as the right table) and
        returns the result table.

        Args:
            table (Table): the right-table of the join
            keys (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            columns_to_add (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            match_rule (MatchRule, optional): the match rule for the as-of join, default is GREATER_THAN_EQUAL

        Returns:
            a Table object

        Raises:
            DHError

        """
        match_rule = MatchRule.GREATER_THAN if match_rule == MatchRule.GREATER_THAN else MatchRule.GREATER_THAN_EQUAL
        table_op = AsOfJoinOp(table=table, keys=keys, columns_to_add=columns_to_add,
                              match_rule=match_rule)
        return self.table_op_handler(table_op)

    def head_by(self, num_rows: int, column_names: List[str]):
        """ Perform a head-by operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = HeadByOp(num_rows=num_rows, column_names=column_names)
        return self.table_op_handler(table_op)

    def tail_by(self, num_rows: int, column_names: List[str]):
        """ Perform a head-by operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the end of each group
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = TailByOp(num_rows=num_rows, column_names=column_names)
        return self.table_op_handler(table_op)

    def group_by(self, column_names: List[str] = []):
        """ Perform a group-by aggregation on the table and return the result table. After the operation,
        the columns not in the group-by columns become array-type.

        If the provided column_names is empty,the content of each column is grouped into its own array.

        Args:
            column_names (List[str], optional): the group-by column names; default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.ARRAY, column_names=column_names)
        return self.table_op_handler(table_op)

    def ungroup(self, column_names: List[str] = [], null_fill: bool = True):
        """ Perform an ungroup operation on the table and return the result table. The ungroup columns should be of
        array types.

        Args:
            column_names (List[str], optional): the names of the array columns, if empty, all array columns will be
                ungrouped, default is empty
            null_fill (bool, optional): indicates whether null should be used to fill missing cells, default is True

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = UngroupOp(column_names=column_names, null_fill=null_fill)
        return self.table_op_handler(table_op)

    def first_by(self, column_names: List[str] = []):
        """ Perform First-by aggregation on the table and return the result table which contains the first row of each
        distinct group.


        Args:
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.FIRST, column_names=column_names)
        return self.table_op_handler(table_op)

    def last_by(self, column_names: List[str] = []):
        """ Perform last-by aggregation on the table and return the result table which contains the last row of each
        distinct group.


        Args:
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.LAST, column_names=column_names)
        return self.table_op_handler(table_op)

    def sum_by(self, column_names: List[str] = []):
        """ Perform sum-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.SUM, column_names=column_names)
        return self.table_op_handler(table_op)

    def avg_by(self, column_names: List[str] = []):
        """ Perform avg-by aggregation on the table and return the result table. Columns not used in the grouping must
        be of numeric types.


        Args:
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.AVG, column_names=column_names)
        return self.table_op_handler(table_op)

    def std_by(self, column_names: List[str] = []):
        """ Perform std-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.STD, column_names=column_names)
        return self.table_op_handler(table_op)

    def var_by(self, column_names: List[str] = []):
        """ Perform var-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.VAR, column_names=column_names)
        return self.table_op_handler(table_op)

    def median_by(self, column_names: List[str] = []):
        """ Perform median-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.MEDIAN, column_names=column_names)
        return self.table_op_handler(table_op)

    def min_by(self, column_names: List[str] = []):
        """ Perform min-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.MIN, column_names=column_names)
        return self.table_op_handler(table_op)

    def max_by(self, column_names: List[str] = []):
        """ Perform max-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.MAX, column_names=column_names)
        return self.table_op_handler(table_op)

    def count_by(self, count_column: str, column_names: List[str] = []):
        """ Perform count-by operation on the table and return the result table. The count of each group is stored in
        a new column named after the 'count_column' parameter.


        Args:
            count_column (str): the name of the count column
            column_names (List[str], optional): the group-by column names, default is empty

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.COUNT, column_names=column_names, count_column=count_column)
        return self.table_op_handler(table_op)

    def count(self, count_column: str):
        """ Count the number of values in the 'count_column' on the table and return the result in a table with one row
        and one column.

        Args:
            count_column (str): the name of the column whose values to be counted

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = DedicatedAggOp(AggType.COUNT, count_column=count_column)
        return self.table_op_handler(table_op)

    def combo_by(self, column_names: List[str], combo_aggregation: ComboAggregation):
        """ Perform a Combined Aggregation operation on the table and return the result table.

        Args:
            column_names (List[str]): the group-by column names
            combo_aggregation (ComboAggregation): the combined aggregation definition

        Returns:
            a Table object

        Raises:
            DHError

        """
        table_op = ComboAggOp(column_names=column_names, combo_aggregation=combo_aggregation)
        return self.table_op_handler(table_op)
