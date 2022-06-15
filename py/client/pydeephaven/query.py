#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from pydeephaven import Table
from pydeephaven._table_ops import *
from pydeephaven.dherror import DHError
from pydeephaven._table_interface import TableInterface


class Query(TableInterface):
    """ A Query object is used to define and execute a sequence of Deephaven table operations on the server.

    When the query is executed, the table operations specified for the Query object are batched together and sent to the
    server in a single request, thus avoiding multiple round trips between the client and the server. The result of
    executing the query is a new Deephaven table.

    Note, an application should always use the factory method on the Session object to create a Query instance as the
    constructor is subject to future changes to support more advanced features already planned.
    """

    def table_op_handler(self, table_op):
        self._ops.append(table_op)
        return self

    def __init__(self, session, table):
        self.session = session
        if not self.session or not table:
            raise DHError("invalid session or table value.")
        self._ops = [NoneOp(table=table)]

    def exec(self):
        """ Execute the query on the server and return the result table.

        Returns:
            a Table object

        Raises:
            DHError
        """
        return self.session.table_service.batch(self._ops)

    def drop_columns(self, cols: List[str]):
        """ Add a drop-columns operation to the query.

        Args:
            cols (List[str]) : the list of column names

        Returns:
            self
        """
        return super().drop_columns(cols)

    def update(self, formulas: List[str]):
        """ Add an update operation to the query.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            self
        """
        return super().update(formulas)

    def lazy_update(self, formulas: List[str]):
        """ Add an lazy-update operation to the query.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            self
        """
        return super().lazy_update(formulas)

    def view(self, formulas: List[str]):
        """ Add a view operation to the query.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            self
        """
        return super().view(formulas)

    def update_view(self, formulas: List[str]):
        """ Add an update-view operation to the query.

        Args:
            formulas (List[str]): the column formulas

        Returns:
            self
        """
        return super().update_view(formulas)

    def select(self, formulas: List[str] = []):
        """ Add a select operation to the query.

        Args:
            formulas (List[str], optional): the column formulas, default is empty

        Returns:
            self
        """
        return super().select(formulas)

    def select_distinct(self, cols: List[str] = []):
        """ Add a select-distinct operation to the query.

        Args:
            cols (List[str], optional): the list of column names, default is empty

        Returns:
            self
        """
        return super().select_distinct(cols)

    def sort(self, order_by: List[str], order: List[SortDirection] = []):
        """ Add sort operation to the query.

        Args:
            order_by (List[str]): the names of the columns to be sorted on
            order (List[SortDirection], optional): the corresponding sort directions for each sort column, default
                is empty. In the absence of explicit sort directions, data will be sorted in the ascending order.

        Returns:
            self
        """
        return super().sort(order_by, order)

    def where(self, filters: List[str]):
        """ Add a filter operation to the query.

        Args:
            filters (List[str]): a list of filter condition expressions

        Returns:
            self
        """
        return super().where(filters)

    def head(self, num_rows: int):
        """ Add a head operation to the query.

        Args:
            num_rows (int): the number of rows at the head of table

        Returns:
            self
        """
        return super().head(num_rows)

    def tail(self, num_rows: int):
        """ Add a tail operation to the query.

        Args:
            num_rows (int): the number of rows at the end of table

        Returns:
            self
        """
        return super().tail(num_rows)

    def natural_join(self, table: Any, on: List[str], joins: List[str] = []):
        """ Add a natural-join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            self
        """
        return super().natural_join(table, on, joins)

    def exact_join(self, table: Any, on: List[str], joins: List[str] = []):
        """ Add a exact-join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty

        Returns:
            self
        """
        return super().exact_join(table, on, joins)

    def join(self, table: Any, on: List[str] = [], joins: List[str] = [], reserve_bits: int = 10):
        """ Add a cross-join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            reserve_bits(int, optional): the number of bits of key-space to initially reserve per group; default is 10

        Returns:
            self
        """
        return super().join(table, on, joins)

    def aj(self, table: Any, on: List[str], joins: List[str] = [], match_rule: MatchRule = MatchRule.LESS_THAN_EQUAL):
        """ Add a as-of join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            match_rule (MatchRule, optional): the match rule for the as-of join, default is LESS_THAN_EQUAL

        Returns:
            self
        """
        return super().aj(table, on, joins, match_rule)

    def raj(self, table: Any, on: List[str], joins: List[str] = [],
            match_rule: MatchRule = MatchRule.GREATER_THAN_EQUAL):
        """ Add a reverse as-of join operation to the query.

        Args:
            table (Table): the right-table of the join
            on (List[str]): the columns to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (List[str], optional): a list of the columns to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is empty
            match_rule (MatchRule, optional): the match rule for the as-of join, default is GREATER_THAN_EQUAL

        Returns:
            self
        """
        return super().raj(table, on, joins)

    def head_by(self, num_rows: int, by: List[str]):
        """ Add a head-by operation to the query.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            by (List[str]): the group-by column names

        Returns:
            self
        """
        return super().head_by(num_rows, by)

    def tail_by(self, num_rows: int, by: List[str]):
        """ Add a tail-by operation to the query.

        Args:
            num_rows (int): the number of rows at the end of each group
            by (List[str]): the group-by column names

        Returns:
            self
        """
        return super().tail_by(num_rows, by)

    def group_by(self, by: List[str] = []):
        """ Add a group-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names; default is empty

        Returns:
            self
        """
        return super().group_by(by)

    def ungroup(self, cols: List[str] = [], null_fill: bool = True):
        """ Add an ungroup operation to the query.

        Args:
            cols (List[str], optional): the names of the array columns, if empty, all array columns will be
                ungrouped, default is empty
            null_fill (bool, optional): indicates whether null should be used to fill missing cells, default is True

        Returns:
            self
        """
        return super().ungroup(cols, null_fill)

    def first_by(self, by: List[str] = []):
        """ Add a first-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().first_by(by)

    def last_by(self, by: List[str] = []):
        """ Add a last-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().last_by(by)

    def sum_by(self, by: List[str] = []):
        """ Add a sum-by aggregation to the query.

        Args:
            by (List[str]): the group-by column names

        Returns:
            self
        """
        return super().sum_by(by)

    def avg_by(self, by: List[str] = []):
        """ Add an avg-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().avg_by(by)

    def std_by(self, by: List[str] = []):
        """ Add a std-by aggregation to the query.

        Args:
            by (List[str]): the group-by column names

        Returns:
            self
        """
        return super().std_by(by)

    def var_by(self, by: List[str] = []):
        """ Add a var-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().var_by(by)

    def median_by(self, by: List[str] = []):
        """ Add a median-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().median_by(by)

    def min_by(self, by: List[str] = []):
        """ Add a min-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().min_by(by)

    def max_by(self, by: List[str] = []):
        """ Add a max-by aggregation to the query.

        Args:
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().max_by(by)

    def count_by(self, col: str, by: List[str] = []):
        """ Add a count-by aggregation to the query.

        Args:
            col (str): the name of the column to store the counts
            by (List[str], optional): the group-by column names, default is empty

        Returns:
            self
        """
        return super().count_by(col, by)

    def count(self, col: str):
        """ Add a count operation to the query.

        Args:
            col (str): the name of the column whose values to be counted

        Returns:
            self
        """
        return super().count(col)

    def agg_by(self, agg: ComboAggregation, by: List[str]):
        """ Add a Combined Aggregation operation to the query.

        Args:
            by (List[str]): the group-by column names
            agg (ComboAggregation): the combined aggregation definition

        Returns:
            self
        """
        return super().agg_by(agg, by)
