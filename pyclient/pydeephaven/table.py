#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from __future__ import annotations

from typing import List

import pyarrow

from pydeephaven._combo_aggs import ComboAggregation
from pydeephaven.constants import MatchRule, AggType, SortDirection
from pydeephaven.dherror import DHError


class Table:
    """ A Table object represents a reference to a table on the server. It is the core data structure of
    Deephaven and supports a rich set of operations such as filtering, sorting, aggregating, joining, snapshotting etc.

    Note, an application should never instantiate a Table object directly. Table objects are always provided through
    factory methods such as Session.empty_table(), or import/export methods such as Session.import_table(),
    open_table(), or any of the Table operations.

    Attributes:
        is_closed (bool): check if the table has been closed on the server
    """

    def __init__(self, session=None, ticket=None, schema_header=b'', size=None, is_static=None, schema=None):
        if not session or not session.is_alive:
            raise DHError("Must be associated with a active session")
        self.session = session
        self.ticket = ticket
        self.schema = schema
        self.is_static = is_static
        self.size = size
        self.cols = []
        if not schema:
            self._parse_schema(schema_header)

    def __del__(self):
        try:
            if self.ticket and self.session.is_alive:
                self.session.release(self.ticket)
        except Exception as e:
            # TODO: log the exception
            ...

    @property
    def is_closed(self):
        return not self.ticket

    def close(self) -> None:
        """ Close the table reference on the server.

        Args:

        Returns:

        Raises:
            DHError

        """
        self.session.release(self.ticket)
        self.ticket = None

    def _parse_schema(self, schema_header):
        if not schema_header:
            return

        reader = pyarrow.ipc.open_stream(schema_header)
        self.schema = reader.schema

    def snapshot(self) -> pyarrow.Table:
        """ Take a snapshot of the table and return a pyarrow Table.

        Args:

        Returns:
            a pyarrow.Table

        Raises:
            DHError

        """
        return self.session.flight_service.snapshot_table(self)

    def drop_columns(self, column_names: List[str]) -> Table:
        """ Drop the specified columns from the table and return the result table.

        Args:
            column_names (List[str]) : the list of column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.drop_columns(self, column_names=column_names)

    def update(self, column_specs: List[str]) -> Table:
        """ Perform an update operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.usv_table(self, command="Update", column_specs=column_specs)

    def lazy_update(self, column_specs: List[str]) -> Table:
        """ Perform a lazy-update operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.usv_table(self, command="LazyUpdate", column_specs=column_specs)

    def view(self, column_specs: List[str]) -> Table:
        """ Perform a view operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.usv_table(self, command="View", column_specs=column_specs)

    def update_view(self, column_specs: List[str]) -> Table:
        """ Perform a update-view operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.usv_table(self, command="UpdateView", column_specs=column_specs)

    def select(self, column_specs: List[str]) -> Table:
        """ Perform a select operation on the table and return the result table.

        Args:
            column_specs (List[str]): the column spec formulas

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.usv_table(self, command="Select", column_specs=column_specs)

    def select_distinct(self, column_names: List[str]) -> Table:
        """ Perform a select-distinct operation on the table and return the result table.

        Args:
            column_names (List[str]): the list of column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.select_distinct(self, column_names=column_names)

    def sort(self, column_names: List[str], directions: List[SortDirection]) -> Table:
        """ Perform a sort operation on the table and return the result table.

        Args:
            column_names (List[str]): the names of the columns to be sorted on
            directions (List[SortDirection]): the corresponding sort directions for each sort column

        Returns:
            a Table object

        Raises:
            DHError


        """
        return self.session.table_service.sort(self, column_names=column_names, directions=directions)

    def filter(self, filters: List[str]) -> Table:
        """ Perform a filter operation on the table and return the result table.

        Args:
            filters (List[str]): a list of filter condition expressions

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.unstructured_filter(self, filters=filters)

    def head(self, num_rows: int) -> Table:
        """ Perform a head operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the head of table

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.head_or_tail(self, "Head", num_rows=num_rows)

    def tail(self, num_rows: int) -> Table:
        """ Perform a tail operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the end of table

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.head_or_tail(self, "Tail", num_rows=num_rows)

    def natural_join(self, table: Table, keys: List[str], columns_to_add: List[str] = []) -> Table:
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
        return self.session.table_service.nel_join(self, table, "natural", columns_to_match=keys,
                                                   columns_to_add=columns_to_add)

    def exact_join(self, table: Table, keys: List[str], columns_to_add: List[str] = []) -> Table:
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
        return self.session.table_service.nel_join(self, table, "exact", columns_to_match=keys,
                                                   columns_to_add=columns_to_add)

    def left_join(self, table: Table, keys: List[str], columns_to_add: List[str] = []) -> Table:
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
        return self.session.table_service.nel_join(self, table, "left", columns_to_match=keys,
                                                   columns_to_add=columns_to_add)

    def join(self, table: Table, keys: List[str] = [], columns_to_add: List[str] = [], reserve_bits: int = 10) -> Table:
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
        return self.session.table_service.cross_join(self, table, columns_to_match=keys,
                                                     columns_to_add=columns_to_add, reserve_bits=reserve_bits)

    def aj(self, table: Table, keys: List[str], columns_to_add: List[str] = []) -> Table:
        """ Perform a as-of join between this table as the left table and another table as the right table) and
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
        return self.session.table_service.as_of_join(self, table, columns_to_match=keys,
                                                     columns_to_add=columns_to_add,
                                                     match_rule=MatchRule.LESS_THAN_EQUAL)

    def raj(self, table: Table, keys: List[str], columns_to_add: List[str] = []) -> Table:
        """ Perform a reverse as-of join between this table as the left table and another table as the right table) and
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
        return self.session.table_service.as_of_join(self, table, columns_to_match=keys,
                                                     columns_to_add=columns_to_add,
                                                     match_rule=MatchRule.GREATER_THAN_EQUAL)

    def head_by(self, num_rows: int, column_names: List[str]) -> Table:
        """ Perform a head-by operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.head_or_tail_by(self, "HeadBy", num_rows=num_rows, column_names=column_names)

    def tail_by(self, num_rows: int, column_names: List[str]) -> Table:
        """ Perform a head-by operation on the table and return the result table.

        Args:
            num_rows (int): the number of rows at the end of each group
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.head_or_tail_by(self, "TailBy", num_rows=num_rows, column_names=column_names)

    def group_by(self, column_names: List[str] = []) -> Table:
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
        return self.session.table_service.group_by(self, group_by_columns=column_names)

    def ungroup(self, column_names: List[str], null_fill: bool = True) -> Table:
        """ Perform an ungroup operation on the table and return the result table. The ungroup columns should be of
        array types.

        Args:
            column_names (List[str]): the names of the array columns, if empty, all array columns will be ungrouped.
            null_fill (bool, optional): indicates whether null should be used to fill missing cells, default is True

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.ungroup(self, null_fill=null_fill, columns_to_ungroup=column_names)

    def first_by(self, column_names: List[str]) -> Table:
        """ Perform First-by aggregation on the table and return the result table which contains the first row of each
        distinct group.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.FIRST,
                                                               group_by_columns=column_names)

    def last_by(self, column_names: List[str]) -> Table:
        """ Perform last-by aggregation on the table and return the result table which contains the last row of each
        distinct group.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.LAST,
                                                               group_by_columns=column_names)

    def sum_by(self, column_names: List[str]) -> Table:
        """ Perform sum-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.SUM,
                                                               group_by_columns=column_names)

    def avg_by(self, column_names: List[str]) -> Table:
        """ Perform avg-by aggregation on the table and return the result table. Columns not used in the grouping must
        be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.AVG,
                                                               group_by_columns=column_names)

    def std_by(self, column_names: List[str]) -> Table:
        """ Perform std-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.STD,
                                                               group_by_columns=column_names)

    def var_by(self, column_names: List[str]) -> Table:
        """ Perform var-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.VAR,
                                                               group_by_columns=column_names)

    def median_by(self, column_names: List[str]) -> Table:
        """ Perform median-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.MEDIAN,
                                                               group_by_columns=column_names)

    def min_by(self, column_names: List[str]) -> Table:
        """ Perform min-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.MIN,
                                                               group_by_columns=column_names)

    def max_by(self, column_names: List[str]) -> Table:
        """ Perform max-by aggregation on the table and return the result table. Columns not used in the grouping
        must be of numeric types.


        Args:
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.MAX,
                                                               group_by_columns=column_names)

    def count_by(self, count_column: str, column_names: List[str]) -> Table:
        """ Perform count-by operation on the table and return the result table. The count of each group is stored in
        a new column named after the 'count_column' parameter.


        Args:
            count_column (str): the name of the count column
            column_names (List[str]): the group-by column names

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.COUNT,
                                                               group_by_columns=column_names,
                                                               count_column=count_column)

    def count(self, count_column: str) -> Table:
        """ Count the number of values in the 'count_column' on the table and return the result in a table with one row
        and one column.

        Args:
            count_column (str): the name of the column whose values to be counted

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.dedicated_aggregator(self, agg_type=AggType.COUNT,
                                                               count_column=count_column)

    def combo_by(self, column_names: List[str], combo_aggregation: ComboAggregation) -> Table:
        """ Perform a Combined Aggregation operation on the table and return the result table.

        Args:
            column_names (List[str]): the group-by column names
            combo_aggregation (ComboAggregation): the combined aggregation definition

        Returns:
            a Table object

        Raises:
            DHError

        """
        return self.session.table_service.combo_aggregate(self, group_by_columns=column_names,
                                                          agg_list=combo_aggregation.aggregates)


class EmptyTable(Table):
    """ A EmptyTable object represents a table with no columns on the server."""

    def __init__(self, session=None, ticket=None, schema_header=b'', size=0, is_static=True):
        super().__init__(session=session, ticket=ticket, schema_header=schema_header,
                         size=size)


class TimeTable(Table):
    """ A TimeTable object represents a ticking table on the server. """

    def __init__(self, session=None, ticket=None, schema_header=b'', start_time=None, period=1000000000,
                 is_static=False):
        super().__init__(session=session, ticket=ticket, schema_header=schema_header,
                         is_static=is_static)
        self.start_time = start_time
        self.period = period
