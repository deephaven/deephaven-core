#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module implements the Table and InputTable classes which are the main instruments to work with Deephaven
data."""

from __future__ import annotations

from typing import List, Union

import pyarrow as pa
from pydeephaven._utils import to_list

from pydeephaven._table_ops import MetaTableOp, SortDirection
from pydeephaven.agg import Aggregation
from pydeephaven.dherror import DHError
from pydeephaven._table_interface import TableInterface
from pydeephaven.updateby import UpdateByOperation

from pydeephaven.experimental.server_object import ServerObject


class Table(TableInterface, ServerObject):
    """A Table object represents a reference to a table on the server. It is the core data structure of
    Deephaven and supports a rich set of operations such as filtering, sorting, aggregating, joining, snapshotting etc.

    Note, an application should never instantiate a Table object directly. Table objects are always provided through
    factory methods such as Session.empty_table(), or import/export methods such as Session.import_table(),
    open_table(), or any of the Table operations.

    Attributes:
        is_closed (bool): check if the table has been closed on the server
    """

    def table_op_handler(self, table_op):
        return self.session.table_service.grpc_table_op(self, table_op)

    def __init__(self, session, ticket, schema_header=b'', size=None, is_static=None, schema=None):
        ServerObject.__init__(self, type_="Table", ticket=ticket)
        if not session or not session.is_alive:
            raise DHError("Must be associated with a active session")
        self.session = session
        self.ticket = ticket
        self.schema = schema
        self.is_static = is_static
        self.size = size
        if not schema:
            self._parse_schema(schema_header)
        self._meta_table = None

    def __del__(self):
        try:
            # only table objects that are explicitly exported have schema info and only the tickets associated with
            # such tables should be released.
            if self.ticket and self.schema and self.session.is_alive:
                self.session.release(self.ticket)
        except Exception as e:
            # TODO(deephaven-core#1858): Better error handling for pyclient around release #1858
            pass

    @property
    def is_refreshing(self) -> bool:
        """Whether this table is refreshing."""
        return not self.is_static

    @property
    def is_closed(self) -> bool:
        """Whether this table is closed on the server."""
        return not self.ticket

    def close(self) -> None:
        """Close the table reference on the server.

        Raises:
            DHError
        """
        self.session.release(self.ticket)
        self.ticket = None

    def _parse_schema(self, schema_header):
        if not schema_header:
            return

        reader = pa.ipc.open_stream(schema_header)
        self.schema = reader.schema

    @property
    def meta_table(self) -> Table:
        """The column definitions of the table in a Table form."""
        if self._meta_table is None:
            table_op = MetaTableOp()
            self._meta_table = self.session.table_service.grpc_table_op(self, table_op)
        return self._meta_table

    def to_arrow(self) -> pa.Table:
        """Takes a snapshot of the table and returns a pyarrow Table.

        Returns:
            a pyarrow.Table

        Raises:
            DHError
        """
        return self.session.flight_service.do_get_table(self)

    def drop_columns(self, cols: Union[str, List[str]]) -> Table:
        """The drop_column method creates a new table with the same size as this table but omits any of the specified 
        columns. 

        Args:
            cols (Union[str, List[str]]) : the column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().drop_columns(cols)

    def update(self, formulas: Union[str, List[str]]) -> Table:
        """The update method creates a new table containing a new, in-memory column for each formula.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().update(formulas)

    def lazy_update(self, formulas: Union[str, List[str]]) -> Table:
        """The lazy_update method creates a new table containing a new, cached, formula column for each formula.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().lazy_update(formulas)

    def view(self, formulas: Union[str, List[str]]) -> Table:
        """The view method creates a new formula table that includes one column for each formula.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().view(formulas)

    def update_view(self, formulas: Union[str, List[str]]) -> Table:
        """The update_view method creates a new table containing a new, formula column for each formula.

        Args:
            formulas (Union[str, List[str]]): the column formula(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().update_view(formulas)

    def select(self, formulas: Union[str, List[str]] = None) -> Table:
        """The select method creates a new in-memory table that includes one column for each formula. If no formula 
        is specified, all columns will be included. 

        Args:
            formulas (Union[str, List[str]], optional): the column formula(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().select(formulas)

    def select_distinct(self, cols: Union[str, List[str]] = None) -> Table:
        """The select_distinct method creates a new table containing all the unique values for a set of key columns. 
        When the selectDistinct method is used on multiple columns, it looks for distinct sets of values in the 
        selected columns. 

        Args:
            cols (Union[str, List[str]], optional): the column name(s), default is None

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().select_distinct(cols)

    def sort(self, order_by: Union[str, List[str]], order: Union[SortDirection, List[SortDirection]] = None) -> Table:
        """The sort method creates a new table where the rows are ordered based on values in the specified set of 
        columns. 

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
        return super().sort(order_by, order)

    def sort_descending(self, order_by: Union[str, List[str]]) -> Table:
        """The sort_descending method creates a new table where rows in a table are sorted in descending
        order based on the order_by column(s).

        Args:
            order_by (Union[str, List[str]]): the column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        order_by = to_list(order_by)
        order = SortDirection.DESCENDING
        return super().sort(order_by, order)

    def where(self, filters: Union[str, List[str]]) -> Table:
        """The where method creates a new table with only the rows meeting the filter criteria in the column(s) of 
        the table. 

        Args:
            filters (Union[str, List[str]]): the filter condition expression(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().where(filters)

    def head(self, num_rows: int) -> Table:
        """The head method creates a new table with a specific number of rows from the beginning of the table. 

        Args:
            num_rows (int): the number of rows at the head of table

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().head(num_rows)

    def tail(self, num_rows: int) -> Table:
        """The tail method creates a new table with a specific number of rows from the end of the table. 

        Args:
            num_rows (int): the number of rows at the end of table

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).tail(num_rows)

    def natural_join(self, table: Table, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Table:
        """The natural_join method creates a new table containing all the rows and columns of this table, 
        plus additional columns containing data from the right table. For columns appended to the left table (joins), 
        row values equal the row values from the right table where the key values in the left and right tables are 
        equal. If there is no matching key in the right table, appended row values are NULL. 

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None, which means all the columns
                from the right table, excluding those specified in 'on'

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super().natural_join(table, on, joins)

    def exact_join(self, table: Table, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Table:
        """The exact_join method creates a new table containing all the rows and columns of this table plus 
        additional columns containing data from the right table. For columns appended to the left table (joins), 
        row values equal the row values from the right table where the key values in the left and right tables are 
        equal. 

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names              
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None, which means all the columns
                from the right table, excluding those specified in 'on'

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).exact_join(table, on, joins)

    def join(self, table: Table, on: Union[str, List[str]] = None, joins: Union[str, List[str]] = None,
             reserve_bits: int = 10) -> Table:
        """The join method creates a new table containing rows that have matching values in both tables. Rows that do 
        not have matching criteria will not be included in the result. If there are multiple matches between a row 
        from the left table and rows from the right table, all matching combinations will be included. If no columns 
        to match (on) are specified, every combination of left and right table rows is included. 

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names, default is None
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None, which means all the columns
                from the right table, excluding those specified in 'on'
            reserve_bits(int, optional): the number of bits of key-space to initially reserve per group; default is 10

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).join(table, on, joins)

    def aj(self, table: Table, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Table:
        """The aj (as-of join) method creates a new table containing all the rows and columns of the left table, 
        plus additional columns containing data from the right table. For columns appended to the left table (joins), 
        row values equal the row values from the right table where the keys from the left table most closely match 
        the keys from the right table without going over. If there is no matching key in the right table, 
        appended row values are NULL. 

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '>' or '>='.  If a common name is used for the inexact match,
                '>=' is used for the comparison.
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None, which means all the columns
                from the right table, excluding those specified in 'on'

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).aj(table, on, joins)

    def raj(self, table: Table, on: Union[str, List[str]], joins: Union[str, List[str]] = None) -> Table:
        """The raj (reverse as-of join) method creates a new table containing all the rows and columns of the left 
        table, plus additional columns containing data from the right table. For columns appended to the left table (
        joins), row values equal the row values from the right table where the keys from the left table most closely 
        match the keys from the right table without going under. If there is no matching key in the right table, 
        appended row values are NULL. 

        Args:
            table (Table): the right-table of the join
            on (Union[str, List[str]]): the column(s) to match, can be a common name or a match condition of two
                columns, e.g. 'col_a = col_b'. The first 'N-1' matches are exact matches.  The final match is an inexact
                match.  The inexact match can use either '<' or '<='.  If a common name is used for the inexact match,
                '<=' is used for the comparison.
            joins (Union[str, List[str]], optional): the column(s) to be added from the right table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None, which means all the columns
                from the right table, excluding those specified in 'on'

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).raj(table, on, joins)

    def head_by(self, num_rows: int, by: Union[str, List[str]]) -> Table:
        """The head_by method creates a new table containing the first number of rows for each group.

        Args:
            num_rows (int): the number of rows at the beginning of each group
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).head_by(num_rows, by)

    def tail_by(self, num_rows: int, by: Union[str, List[str]]) -> Table:
        """The tail_by method creates a new table containing the last number of rows for each group.

        Args:
            num_rows (int): the number of rows at the end of each group
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).tail_by(num_rows, by)

    def group_by(self, by: Union[str, List[str]] = None) -> Table:
        """The group_by method creates a new table containing grouping columns and grouped data, column content is 
        grouped into arrays. 

        If no group-by column is given, the content of each column is grouped into its own array.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).group_by(by)

    def ungroup(self, cols: Union[str, List[str]] = None, null_fill: bool = True) -> Table:
        """The ungroup method creates a new table in which array columns from the source table are unwrapped into 
        separate rows. The ungroup columns should be of array types. 

        Args:
            cols (Union[str, List[str]], optional): the array column(s), default is None, meaning all array columns will
                be ungrouped, default is None, meaning all array columns will be ungrouped
            null_fill (bool, optional): indicates whether null should be used to fill missing cells, default is True

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).ungroup(cols, null_fill)

    def first_by(self, by: Union[str, List[str]] = None) -> Table:
        """The first_by method creates a new table which contains the first row of each distinct group.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).first_by(by)

    def last_by(self, by: Union[str, List[str]] = None) -> Table:
        """The last_by method creates a new table which contains the last row of each distinct group.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).last_by(by)

    def sum_by(self, by: Union[str, List[str]] = None) -> Table:
        """The sum_by method creates a new table containing the sum for each group. Columns not used in the grouping
        must be of numeric types.

        Args:
            by (Union[str, List[str]]): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).sum_by(by)

    def avg_by(self, by: Union[str, List[str]] = None) -> Table:
        """The avg_by method creates a new table containing the average for each group. Columns not used in the
        grouping must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).avg_by(by)

    def std_by(self, by: Union[str, List[str]] = None) -> Table:
        """The std_by method creates a new table containing the sample standard deviation for each group. Columns not
        used in the grouping must be of numeric types.

        Sample standard deviation is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
        which ensures that the sample variance will be an unbiased estimator of population variance.

        Args:
            by (Union[str, List[str]]): the group-by column names(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).std_by(by)

    def var_by(self, by: Union[str, List[str]] = None) -> Table:
        """The var_by method creates a new table containing the sample variance for each group. Columns not used in the
        grouping must be of numeric types.

        Sample variance is computed using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
        which ensures that the sample variance will be an unbiased estimator of population variance.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).var_by(by)

    def median_by(self, by: Union[str, List[str]] = None) -> Table:
        """The median_by method creates a new table containing the median for each group. Columns not used in the
        grouping must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).median_by(by)

    def min_by(self, by: Union[str, List[str]] = None) -> Table:
        """The min_by method creates a new table containing the minimum value for each group. Columns not used in the
        grouping must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).min_by(by)

    def max_by(self, by: Union[str, List[str]] = None) -> Table:
        """The max_by method creates a new table containing the maximum value for each group. Columns not used in the
        grouping must be of numeric types.

        Args:
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).max_by(by)

    def count_by(self, col: str, by: Union[str, List[str]] = None) -> Table:
        """The count_by method creates a new table containing the number of rows for each group. The count of each
        group is stored in a new column named after the 'col' parameter.

        Args:
            col (str): the name of the column to store the counts
            by (Union[str, List[str]], optional): the group-by column name(s), default is None, meaning grouping
                all the rows into one group

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).count_by(col, by)

    def agg_by(self, aggs: Union[Aggregation, List[Aggregation]], by: Union[str, List[str]]) -> Table:
        """The agg_by method creates a new table containing grouping columns and grouped data. The resulting grouped
        data is defined by the aggregation(s) specified.

        Args:
            aggs (Union[Aggregation, List[Aggregation]]): the aggregation(s) to be applied
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).agg_by(aggs, by)

    def agg_all_by(self, agg: Aggregation, by: Union[str, List[str]]) -> Table:
        """The agg_all_by method creates a new table containing grouping columns and grouped data. The resulting
        grouped data is defined by the aggregation specified.

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
        return super(Table, self).agg_all_by(agg, by)

    def update_by(self, ops: Union[UpdateByOperation, List[UpdateByOperation]], by: Union[str, List[str]]) -> Table:
        """The update_by method creates a table with additional columns calculated from
        window-based aggregations of columns in this table. The aggregations are defined by the provided operations,
        which support incremental aggregations over the corresponding rows in the table. The aggregations will
        apply position or time-based windowing and compute the results over the entire table or each row group as
        identified by the provided key columns.

        Args:
            ops (Union[UpdateByOperatoin, List[UpdateByOperation]]): the UpdateByOperation(s) to be applied
            by (Union[str, List[str]]): the group-by column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).update_by(ops, by)

    def snapshot(self) -> Table:
        """The snapshot method creates a static snapshot table.

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).snapshot()

    def snapshot_when(self, trigger_table: Table, stamp_cols: Union[str, List[str]] = None, initial: bool = False,
                      incremental: bool = False, history: bool = False) -> Table:
        """The snapshot_when creates a table that captures a snapshot of this table whenever trigger_table updates.

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
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).snapshot_when(trigger_table, stamp_cols, initial, incremental, history)

    def where_in(self, filter_table: Table, cols: Union[str, List[str]]) -> Table:
        """The where_in method creates a new table containing rows from the source table, where the rows match values
        in the filter table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, List[str]]): the column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).where_in(filter_table, cols)

    def where_not_in(self, filter_table: Table, cols: Union[str, List[str]]) -> Table:
        """The where_not_in method creates a new table containing rows from the source table, where the rows do not
        match values in the filter table.

        Args:
            filter_table (Table): the table containing the set of values to filter on
            cols (Union[str, List[str]]): the column name(s)

        Returns:
            a Table object

        Raises:
            DHError
        """
        return super(Table, self).where_not_in(filter_table, cols)


class InputTable(Table):
    """InputTable is a subclass of Table that allows the users to dynamically add/delete/modify data in it. There are
    two types of InputTable - append-only and keyed.

    The append-only input table is not keyed, all rows are added to the end of the table, and deletions and edits are
    not permitted.

    The keyed input tablet has keys for each row and supports addition/deletion/modification of rows by the keys.
    """

    def __init__(self, session, ticket, schema_header=b'', size=None, is_static=None, schema=None):
        super().__init__(session=session, ticket=ticket, schema_header=schema_header, size=size,
                         is_static=is_static, schema=schema)
        self.key_cols: List[str] = None

    def add(self, table: Table) -> None:
        """Writes rows from the provided table to this input table. If this is a keyed input table, added rows with keys
        that match existing rows will replace those rows.

        Args:
            table (Table): the table that provides the rows to write

        Raises:
            DHError
        """
        try:
            self.session.input_table_service.add(self, table)
        except Exception as e:
            raise DHError("add to InputTable failed.") from e

    def delete(self, table: Table) -> None:
        """Deletes the keys contained in the provided table from this keyed input table. If this method is called on an
        append-only input table, a PermissionError will be raised.

        Args:
            table (Table): the table with the keys to delete

        Raises:
            DHError, PermissionError
        """
        if not self.key_cols:
            raise PermissionError("deletion on an append-only input table is not allowed.")
        try:
            self.session.input_table_service.delete(self, table)
        except Exception as e:
            raise DHError("delete data in the InputTable failed.") from e
