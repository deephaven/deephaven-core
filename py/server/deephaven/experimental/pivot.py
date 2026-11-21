#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

"""This module defines functions for creating pivot tables."""

from typing import Sequence, Union, Optional, Callable, Any
import re
from deephaven import DHError, empty_table
from deephaven.table import Table, PartitionedTable, multi_join
from deephaven.numpy import to_numpy
from deephaven.update_graph import auto_locking_ctx
from deephaven.jcompat import to_sequence


def _is_legal_column(s: str) -> bool:
    """Check if a column name is legal.

    Args:
        s (str): The column name to check.

    Returns:
        bool: True if the column name is legal, False otherwise.
    """
    return re.match("^[_a-zA-Z][_a-zA-Z0-9]*$", s) is not None


def _legalize_column(s: str) -> str:
    """Legalize a column name.  Invalid characters are replaced with underscores.
    The legalized column name is not guaranteed to be unique.

    Args:
        s (str): The column name to legalize.

    Returns:
        str: The legalized column name.

    Raises:
        ValueError: If the column name is empty.
    """
    if re.match("^[_a-zA-Z][_a-zA-Z0-9]*$", s):
        return s
    if re.match("^[_a-zA-Z].*$", s):
        return re.sub("[^_a-zA-Z0-9]", "_", s)
    return "_" + re.sub("[^_a-zA-Z0-9]", "_", s)


def pivot(table: Table, row_cols: Union[str, Sequence[str]], column_col: str, value_col: str,
          value_to_col_name: Optional[Callable[[Any], str]] = None) -> Table:
    """ Create a pivot table from the input table.

    NOTE: The schema of the pivot table is frozen at the time of creation.  As a result, columns in the output table
    will not change after the pivot table is created.  If the input table changes, the pivot table may not reflect the
    changes.

    Args:
        table (Table): The input table.
        row_cols (Union[str, Sequence[str]]): The row columns in the input table.
        column_col (str): The column column in the input table.
        value_col (str): The value column in the input table.
        value_to_col_name (Optional[Callable[[Any],str]]): A function that converts a value to a column name.
            The function should return a string that is a valid column name.
            If None (default), a string representation of the value is used as the column name, with invalid
            characters replaced by underscores.  The character replacement is not guaranteed to produce unique
            column names.
        
    Returns:
        Table: The pivot table.
        
    Raises:
        ValueError: If the input table is empty.
        DHError: If an error occurs while creating the pivot table.
    """
    row_cols = list(to_sequence(row_cols))
    ptable = table.partition_by(column_col)

    if not value_to_col_name:
        value_to_col_name = lambda x: _legalize_column(str(x))

    # Locking to ensure that the partitioned table doesn't change while creating the query
    with auto_locking_ctx(ptable):
        # TODO: this does not handle key changes in the constituent tables.  It should.
        keys = ptable.keys()
        key_values = to_numpy(table=keys, cols=[column_col])

        if len(key_values) == 0:
            return empty_table(0)

        tables = []
        col_names = set()

        for key, con in zip(key_values, ptable.constituent_tables):
            col_name = value_to_col_name(key[0])

            if not isinstance(col_name, str):
                raise DHError(
                    f"Value does not map to a string: value={key[0]} col_name={col_name} col_type={type(col_name)}")

            if not _is_legal_column(col_name):
                raise DHError(f"Value maps to an invalid column name: value={key[0]} col_name={col_name}")

            if col_name in col_names:
                raise DHError(f"Value maps to a duplicate column name: value={key[0]} col_name={col_name}")

            col_names.add(col_name)
            tables.append(con.view(row_cols + [f"{col_name}={value_col}"]))

    return multi_join(input=tables, on=row_cols).table()
