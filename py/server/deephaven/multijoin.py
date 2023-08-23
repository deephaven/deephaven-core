#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module exposes the interfaces for efficient multi-table natural join."""
from typing import Sequence, Union, List
import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.table import Table
from deephaven.jcompat import to_sequence
from deephaven.update_graph import auto_locking_ctx

_JMultiJoinInput = jpy.get_type("io.deephaven.engine.table.MultiJoinInput")
_JMultiJoinTable = jpy.get_type("io.deephaven.engine.table.MultiJoinTable")
_JMultiJoinFactory = jpy.get_type("io.deephaven.engine.table.MultiJoinFactory")

class MultiJoinInput(JObjectWrapper):
    """A MultiJoinInput represents the input tables, key columns and additional columns to be used in the multi-table
    natural join. """
    j_object_type = _JMultiJoinInput

    @property
    def j_object(self) -> jpy.JType:
        return self.j_multijoininput

    def __init__(self, table: Table, on: Union[str, Sequence[str]], joins: Union[str, Sequence[str]] = None):
        """Creates a new MultiJoinInput containing the table to include for the join, the key columns from the table to
        match with other table keys plus additional columns containing data from the table. Rows containing unique keys
        will be added to the output table, otherwise the data from these columns will be added to the existing output
        rows.

        Args:
            table (Table): the right table to include in the join
            on (Union[str, Sequence[str]]): the column(s) to match, can be a common name or an equal expression,
                i.e. "col_a = col_b" for different column names
            joins (Union[str, Sequence[str]], optional): the column(s) to be added from the this table to the result
                table, can be renaming expressions, i.e. "new_col = col"; default is None

        Raises:
            DHError
        """
        try:
            self.table = table
            on = to_sequence(on)
            joins = to_sequence(joins)
            self.j_multijoininput = _JMultiJoinInput.of(table.j_table, on, joins)
        except Exception as e:
            raise DHError(e, "failed to build a MultiJoinInput object.") from e


class MultiJoinTable(JObjectWrapper):
    """A MultiJoinTable represents the result of a multi-table natural join. """
    j_object_type = _JMultiJoinTable

    @property
    def j_object(self) -> jpy.JType:
        return self.j_multijointable

    def table(self) -> Table:
        return Table(j_table=self.j_multijointable.table())

    def __init__(self, input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
                 on: Union[str, Sequence[str]] = None):
        """Creates a new MultiJoinTable. The join can be specified in terms of either tables or MultiJoinInputs.

        Args:
            input (Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]): the input objects
                specifying the tables and columns to include in the join.
            on (Union[str, Sequence[str]], optional): the column(s) to match, can be a common name or an equality
                expression that matches every input table, i.e. "col_a = col_b" to rename output column names.  When
                using MultiJoinInput objects, this parameter is ignored.

        Raises:
            DHError
        """
        try:
            if isinstance(input, Table) or (isinstance(input, Sequence) and all(isinstance(t, Table) for t in input)):
                tables = to_sequence(input, wrapped=True)
                with auto_locking_ctx(*tables):
                    j_tables = to_sequence(input)
                    self.j_multijointable = _JMultiJoinFactory.of(on, *j_tables)
            elif isinstance(input, MultiJoinInput) or (isinstance(input, Sequence) and all(isinstance(ji, MultiJoinInput) for ji in input)):
                wrapped_input = to_sequence(input, wrapped=True)
                tables = [ji.table for ji in wrapped_input]
                with auto_locking_ctx(*tables):
                    input = to_sequence(input)
                    self.j_multijointable = _JMultiJoinFactory.of(*input)
            else:
                raise DHError(message="input must be a Table, a sequence of Tables, a MultiJoinInput, or a sequence of MultiJoinInputs.")

        except Exception as e:
            raise DHError(e, "failed to build a MultiJoinTable object.") from e


""" The multi_join method creates a new table by performing a multi-table natural join on the input tables.  The result
consists of the set of distinct keys from the input tables natural joined to each input table. Input tables need not
have a matching row for each key, but they may not have multiple matching rows for a given key.
 
Args:
    input (Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]): the input objects specifying the
        tables and columns to include in the join.
    on (Union[str, Sequence[str]], optional): the column(s) to match, can be a common name or an equality expression
        that matches every input table, i.e. "col_a = col_b" to rename output column names.  When using MultiJoinInput
        objects, this parameter is ignored.
        
Returns:
    MultiJoinTable: the result of the multi-table natural join operation. To access the underlying Table, use the
        table() method.
"""
def multi_join(input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
               on: Union[str, Sequence[str]] = None) -> MultiJoinTable:
    return MultiJoinTable(input, on)
