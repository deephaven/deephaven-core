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

"""
The call syntax is the following:

# complex join
mj_input = [
    MultiJoinInput(table=t1, on="key"), # all columns added
    MultiJoinInput(table=t2, on="key=otherKey", joins=["col1", "col2"]), #specific columns added
]
multitable = multi_join(input=mj_input)

#simple joins
multitable = multi_join(tables=[t1,t2], on="common_key") # all columns from t1,t2 included in output
multitable = multi_join(tables=[t1,t2], on=["common_key1", "common_key2"]) # all columns from t1,t2 included

"""


class MultiJoinInput(JObjectWrapper):
    """A MultiJoinInput represents the input tables, key columns and additional columns to be used in the multiJoin. """
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
            table (Table): the table to include in the join
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
    """A MultiJoinTable represents the result of a multiJoin. """
    j_object_type = _JMultiJoinTable

    @property
    def j_object(self) -> jpy.JType:
        return self.j_multijointable

    def table(self) -> Table:
        return Table(j_table=self.j_multijointable.table())

    def __init__(self, input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
                 on: Union[str, Sequence[str]] = None):
        """Creates a new MultiJoinTable from MultiJoinInput or directly from Tables and key columns. Either
        MultiJoinInput objects or Table objects should be provided as the `input` parameter.

        Args:
            input (Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]): the input objects
                specifying the tables and columns to include in the join.
            on (Union[str, Sequence[str]], optional): the column(s) to match if Table objects are provided as the input
                parameter, must be a common name or an equality expression that matches every input table, i.e.
                "col_a = col_b" to rename output column names.  When using MultiJoinInput objects, this parameter
                is ignored.

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
                raise DHError(message="input must be Table or MultiJoinInput.")

        except Exception as e:
            raise DHError(e, "failed to build a MultiJoinTable object.") from e


"""Creates a new MultiJoinTable from MultiJoinInput or directly from tables and key columns. Either
MultiJoinInput objects or Table objects should be provided as the `input` parameter. 

Args:
    input (Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]): the input objects specifying the
        tables and columns to include in the join.
    on (Union[str, Sequence[str]], optional): the column(s) to match if Table objects are provided as the input 
        parameter, must be a common name or an equality expression that matches every input table, i.e. "col_a = col_b"
        to rename output column names.  When using MultiJoinInput objects, this parameter is ignored.
"""
def multi_join(input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
               on: Union[str, Sequence[str]] = None) -> MultiJoinTable:
    return MultiJoinTable(input, on)
