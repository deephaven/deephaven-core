#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Deephaven's learn module provides utilities for efficient data transfer between Deephaven tables and Python objects,
as well as a framework for using popular machine-learning / deep-learning libraries with Deephaven tables.
"""

from typing import List, Union, Callable, Type

import jpy

from deephaven import DHError
from deephaven.table import Table

_JLearnInput = jpy.get_type("io.deephaven.integrations.learn.Input")
_JLearnOutput = jpy.get_type("io.deephaven.integrations.learn.Output")
_JLearnComputer = jpy.get_type("io.deephaven.integrations.learn.Computer")
_JLearnScatterer = jpy.get_type("io.deephaven.integrations.learn.Scatterer")


class Input:
    """ Input specifies how to gather data from a Deephaven table into an object. """

    def __init__(self, col_names: Union[str, List[str]], gather_func: Callable):
        """  Initializes an Input object with the given arguments.

        Args:
            col_names (Union[str, List[str]]) : column name or list of column names from which to gather input.
            gather_func (Callable): function that determines how input gets transformed into an object.
        """
        self.input = _JLearnInput(col_names, gather_func)

    def __str__(self):
        """ Returns the Input object as a string containing a printable representation of the Input object."""
        return self.input.toString()


class Output:
    """ Output specifies how to scatter data from an object into a table column. """

    def __init__(self, col_name: str, scatter_func: Callable, col_type: Type):
        """ Initializes an Output object with the given arguments.

        Args:
            col_name (str) : name of the new column that will store results.
            scatter_func (Callable): function that determines how data is taken from an object and placed into a
                Deephaven table column.
            col_type (Type) : desired data type of the new output column, default is None (no explicit type cast).
        """
        self.output = _JLearnOutput(col_name, scatter_func, col_type)

    def __str__(self):
        """ Returns the Output object as a string containing a printable representation of the Output object. """
        return self.output.toString()


def _validate(inputs: Input, outputs: Output, table: Table):
    """ Ensures that all input columns exist in the table, and that no output column names already exist in the table.

    Args:
        inputs (Input)  : list of Inputs to validate.
        outputs (Output) : list of Outputs to validate.
        table (Table)  : table to check Input and Output columns against.

    Raises:
        ValueError : if at least one of the Input columns does not exist in the table.
        ValueError : if at least one of the Output columns already exists in the table.
        ValueError : if there are duplicates in the Output column names.
    """
    input_columns_list = [input_.input.getColNames()[i] for input_ in inputs for i in
                          range(len(input_.input.getColNames()))]
    input_columns = set(input_columns_list)
    table_columns = {col.name for col in table.columns}
    if table_columns >= input_columns:
        if outputs is not None:
            output_columns_list = [output.output.getColName() for output in outputs]
            output_columns = set(output_columns_list)
            if len(output_columns_list) != len(output_columns):
                repeats = set([column for column in output_columns_list if output_columns_list.count(column) > 1])
                raise ValueError(f"Cannot assign the same column name {repeats} to multiple columns.")
            elif table_columns & output_columns:
                overlap = output_columns & table_columns
                raise ValueError(
                    f"The columns {overlap} already exist in the table. Please choose Output column names that are "
                    f"not already in the table.")
    else:
        difference = input_columns - table_columns
        raise ValueError(f"Cannot find columns {difference} in the table.")


def _create_non_conflicting_col_name(table: Table, base_col_name: str) -> str:
    """ Creates a column name that is not present in the table.

    Args:
        table (Table): table to check column name against.
        base_col_name (str): base name to create a column from.

    Returns:
        column name that is not present in the table.
    """
    table_col_names = set([col.name for col in table.columns])
    if base_col_name not in table_col_names:
        return base_col_name
    else:
        i = 0
        while base_col_name in table_col_names:
            base_col_name = base_col_name + str(i)

        return base_col_name


def learn(table: Table = None, model_func: Callable = None, inputs: List[Input] = [], outputs: List[Output] = [],
          batch_size: int = None) -> Table:
    """ Learn gathers data from multiple rows of the input table, performs a calculation, and scatters values from the
    calculation into an output table. This is a common computing paradigm for artificial intelligence, machine learning,
    and deep learning.

    Args:
        table (Table): the Deephaven table to perform computations on.
        model_func (Callable): function that performs computations on the table.
        inputs (List[Input]): list of Input objects that determine how data gets extracted from the table.
        outputs (List[Output]): list of Output objects that determine how data gets scattered back into the results table.
        batch_size (int): maximum number of rows for which model_func is evaluated at once.

    Returns:
        a Table with added columns containing the results of evaluating model_func.

    Raises:
        DHError
    """

    try:
        _validate(inputs, outputs, table)

        if batch_size is None:
            raise ValueError("Batch size cannot be inferred. Please specify a batch size.")

        __computer = _JLearnComputer(table.j_table, model_func,
                [input_.input for input_ in inputs], batch_size)

        future_offset = _create_non_conflicting_col_name(table, "__FutureOffset")
        clean = _create_non_conflicting_col_name(table, "__CleanComputer")

        if outputs is not None:
            __scatterer = _JLearnScatterer([output.output for output in outputs])

            return (table
                    .update(formulas=[f"{future_offset} = __computer.compute(k)", ])
                    .update(formulas=[__scatterer.generateQueryStrings(f"{future_offset}"), ])
                    .update(formulas=[f"{clean} = __computer.clear()", ])
                    .drop_columns(cols=[f"{future_offset}", f"{clean}", ]))

        result = _create_non_conflicting_col_name(table, "__Result")

        # calling __computer.clear() in a separate update ensures calculations are complete before computer is cleared
        return (table
                .update(formulas=[
                    f"{future_offset} = __computer.compute(k)",
                    f"{result} = {future_offset}.getFuture().get()"
                ])
                .update(formulas=[
                    f"{clean} = __computer.clear()"
                ])
                .drop_columns(cols=[
                    f"{future_offset}",
                    f"{clean}",
                    f"{result}",
                ]))
    except Exception as e:
        raise DHError(e, "failed to complete the learn function.") from e
