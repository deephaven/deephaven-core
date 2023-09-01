from typing import Callable, Union, List

import jpy

from deephaven import execution_context, DHError
from deephaven.execution_context import ExecutionContext
from deephaven.jcompat import j_lambda
from deephaven.table import Table

_J_Table = jpy.get_type("io.deephaven.engine.table.Table")
_J_Supplier = jpy.get_type('java.util.function.Supplier')
_J_FunctionGeneratedTableFactory = jpy.get_type("io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory")


def create(table_generator: Callable[[], Table],
           source_tables: Union[Table, List[Table]] = None,
           refresh_interval_ms: int = None,
           exec_context: ExecutionContext = None) -> Table:
    """
    Creates an abstract table that is generated by running the table_generator() function. The function will first be
    run to generate the table when this method is called, then subsequently either (a) whenever one of the
    'source_tables' ticks or (b) after refresh_interval_ms have elapsed. Either 'refresh_interval_ms' or
    'source_tables' must be set (but not both).

    The table definition must not change between invocations of the 'table_generator' function.

    Note that any tables used by the 'table_generator' function *MUST* be specified in the 'source_tables'.

    :param table_generator: The table generator function. This function must return a Table.
    :param source_tables: Source tables used by the 'table_generator' function. The 'table_generator' is rerun when any of these tables tick.
    :param refresh_interval_ms: Interval (in milliseconds) at which the 'table_generator' function is rerun.
    :param exec_context: Optionally, a custom execution context. If 'None', the current execution context is used.
    :return: A Table that is automatically regenerated when any of the source_tables tick. This table will be regenerated *after* any changes to the source_tables have been processed.
    """

    if refresh_interval_ms is None and source_tables is None:
        raise DHError("Either refresh_interval_ms or source_tables must be defined!")

    if refresh_interval_ms is not None and source_tables is not None:
        raise DHError("Only one of refresh_interval_ms and source_tables must be defined!")

    # If no execution context is provided, assume we want to use the current one.
    if exec_context is None:
        exec_context = execution_context.get_exec_ctx()

    def table_generator_function():
        with exec_context:
            result = table_generator()

        if result is None:
            raise DHError("table_generator did not return a result")

        if isinstance(result, Table) is False:
            raise DHError("table_generator did not return a Table")

        return result.j_table

    # Treat the table_generator_function as a Java lambda (i.e., wrap it in a Java Supplier):
    table_generator_j_function = j_lambda(table_generator_function, _J_Supplier)

    if refresh_interval_ms is not None:
        j_function_generated_table = _J_FunctionGeneratedTableFactory.create(
            table_generator_j_function,
            refresh_interval_ms
        )
    else:
        # Extract the underlying Java tables of any source_tables:
        source_j_tables = []
        if isinstance(source_tables, Table):
            source_j_tables.append(source_tables.j_table)
        else:
            for tbl in source_tables:
                source_j_tables.append(tbl.j_table)

        # Wrap the source_j_tables in a Java array:
        source_j_tables_jarray = jpy.array(_J_Table, source_j_tables)

        # Create the function-generated table:
        j_function_generated_table = _J_FunctionGeneratedTableFactory.create(
            table_generator_j_function,
            source_j_tables_jarray
        )

    return Table(j_function_generated_table)
