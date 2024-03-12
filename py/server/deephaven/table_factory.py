#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides various ways to make a Deephaven table. """

import datetime
from typing import Callable, List, Dict, Any, Union, Sequence, Tuple, Mapping

import jpy
import numpy as np
import pandas as pd

from deephaven import execution_context, DHError, time
from deephaven._wrapper import JObjectWrapper
from deephaven.column import InputColumn, Column
from deephaven.dtypes import DType, Duration, Instant
from deephaven.execution_context import ExecutionContext
from deephaven.jcompat import j_lambda, j_list_to_list, to_sequence
from deephaven.table import Table
from deephaven.update_graph import auto_locking_ctx

_JTableFactory = jpy.get_type("io.deephaven.engine.table.TableFactory")
_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")
_JDynamicTableWriter = jpy.get_type("io.deephaven.engine.table.impl.util.DynamicTableWriter")
_JBaseArrayBackedInputTable = jpy.get_type("io.deephaven.engine.table.impl.util.BaseArrayBackedInputTable")
_JAppendOnlyArrayBackedInputTable = jpy.get_type(
    "io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable")
_JKeyedArrayBackedInputTable = jpy.get_type("io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable")
_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")
_JTable = jpy.get_type("io.deephaven.engine.table.Table")
_J_INPUT_TABLE_ATTRIBUTE = _JTable.INPUT_TABLE_ATTRIBUTE
_JRingTableTools = jpy.get_type("io.deephaven.engine.table.impl.sources.ring.RingTableTools")
_JSupplier = jpy.get_type('java.util.function.Supplier')
_JFunctionGeneratedTableFactory = jpy.get_type("io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory")


def empty_table(size: int) -> Table:
    """Creates a table with rows but no columns.

    Args:
        size (int): the number of rows

    Returns:
         a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableTools.emptyTable(size))
    except Exception as e:
        raise DHError(e, "failed to create an empty table.") from e


def time_table(period: Union[Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta],
               start_time: Union[None, Instant, int, str, datetime.datetime, np.datetime64, pd.Timestamp] = None,
               blink_table: bool = False) -> Table:
    """Creates a table that adds a new row on a regular interval.

    Args:
        period (Union[dtypes.Duration, int, str, datetime.timedelta, np.timedelta64, pd.Timedelta]):
            time interval between new row additions, can be expressed as an integer in nanoseconds,
            a time interval string, e.g. "PT00:00:00.001" or "PT1s", or other time duration types.
        start_time (Union[None, Instant, int, str, datetime.datetime, np.datetime64, pd.Timestamp], optional):
            start time for adding new rows, defaults to None which means use the current time
            as the start time.
        blink_table (bool, optional): if the time table should be a blink table, defaults to False

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        builder = _JTableTools.timeTableBuilder()

        if not isinstance(period, str) and not isinstance(period, int):
            period = time.to_j_duration(period)

        builder.period(period)

        if start_time:
            start_time = time.to_j_instant(start_time)
            builder.startTime(start_time)

        if blink_table:
            builder.blinkTable(blink_table)

        return Table(j_table=builder.build())
    except Exception as e:
        raise DHError(e, "failed to create a time table.") from e


def new_table(cols: Union[List[InputColumn], Mapping[str, Sequence]]) -> Table:
    """Creates an in-memory table from a list of input columns or a Dict (mapping) of column names and column data.
    Each column must have an equal number of elements.

    When the input is a mapping, an intermediary Pandas DataFrame is created from the mapping, which then is converted
    to an in-memory table. In this case, as opposed to when the input is a list of InputColumns, the column types are
    determined by Pandas' type inference logic.

    Args:
        cols (Union[List[InputColumn], Mapping[str, Sequence]]): a list of InputColumns or a mapping of columns
            names and column data.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        if isinstance(cols, list):
            return Table(j_table=_JTableFactory.newTable(*[col.j_column for col in cols]))
        else:
            from deephaven.pandas import to_table
            df = pd.DataFrame(cols).convert_dtypes()
            return to_table(df)
    except Exception as e:
        raise DHError(e, "failed to create a new time table.") from e


def merge(tables: List[Table]):
    """Combines two or more tables into one aggregate table. This essentially appends the tables one on top of the
    other. Null tables are ignored.

    Args:
        tables (List[Table]): the source tables

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        with auto_locking_ctx(*tables):
            return Table(j_table=_JTableTools.merge([t.j_table for t in tables]))
    except Exception as e:
        raise DHError(e, "merge tables operation failed.") from e


def merge_sorted(tables: List[Table], order_by: str) -> Table:
    """Combines two or more tables into one sorted, aggregate table. This essentially stacks the tables one on top
    of the other and sorts the result. Null tables are ignored. mergeSorted is more efficient than using merge
    followed by sort.

    Args:
        tables (List[Table]): the source tables
        order_by (str): the name of the key column

    Returns:
         a Table

    Raises:
        DHError
    """
    try:
        with auto_locking_ctx(*tables):
            return Table(j_table=_JTableTools.mergeSorted(order_by, *[t.j_table for t in tables]))
    except Exception as e:
        raise DHError(e, "merge sorted operation failed.") from e


class DynamicTableWriter(JObjectWrapper):
    """The DynamicTableWriter creates a new in-memory table and supports writing data to it.

    This class implements the context manager protocol and thus can be used in with statements.
    """

    j_object_type = _JDynamicTableWriter

    def __init__(self, col_defs: Dict[str, DType]):
        """Initializes the writer and creates a new in-memory table.

        Args:
            col_defs(Dict[str, DTypes]): a map of column names and types of the new table

        Raises:
            DHError
        """
        col_names = list(col_defs.keys())
        col_dtypes = list(col_defs.values())
        try:
            self._j_table_writer = _JDynamicTableWriter(col_names, [t.qst_type for t in col_dtypes])
            self.table = Table(j_table=self._j_table_writer.getTable())
        except Exception as e:
            raise DHError(e, "failed to create a DynamicTableWriter.") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_table_writer

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self) -> None:
        """Closes the writer.

        Raises:
            DHError
        """
        try:
            self._j_table_writer.close()
        except Exception as e:
            raise DHError(e, "failed to close the writer.") from e

    def write_row(self, *values: Any) -> None:
        """Writes a row to the newly created table.

        The type of a value must be convertible (safely or unsafely, e.g. lose precision, overflow, etc.) to the type
        of the corresponding column.

        Args:
            *values (Any): the values of the new row, the data types of these values must match the column definitions
                of the table

        Raises:
            DHError
        """
        try:
            values = to_sequence(values)
            self._j_table_writer.logRowPermissive(values)
        except Exception as e:
            raise DHError(e, "failed to write a row.") from e


class InputTable(Table):
    """InputTable is a subclass of Table that allows the users to dynamically add/delete/modify data in it.

    Users should always create InputTables through factory methods rather than directly from the constructor.
    """
    j_object_type = _JBaseArrayBackedInputTable

    def __init__(self, j_table: jpy.JType):
        super().__init__(j_table)
        self.j_input_table = self.j_table.getAttribute(_J_INPUT_TABLE_ATTRIBUTE)
        if not self.j_input_table:
            raise DHError("the provided table input is not suitable for input tables.")

    def add(self, table: Table) -> None:
        """Synchronously writes rows from the provided table to this input table. If this is a keyed input table, added rows with keys
        that match existing rows will replace those rows.

        Args:
            table (Table): the table that provides the rows to write

        Raises:
            DHError
        """
        try:
            self.j_input_table.add(table.j_table)
        except Exception as e:
            raise DHError(e, "add to InputTable failed.") from e

    def delete(self, table: Table) -> None:
        """Synchronously  deletes the keys contained in the provided table from this keyed input table. If this method is called on an
        append-only input table, an error will be raised.

        Args:
            table (Table): the table with the keys to delete

        Raises:
            DHError
        """
        try:
            self.j_input_table.delete(table.j_table)
        except Exception as e:
            raise DHError(e, "delete data in the InputTable failed.") from e

    @property
    def key_names(self) -> List[str]:
        """The names of the key columns of the InputTable."""
        return j_list_to_list(self.j_input_table.getKeyNames())

    @property
    def value_names(self) -> List[str]:
        """The names of the value columns. By default, any column not marked as a key column is a value column."""
        return j_list_to_list(self.j_input_table.getValueNames())


def input_table(col_defs: Dict[str, DType] = None, init_table: Table = None,
                key_cols: Union[str, Sequence[str]] = None) -> InputTable:
    """Creates an in-memory InputTable from either column definitions or an initial table. When key columns are
    provided, the InputTable will be keyed, otherwise it will be append-only.

    There are two types of in-memory InputTable - append-only and keyed.

    The append-only input table is not keyed, all rows are added to the end of the table, and deletions and edits are
    not permitted.

    The keyed input table has keys for each row and supports addition/deletion/modification of rows by the keys.

    Args:
        col_defs (Dict[str, DType]): the column definitions
        init_table (Table): the initial table
        key_cols (Union[str, Sequence[str]): the name(s) of the key column(s)

    Returns:
        an InputTable

    Raises:
        DHError
    """

    try:
        if col_defs is None and init_table is None:
            raise ValueError("either column definitions or init table should be provided.")
        elif col_defs and init_table:
            raise ValueError("both column definitions and init table are provided.")

        if col_defs:
            j_arg_1 = _JTableDefinition.of(
                [Column(name=n, data_type=t).j_column_definition for n, t in col_defs.items()])
        else:
            j_arg_1 = init_table.j_table

        key_cols = to_sequence(key_cols)
        if key_cols:
            j_table = _JKeyedArrayBackedInputTable.make(j_arg_1, key_cols)
        else:
            j_table = _JAppendOnlyArrayBackedInputTable.make(j_arg_1)
    except Exception as e:
        raise DHError(e, "failed to create an in-memory InputTable.") from e

    return InputTable(j_table)


def ring_table(parent: Table, capacity: int, initialize: bool = True) -> Table:
    """Creates a ring table that retains the latest 'capacity' number of rows from the parent table.
    Latest rows are determined solely by the new rows added to the parent table, deleted rows are ignored,
    and updated rows are not expected and will raise an exception.

    Ring table is mostly used with blink tables which do not retain their own data for more than an update cycle.

    Args:
        parent (Table): the parent table
        capacity (int): the capacity of the ring table
        initialize (bool): whether to initialize the ring table with a snapshot of the parent table, default is True

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JRingTableTools.of(parent.j_table, capacity, initialize))
    except Exception as e:
        raise DHError(e, "failed to create a ring table.") from e


def function_generated_table(table_generator: Callable[..., Table],
           source_tables: Union[Table, List[Table]] = None,
           refresh_interval_ms: int = None,
           exec_ctx: ExecutionContext = None,
           args: Tuple = (),
           kwargs: Dict = {}) -> Table:
    """Creates an abstract table that is generated by running the table_generator() function. The function will first be
    run to generate the table when this method is called, then subsequently either (a) whenever one of the
    'source_tables' ticks or (b) after refresh_interval_ms have elapsed. Either 'refresh_interval_ms' or
    'source_tables' must be set (but not both).

    Function-generated tables can be used to produce dynamic tables from sources outside Deephaven. For example,
    function-generated tables can create tables that are produced by arbitrary Python logic (including using Pandas or
    numpy). They can also be used to retrieve data from external sources (such as files or websites).

    The table definition must not change between invocations of the 'table_generator' function, or an exception will be raised.

    Note that the 'table_generator' may access data in the sourceTables but should not perform further table operations
    on them without careful handling. Table operations may be memoized, and it is possible that a table operation will
    return a table created by a previous invocation of the same operation. Since that result will not have been included
    in the 'source_table', it's not automatically treated as a dependency for purposes of determining when it's safe to
    invoke 'table_generator', allowing races to exist between accessing the operation result and that result's own update
    processing. It's best to include all dependencies directly in 'source_table', or only compute on-demand inputs under
    a LivenessScope.

    Args:
        table_generator (Callable[..., Table]): The table generator function. This function must return a Table.
        source_tables (Union[Table, List[Table]]): Source tables used by the 'table_generator' function. The
            'table_generator' is rerun when any of these tables tick.
        refresh_interval_ms (int): Interval (in milliseconds) at which the 'table_generator' function is rerun.
        exec_ctx (ExecutionContext): A custom execution context. If 'None', the current
            execution context is used. If there is no current execution context, a ValueError is raised.
        args (Tuple): Optional tuple of positional arguments to pass to table_generator. Defaults to ()
        kwargs (Dict): Optional dictionary of keyword arguments to pass to table_generator. Defaults to {}

    Returns:
        a new table

    Raises:
        DHError
    """
    if refresh_interval_ms is None and source_tables is None:
        raise DHError("Either refresh_interval_ms or source_tables must be provided!")

    if refresh_interval_ms is not None and source_tables is not None:
        raise DHError("Only one of refresh_interval_ms and source_tables must be provided!")

    # If no execution context is provided, assume we want to use the current one.
    if exec_ctx is None:
        exec_ctx = execution_context.get_exec_ctx()
        if exec_ctx is None:
            raise ValueError("No execution context is available and exec_ctx was not provided! ")

    def table_generator_function():
        with exec_ctx:
            result = table_generator(*args, **kwargs)

        if result is None:
            raise DHError("table_generator did not return a result")

        if isinstance(result, Table) is False:
            raise DHError("table_generator did not return a Table")

        return result.j_table

    # Treat the table_generator_function as a Java lambda (i.e., wrap it in a Java Supplier):
    table_generator_j_function = j_lambda(table_generator_function, _JSupplier)

    if refresh_interval_ms is not None:
        j_function_generated_table = _JFunctionGeneratedTableFactory.create(
            table_generator_j_function,
            refresh_interval_ms
        )
    else:
        # Make sure we have a list of Tables
        if isinstance(source_tables, Table):
            source_tables = [source_tables]

        # Extract the underlying Java tables of any source_tables:
        source_j_tables = []
        for tbl in source_tables:
            source_j_tables.append(tbl.j_table)

        # Create the function-generated table:
        with auto_locking_ctx(*source_tables):
            j_function_generated_table = _JFunctionGeneratedTableFactory.create(
                table_generator_j_function,
                source_j_tables
        )

    return Table(j_function_generated_table)
