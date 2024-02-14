#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""The table_publisher module supports publishing Deephaven Tables into blink Tables."""

import jpy

from typing import Callable, Dict, Optional, Tuple

from deephaven._wrapper import JObjectWrapper
from deephaven.column import Column
from deephaven.dtypes import DType
from deephaven.execution_context import get_exec_ctx
from deephaven.jcompat import j_lambda, j_runnable
from deephaven.table import Table
from deephaven.update_graph import UpdateGraph

_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")
_JTablePublisher = jpy.get_type("io.deephaven.stream.TablePublisher")
_JRuntimeException = jpy.get_type("java.lang.RuntimeException")
_JConsumer = jpy.get_type("java.util.function.Consumer")


class TablePublisher(JObjectWrapper):
    """The interface for publishing table data into a blink table."""

    j_object_type = _JTablePublisher

    def __init__(self, j_table_publisher: jpy.JType):
        self.j_table_publisher = j_table_publisher

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table_publisher

    @property
    def is_alive(self) -> bool:
        """Checks whether this is alive; if False, the caller should stop adding new data and release any related
        resources as soon as practicable since adding new data won't have any downstream effects.

        Once this is False, it will always remain False. For more prompt notifications, callers may prefer to use
        on_shutdown_callback during construction.

        Returns:
            if this publisher is alive
        """
        return self.j_table_publisher.isAlive()

    def add(self, table: Table) -> None:
        """Adds a snapshot of the data from table into the blink table. The table must contain a superset of the columns
        from the blink table's definition; the columns may be in any order. Columns from table that are not in the blink
        table's definition are ignored.

        All of the data from table will be:

        1. consistent with a point in time
        2. fully contained in a single blink table's update cycle
        3. non-interleaved with any other calls to add (concurrent, or not)

        Args:
            table (Table): the table to add
        """
        self.j_table_publisher.add(table.j_table)

    def publish_failure(self, failure: Exception) -> None:
        """Indicate that data publication has failed. Blink table listeners will be notified of the failure, the
        on-shutdown callback will be invoked if it hasn't already been, this publisher will no longer be alive, and
        future calls to add will silently return without publishing. These effects may resolve asynchronously.

        Args:
            failure (Exception): the failure
        """
        self.j_table_publisher.publishFailure(_JRuntimeException(str(failure)))


def table_publisher(
    name: str,
    col_defs: Dict[str, DType],
    on_flush_callback: Optional[Callable[[TablePublisher], None]] = None,
    on_shutdown_callback: Optional[Callable[[], None]] = None,
    update_graph: Optional[UpdateGraph] = None,
    chunk_size: int = 2048,
) -> Tuple[Table, TablePublisher]:
    """Constructs a blink Table and TablePublisher to populate it.

    Args:
        name (str): the name, used for logging
        col_defs (Dict[str, DType]): the column definitions for the resulting blink table
        on_flush_callback (Optional[Callable[[TablePublisher], None]]): the on-flush callback, if present, is called
            once at the beginning of each update graph cycle. This is a pattern that allows publishers to add any data
            they may have been batching. Do note though, this blocks the update cycle from proceeding, so
            implementations should take care to not do extraneous work.
        on_shutdown_callback (Optional[Callable[[], None]]): the on-shutdown callback, if present, is called one time
            when the caller should stop adding new data and release any related resources as soon as practicable since
            adding data won't have any downstream effects
        update_graph (Optional[UpdateGraph]): the update graph the resulting table will belong to. If unset, the update
            graph of the current execution context will be used.
        chunk_size (int): the chunk size, the size at which chunks will be filled from the source table during an add,
            defaults to 2048

    Returns:
        a two-tuple, where the first item is resulting blink Table and the second item is the TablePublisher
    """

    def adapt_callback(_table_publisher: jpy.JType):
        on_flush_callback(TablePublisher(j_table_publisher=_table_publisher))

    j_table_publisher = _JTablePublisher.of(
        name,
        _JTableDefinition.of(
            [
                Column(name=name, data_type=dtype).j_column_definition
                for name, dtype in col_defs.items()
            ]
        ),
        j_lambda(adapt_callback, _JConsumer, None) if on_flush_callback else None,
        j_runnable(on_shutdown_callback) if on_shutdown_callback else None,
        (update_graph or get_exec_ctx().update_graph).j_update_graph,
        chunk_size,
    )
    # Note: this differs from the java version where the end user is given access to TablePublisher.table().
    # In a theoretical world where java allows more ergonomic creation of tuples / tuple-typing, it would look more like
    # this python version.
    return (
        Table(j_table=j_table_publisher.table()),
        TablePublisher(j_table_publisher=j_table_publisher),
    )
