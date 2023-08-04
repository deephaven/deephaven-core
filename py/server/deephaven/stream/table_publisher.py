#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""The table_publisher module supports publishing Deephaven Tables into blink Tables."""

import jpy
import traceback

from typing import Callable, Dict, Optional

from deephaven._wrapper import JObjectWrapper
from deephaven.column import Column
from deephaven.dtypes import DType
from deephaven.execution_context import get_exec_ctx
from deephaven.jcompat import j_runnable
from deephaven.table import Table
from deephaven.update_graph import UpdateGraph

_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")
_JTablePublisher = jpy.get_type("io.deephaven.stream.TablePublisher")
_JRuntimeException = jpy.get_type("java.lang.RuntimeException")


class TablePublisher(JObjectWrapper):
    """Supports publishing Table data."""

    j_object_type = _JTablePublisher

    def __init__(self, j_table_publisher: jpy.JType):
        self.j_table_publisher = j_table_publisher

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table_publisher

    @property
    def is_alive(self) -> bool:
        """Checks whether this publisher is alive; if False, the publisher should stop publishing new data and release
        any related resources as soon as practicable since publishing won't have any downstream effects.

        Once this is False, it will always remain False. For more prompt notifications, publishers may
        prefer to use on_shutdown_callback during construction.

        Returns:
            if this publisher is alive
        """
        return self.j_table_publisher.isAlive()

    def add(self, table: Table):
        """Adds a snapshot of the data from table into the blink table according to the blink table's definition.

        All of the data from table will be:

        1. consistent with a point in time
        2. fully contained in a single blink table's update cycle
        3. non-interleaved with any other calls to add (concurrent, or not)

        Args:
            table (Table): the table to add
        """
        self.j_table_publisher.add(table.j_table)

    def publish_failure(self, failure: Exception):
        """Publish a failure for notification to the listeners of the blink table. Future calls to add will silently
        return. Will cause the on-shutdown callback to be invoked if it hasn't already been invoked.

        Args:
            failure (Exception): the failure
        """
        self.j_table_publisher.publishFailure(_JRuntimeException(str(failure)))


# Note: we may want to expand the column_definitions typing here and elsewhere in the future to also accept List[Column]


def table_publisher(
    name: str,
    col_defs: Dict[str, DType],
    on_shutdown_callback: Optional[Callable[[], None]] = None,
    update_graph: Optional[UpdateGraph] = None,
    chunk_size: int = 2048,
) -> tuple[Table, TablePublisher]:
    """Constructs a blink Table and TablePublisher.

    The on_shutdown_callback, if present, is called one time when the publisher should stop publishing new data
    and release any related resources as soon as practicable since publishing won't have any downstream effects.

    The update_graph is the update graph that the resulting table will belong to. If unset, the update graph of
    the current execution context will be used.

    The chunk_size is the size at which chunks will be filled from the source table during an add.

    The caller is responsible for keeping a reference to the resulting blink (or derived) Table.

    Args:
        name (str): the name, used for logging
        col_defs (Dict[str, DType]): the column definitions
        on_shutdown_callback (Optional[Callable[[], None]]): the on-shutdown callback
        update_graph (Optional[UpdateGraph]): the update graph
        chunk_size (int): the chunk size, defaults to 2048

    Returns:
        a two-tuple, where the first item is resulting blink Table and the second item is the TablePublisher
    """
    j_table_publisher = _JTablePublisher.of(
        name,
        _JTableDefinition.of(
            [
                Column(name=name, data_type=dtype).j_column_definition
                for name, dtype in col_defs.items()
            ]
        ),
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
