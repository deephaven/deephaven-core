#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

from abc import ABC, abstractmethod
from typing import Tuple, Optional, Any, Callable

import jpy

import pyarrow as pa

from deephaven.dherror import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.table import Table

_JPythonTableDataService = jpy.get_type("io.deephaven.extensions.barrage.util.PythonTableDataService")
_JTableKeyImpl = jpy.get_type("io.deephaven.extensions.barrage.util.PythonTableDataService$TableKeyImpl")
_JTableLocationKeyImpl = jpy.get_type("io.deephaven.extensions.barrage.util.PythonTableDataService$TableLocationKeyImpl")


class TableKey:
    """A key that identifies a table. The key should be unique for each table. The key can be any Python object and
    should include sufficient information to uniquely identify the table for the backend service."""

    def __init__(self, key: Any):
        self._key = key

    @property
    def key(self) -> Any:
        """The user defined key that identifies the table."""
        return self._key


class PartitionedTableLocationKey:
    """A key that identifies a specific partition of a table. The key should be unique for each partition of the table.
    The key can be any Python object and should include sufficient information to uniquely identify the partition for
    the backend service to fetch the partition data.
    """

    def __init__(self, pt_location_key: Any):
        self._pt_location_key = pt_location_key

    @property
    def pt_location_key(self) -> Any:
        return self._pt_location_key



class PartitionedTableServiceBackend(ABC):
    """An interface for a backend service that provides access to partitioned data."""

    @abstractmethod
    def table_schema(self, table_key: TableKey) -> Tuple[pa.Schema, Optional[pa.Schema]]:
        """ Returns the table schema and optionally the schema for the partition columns for the table with the given
        table key.
        The table schema is not required to include the partition columns defined in the partition schema. THe
        partition columns are limited to primitive types and strings.

        Args:
            table_key (TableKey): the table key

        Returns:
            Tuple[pa.Schema, Optional[pa.Schema]]: a tuple of the table schema and the optional schema for the partition
                columns
        """
        pass

    @abstractmethod
    def existing_partitions(self, table_key: TableKey,
                            callback: Callable[[PartitionedTableLocationKey, Optional[pa.Table]], None]) -> None:
        """ Provides a callback for the backend service to pass the existing partitions for the table with the given
        table key. The 2nd argument of the callback is an optional pa.Table that contains the values for the partitions.
        The schema of the table should match the optional partition schema returned by table_schema() for the table_key.
        The table should have a single row for the particular partition location key provided in the 1st argument,
        with the values for the partition columns in the row.

        Args:
            table_key (TableKey): the table key
            callback (Callable[[PartitionedTableLocationKey, Optional[pa.Table]], None]): the callback function
        """
        pass

    @abstractmethod
    def subscribe_to_new_partitions(self, table_key: TableKey,
                                      callback: Callable[[PartitionedTableLocationKey, Optional[pa.Table]], None]) -> \
    Callable[[], None]:
        """ Provides a callback for the backend service to pass new partitions for the table with the given table key.
        The 2nd argument of the callback is a pa.Table that contains the values for the partitions. The schema of the
        table should match the optional partition schema returned by table_schema() for the table_key. The table should
        have a single row for the particular partition location key provided in the 1st argument, with the values for
        the partition columns in the row.

        The return value is a function that can be called to unsubscribe from the new partitions.

        Args:
            table_key (TableKey): the table key
            callback (Callable[[PartitionedTableLocationKey, Optional[pa.Table]], None]): the callback function
        """
        pass

    @abstractmethod
    def partition_size(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                       callback: Callable[[int], None]) -> None:
        """ Provides a callback for the backend service to pass the size of the partition with the given table key
        and partition location key. The callback should be called with the size of the partition in number of rows.

        Args:
            table_key (TableKey): the table key
            table_location_key (PartitionedTableLocationKey): the partition location key
            callback (Callable[[int], None]): the callback function
        """
        pass

    @abstractmethod
    def subscribe_to_partition_size_changes(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                                            callback: Callable[[int], None]) -> Callable[[], None]:
        """ Provides a callback for the backend service to pass the changed size of the partition with the given
        table key and partition location key. The callback should be called with the size of the partition in number of
        rows.

        The return value is a function that can be called to unsubscribe from the partition size changes.

        Args:
            table_key (TableKey): the table key
            table_location_key (PartitionedTableLocationKey): the partition location key
            callback (Callable[[int], None]): the callback function

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from the partition size changes
        """
        pass

    @abstractmethod
    def column_values(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey, col: str, offset: int,
                      min_rows: int, max_rows: int) -> pa.Table:
        """ Returns the values for the column with the given name for the partition with the given table key and
        partition location key. The returned pa.Table should have a single column with values of the specified range
        requirement for the given column.

        Args:
            table_key (TableKey): the table key
            table_location_key (PartitionedTableLocationKey): the partition location key
            col (str): the column name
            offset (int): the starting row index
            min_rows (int): the minimum number of rows to return
            max_rows (int): the maximum number of rows to return

        Returns:
            pa.Table: a pa.Table that contains the values for the column
        """
        pass


class PythonTableDataService(JObjectWrapper):
    """ A Python wrapper for the Java PythonTableDataService class. It also serves as an adapter between the Java backend
    interface and the Python backend interface.
    """
    j_object_type = _JPythonTableDataService
    _backend: PartitionedTableServiceBackend

    def __init__(self, backend: PartitionedTableServiceBackend):
        """ Creates a new PythonTableDataService with the given user-implemented backend service.

        Args:
            backend (PartitionedTableServiceBackend): the user-implemented backend service implementation
        """
        self._backend = backend
        self._j_tbl_service = _JPythonTableDataService.create(self)

    @property
    def j_object(self):
        return self._j_tbl_service

    def make_table(self, table_key: TableKey, *, live: bool) -> Table:
        """ Creates a Table backed by the backend service with the given table key.

        Args:
            table_key (TableKey): the table key
            live (bool): whether the table is live or static

        Returns:
            Table: a new table

        Raises:
            DHError
        """
        j_table_key = _JTableKeyImpl(table_key)
        try:
            return Table(self._j_tbl_service.makeTable(j_table_key, live))
        except Exception as e:
            raise DHError(e, message=f"failed to make a table for the key {table_key.key}") from e

    def _table_schema(self, table_key: TableKey) -> jpy.JType:
        """ Returns the table schema and the partition schema for the table with the given table key as two serialized
        byte buffers.

        Args:
            table_key (TableKey): the table key

        Returns:
            jpy.JType: an array of two serialized byte buffers
        """
        schemas = self._backend.table_schema(table_key)
        pt_schema = schemas[0]
        pc_schema = schemas[1] if len(schemas) > 1 else None
        pc_schema = pc_schema if pc_schema is not None else pa.schema([])
        j_pt_schema_bb = jpy.byte_buffer(pt_schema.serialize())
        j_pc_schema_bb = jpy.byte_buffer(pc_schema.serialize())
        return jpy.array("java.nio.ByteBuffer", [j_pt_schema_bb, j_pc_schema_bb])

    def _existing_partitions(self, table_key: TableKey, callback: jpy.JType) -> None:
        """ Provides the existing partitions for the table with the given table key to the table service in the engine.

        Args:
            table_key (TableKey): the table key
            callback (jpy.JType): the Java callback function with two arguments: a table location key and an array of
                byte buffers that contain the arrow schema and serialized record batches for the partition columns
        """
        def callback_proxy(pt_location_key, pt_table):
            j_tbl_location_key = _JTableLocationKeyImpl(pt_location_key)
            if pt_table is None or pt_table.to_batches() is None:
                callback.apply(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", []))
            else:
                if pt_table.num_rows != 1:
                    raise ValueError("The number of rows in the pyarrow table for partition column values must be 1")
                bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
                bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
                callback.accept(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", bb_list))

        self._backend.existing_partitions(table_key, callback_proxy)

    def _subscribe_to_new_partitions(self, table_key: TableKey, callback: jpy.JType) -> jpy.JType:
        """ Provides the new partitions for the table with the given table key to the table service in the engine.

        Args:
            table_key (TableKey): the table key
            callback (jpy.JType): the Java callback function with two arguments: a table location key of the new
                partition and an array of byte buffers that contain the arrow schema and the serialized record batches
                for the partition column values
        """
        def callback_proxy(pt_location_key, pt_table):
            j_tbl_location_key = _JTableLocationKeyImpl(pt_location_key)
            if pt_table is None:
                callback.apply(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", []))
            else:
                if pt_table.num_rows != 1:
                    raise ValueError("The number of rows in the pyarrow table for partition column values must be 1")
                bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
                bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
                callback.accept(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", bb_list))

        return self._backend.subscribe_to_new_partitions(table_key, callback_proxy)

    def _partition_size(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey, callback: jpy.JType):
        """ Provides the size of the partition with the given table key and partition location key to the table service
        in the engine.

        Args:
            table_key (TableKey): the table key
            table_location_key (PartitionedTableLocationKey): the partition location key
            callback (jpy.JType): the Java callback function with one argument: the size of the partition in number of
                rows
        """
        def callback_proxy(size):
            callback.accept(size)

        self._backend.partition_size(table_key, table_location_key, callback_proxy)

    def _subscribe_to_partition_size_changes(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey,
                                             callback: jpy.JType) -> jpy.JType:
        """ Provides the changed size of the partition with the given table key and partition location key to the table
        service in the engine.

        Args:
            table_key (TableKey): the table key
            table_location_key (PartitionedTableLocationKey): the partition location key
            callback (jpy.JType): the Java callback function with one argument: the size of the partition in number of
                rows
        """
        def callback_proxy(size):
            callback.accept(size)

        return self._backend.subscribe_to_partition_size_changes(table_key, table_location_key, callback_proxy)

    def _column_values(self, table_key: TableKey, table_location_key: PartitionedTableLocationKey, col: str, offset: int,
                       min_rows: int, max_rows: int, callback: jpy.JType) -> None:
        """ Returns the values for the column with the given name for the partition with the given table key and
        partition location key to the table service in the engine.

        Args:
            table_key (TableKey): the table key
            table_location_key (PartitionedTableLocationKey): the partition location key
            col (str): the column name
            offset (int): the starting row index
            min_rows (int): the minimum number of rows to return
            max_rows (int): the maximum number of rows to return
            callback (jpy.JType): the Java callback function with one argument: an array of byte buffers that contain
                the arrow schema and the serialized record batches for the given column

        Returns:
            jpy.JType: an array of byte buffers that contain the arrow schema and the serialized record batches for the
                partition column values
        """
        pt_table = self._backend.column_values(table_key, table_location_key, col, offset, min_rows, max_rows)
        bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
        bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
        callback.accept(jpy.array("java.nio.ByteBuffer", bb_list))

