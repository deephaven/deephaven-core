#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
"""This module defines a table service backend interface that users can implement to provide external data to Deephaven
tables."""

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

class TableKey(ABC):
    """A key that identifies a table. The key should be unique for each table. The key can be any Python object and
    should include sufficient information to uniquely identify the table for the backend service."""

    def __init__(self, key: Any):
        self._key = key

    @abstractmethod
    def __hash__(self):
        pass

    @property
    def key(self) -> Any:
        """The user defined key that identifies the table."""
        return self._key


class TableLocationKey(ABC):
    """A key that identifies a specific location of a table. The key should be unique for each table location of the
    table. The key can be any Python object and should include sufficient information to uniquely identify the location
    for the backend service to fetch the data values and data size.
    """

    def __init__(self, location_key: Any):
        self._location_key = location_key

    @abstractmethod
    def __hash__(self):
        pass

    @property
    def key(self) -> Any:
        """The user defined key that identifies the table location."""
        return self._location_key



class TableDataServiceBackend(ABC):
    """An interface for a backend service that provides access to table data."""

    @abstractmethod
    def table_schema(self, table_key: TableKey) -> Tuple[pa.Schema, Optional[pa.Schema]]:
        """ Returns the table data schema and optionally the partitioning column schema for the table with the given
        table key.
        The table data schema is not required to include the partitioning columns defined in the partitioning schema.

        Args:
            table_key (TableKey): the table key

        Returns:
            Tuple[pa.Schema, Optional[pa.Schema]]: a tuple of the table schema and the optional schema for the
                partitioning columns
        """
        pass

    @abstractmethod
    def table_locations(self, table_key: TableKey,
                        callback: Callable[[TableLocationKey, Optional[pa.Table]], None]) -> None:
        """ Provides a callback for the backend service to pass the existing locations for the table with the given
        table key. The 2nd argument of the callback is an optional pa.Table that contains the partition values for the
        location. The schema of the table should be compatible with the optional partitioning column schema returned by
        table_schema() for the table_key. The table should have a single row for the particular location key provided in
        the 1st argument, with the partitioning values for each partitioning column in the row.

        This is called for tables created when ：meth:`PythonTableDataService.make_table` is called with live=False

        Args:
            table_key (TableKey): the table key
            callback (Callable[[TableLocationKey, Optional[pa.Table]], None]): the callback function
        """
        pass

    @abstractmethod
    def subscribe_to_table_locations(self, table_key: TableKey,
                                     callback: Callable[[TableLocationKey, Optional[pa.Table]], None]) -> \
            Callable[[], None]:
        """ Provides a callback for the backend service to pass table locations for the table with the given table key.
        The 2nd argument of the callback is a pa.Table that contains the partitioning values. The schema of the
        table should match the optional partitioning column schema returned by table_schema() for the table_key. The
        table should have a single row for the particular partition location key provided in the 1st argument, with the
        values for partitioning values for each partitioning column in the row.

        This is called for tables created when ：meth:`PythonTableDataService.make_table` is called with live=True.

        Any existing table locations should be provided to the callback prior to returning from this method.

        Note that any asynchronous calls to the callback will block until this method has returned.

        Args:
            table_key (TableKey): the table key
            callback (Callable[[TableLocationKey, Optional[pa.Table]], None]): the callback function

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
        """
        pass

    @abstractmethod
    def table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey,
                       callback: Callable[[int], None]) -> None:
        """ Provides a callback for the backend service to pass the size of the table location with the given table key
        and table location key. The callback should be called with the size of the table location in number of rows.

        This is called for tables created when ：meth:`PythonTableDataService.make_table` is called with live=False.

        The existing table location size should be provided to the callback prior to returning from this method.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            callback (Callable[[int], None]): the callback function
        """
        pass

    @abstractmethod
    def subscribe_to_table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey,
                                        callback: Callable[[int], None]) -> Callable[[], None]:
        """ Provides a callback for the backend service to pass existing, and any future, size of the table location
        with the given table key and table location key. The callback should be called with the size of the table
        location in number of rows.

        This is called for tables created when ：meth:`PythonTableDataService.make_table` is called with live=True

        Note that any asynchronous calls to the callback will block until this method has returned.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the partition location key
            callback (Callable[[int], None]): the callback function

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
        """
        pass

    @abstractmethod
    def column_values(self, table_key: TableKey, table_location_key: TableLocationKey, col: str, offset: int,
                      min_rows: int, max_rows: int) -> pa.Table:
        """ Returns the values for the column with the given name for the table location with the given table key and
        table location key. The returned pa.Table should have a single column with values within the specified range
        requirement.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            col (str): the column name
            offset (int): the starting row index
            min_rows (int): the minimum number of rows to return
            max_rows (int): the maximum number of rows to return

        Returns:
            pa.Table: a pa.Table that contains the data values for the column within the specified range
        """
        pass


class TableDataService(JObjectWrapper):
    """ A TableDataService serves as a bridge between the Deephaven data service and the Python data service backend.
    It supports the creation of Deephaven tables from the Python backend service that provides table data and table
    data locations to the Deephaven tables.
    """
    j_object_type = _JPythonTableDataService
    _backend: TableDataServiceBackend

    def __init__(self, backend: TableDataServiceBackend, *, chunk_reader_factory: Optional[jpy.JType] = None,
                 stream_reader_options: Optional[jpy.JType] = None, page_size: int = 0):
        """ Creates a new PythonTableDataService with the given user-implemented backend service.

        Args:
            backend (TableDataServiceBackend): the user-implemented backend service implementation
        """
        self._backend = backend
        self._j_tbl_service = _JPythonTableDataService.create(
            self, chunk_reader_factory, stream_reader_options, page_size)

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

    def _table_schema(self, table_key: TableKey, callback: jpy.JType) -> jpy.JType:
        """ Returns the table data schema and the partitioning values schema for the table with the given table key as
        two serialized byte buffers.

        Args:
            table_key (TableKey): the table key
            callback (jpy.JType): the Java callback function with one argument: an array of byte buffers that contain
                the serialized table data arrow and partitioning values schemas

        Returns:
            jpy.JType: an array of two serialized byte buffers
        """
        schemas = self._backend.table_schema(table_key)
        dt_schema = schemas[0]
        pc_schema = schemas[1] if len(schemas) > 1 else None
        pc_schema = pc_schema if pc_schema is not None else pa.schema([])
        j_dt_schema_bb = jpy.byte_buffer(dt_schema.serialize())
        j_pc_schema_bb = jpy.byte_buffer(pc_schema.serialize())
        # note that the java callback expects the partitioning schema first
        callback.accept(jpy.array("java.nio.ByteBuffer", [j_pc_schema_bb, j_dt_schema_bb]))

    def _table_locations(self, table_key: TableKey, callback: jpy.JType) -> None:
        """ Provides the existing table locations for the table with the given table key to the table service in the
        engine.

        Args:
            table_key (TableKey): the table key
            callback (jpy.JType): the Java callback function with two arguments: a table location key and an array of
                byte buffers that contain the serialized arrow schema and a record batch of the partitioning values
        """
        def callback_proxy(pt_location_key, pt_table):
            j_tbl_location_key = _JTableLocationKeyImpl(pt_location_key)
            if pt_table is None or pt_table.to_batches() is None:
                callback.apply(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", []))
            else:
                if pt_table.num_rows != 1:
                    raise ValueError("The number of rows in the pyarrow table for partitioning values must be 1")
                bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
                bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
                callback.accept(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", bb_list))

        self._backend.table_locations(table_key, callback_proxy)

    def _subscribe_to_table_locations(self, table_key: TableKey, callback: jpy.JType) -> Callable[[], None]:
        """ Provides the table locations, existing and new, for the table with the given table key to the table service
        in the engine.

        Args:
            table_key (TableKey): the table key
            callback (jpy.JType): the Java callback function with two arguments: a table location key of the new
                location and an array of byte buffers that contain the partitioning arrow schema and the serialized
                record batches of the partitioning values

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
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

        return self._backend.subscribe_to_table_locations(table_key, callback_proxy)

    def _table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey, callback: jpy.JType):
        """ Provides the size of the table location with the given table key and table location key to the table service
        in the engine.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            callback (jpy.JType): the Java callback function with one argument: the size of the table location in number
                of rows
        """
        def callback_proxy(size):
            callback.accept(size)

        self._backend.table_location_size(table_key, table_location_key, callback_proxy)

    def _subscribe_to_table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey,
                                             callback: jpy.JType) -> Callable[[], None]:
        """ Provides the current and future sizes of the table location with the given table key and table location key
        to the table service in the engine.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            callback (jpy.JType): the Java callback function with one argument: the size of the partition in number of
                rows

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
        """
        def callback_proxy(size):
            callback.accept(size)

        return self._backend.subscribe_to_table_location_size(table_key, table_location_key, callback_proxy)

    def _column_values(self, table_key: TableKey, table_location_key: TableLocationKey, col: str, offset: int,
                       min_rows: int, max_rows: int, callback: jpy.JType) -> None:
        """ Provides the data values for the column with the given name for the table location with the given table key
        and table location key to the table service in the engine.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            col (str): the column name
            offset (int): the starting row index
            min_rows (int): the minimum number of rows to return
            max_rows (int): the maximum number of rows to return
            callback (jpy.JType): the Java callback function with one argument: an array of byte buffers that contain
                the arrow schema and the serialized record batches for the given column
        """
        pt_table = self._backend.column_values(table_key, table_location_key, col, offset, min_rows, max_rows)
        if len(pt_table) < min_rows or len(pt_table) > max_rows:
            raise ValueError("The number of rows in the pyarrow table for column values must be in the range of "
                             f"{min_rows} to {max_rows}")
        bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
        bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
        callback.accept(jpy.array("java.nio.ByteBuffer", bb_list))

