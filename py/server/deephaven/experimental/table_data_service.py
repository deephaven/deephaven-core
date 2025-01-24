#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
"""This module defines a table service backend interface TableDataServiceBackend that users can implement to provide
external data in the format of  pyarrow Table to Deephaven tables. The backend service implementation should be passed
to the TableDataService constructor to create a new TableDataService instance. The TableDataService instance can then
be used to create Deephaven tables backed by the backend service."""
import traceback
from abc import ABC, abstractmethod
from typing import Optional, Callable

import jpy

import pyarrow as pa

from deephaven.dherror import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.table import Table, PartitionedTable

_JPythonTableDataService = jpy.get_type("io.deephaven.extensions.barrage.util.PythonTableDataService")
_JTableKeyImpl = jpy.get_type("io.deephaven.extensions.barrage.util.PythonTableDataService$TableKeyImpl")
_JTableLocationKeyImpl = jpy.get_type("io.deephaven.extensions.barrage.util.PythonTableDataService$TableLocationKeyImpl")


class TableKey(ABC):
    """A key that identifies a table. The key should be unique for each table. The key can be any Python object and
    should include sufficient information to uniquely identify the table for the backend service. The __hash__ method
    must be implemented to ensure that the key is hashable.
    """

    @abstractmethod
    def __hash__(self):
        pass


class TableLocationKey(ABC):
    """A key that identifies a specific location of a table. The key should be unique for each table location of the
    table. The key can be any Python object and should include sufficient information to uniquely identify the location
    for the backend service to fetch the data values and data size. The __hash__ method must be implemented to ensure
    that the key is hashable.
    """

    @abstractmethod
    def __hash__(self):
        pass


class TableDataServiceBackend(ABC):
    """An interface for a backend service that provides access to table data."""

    @abstractmethod
    def table_schema(self, table_key: TableKey,
                     schema_cb: Callable[[pa.Schema, Optional[pa.Schema]], None],
                     failure_cb: Callable[[Exception], None]) -> None:
        """ Provides the table data schema and the partitioning column schema for the table with the given table key via
        the schema_cb callback. The table data schema is not required to include the partitioning columns defined in
        the partitioning column schema.

        The failure callback should be invoked when a failure to provide the schemas occurs.

        The table_schema caller will block until one of the schema or failure callbacks is called.

        Note that asynchronous calls to any callback may block until this method has returned.

        Args:
            table_key (TableKey): the table key
            schema_cb (Callable[[pa.Schema, Optional[pa.Schema]], None]): the callback function with two arguments: the
                table data schema and the optional partitioning column schema
            failure_cb (Callable[[Exception], None]): the failure callback function
        """
        pass

    @abstractmethod
    def table_locations(self, table_key: TableKey,
                        location_cb: Callable[[TableLocationKey, Optional[pa.Table]], None],
                        success_cb: Callable[[], None],
                        failure_cb: Callable[[Exception], None]) -> None:
        """ Provides the existing table locations for the table with the given table via the location_cb callback.

        The location callback should be called with the table location key and an optional pyarrow.Table that contains
        the partitioning values for the location. The schema of the table must match the optional partitioning column
        schema returned by :meth:`table_schema` for the table_key. The table must have a single row for the particular
        table location key provided in the 1st argument, with values for each partitioning column in the row.

        The success callback should be called when all existing table locations have been delivered to the table
        location callback.

        The failure callback should be invoked when failure to provide existing table locations occurs.

        The table_locations caller will block until one of the success or failure callbacks is called.

        This is called for tables created when :meth:`TableDataService.make_table` is called with refreshing=False

        Note that asynchronous calls to any callback may block until this method has returned.

        Args:
            table_key (TableKey): the table key
            location_cb (Callable[[TableLocationKey, Optional[pa.Table]], None]): the callback function
            success_cb (Callable[[], None]): the success callback function
            failure_cb (Callable[[Exception], None]): the failure callback function
        """
        pass

    @abstractmethod
    def subscribe_to_table_locations(self, table_key: TableKey,
                                     location_cb: Callable[[TableLocationKey, Optional[pa.Table]], None],
                                     success_cb: Callable[[], None],
                                     failure_cb: Callable[[Exception], None]) -> Callable[[], None]:
        """ Provides the table locations, existing and new, for the table with the given table key via the location_cb
        callback.

        The location callback should be called with the table location key and an optional pyarrow.Table that contains
        the partitioning values for the location. The schema of the table must match the optional partitioning column
        schema returned by :meth:`table_schema` for the table_key. The table must have a single row for the particular
        table location key provided in the 1st argument, with values for each partitioning column in the row.

        The success callback should be called when the subscription is established successfully and after all existing
        table locations have been delivered to the table location callback.

        The failure callback should be invoked at initial failure to establish a subscription, or on a permanent failure
        to keep the subscription active (e.g. failure with no reconnection possible, or failure to reconnect/resubscribe
        before a timeout).

        This is called for tables created when :meth:`TableDataService.make_table` is called with refreshing=True.

        Note that asynchronous calls to any callback will block until this method has returned.

        Args:
            table_key (TableKey): the table key
            location_cb (Callable[[TableLocationKey, Optional[pa.Table]], None]): the table location callback function
            success_cb (Callable[[], None]): the success callback function
            failure_cb (Callable[[Exception], None]): the failure callback function


        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
        """
        pass

    @abstractmethod
    def table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey,
                            size_cb: Callable[[int], None],
                            failure_cb: Callable[[Exception], None]) -> None:
        """ Provides the size of the table location with the given table key and table location key via the size_cb
        callback. The size is the number of rows in the table location.

        The failure callback should be invoked when a failure to provide the table location size occurs.

        The table_location_size caller will block until one of the size or failure callbacks is called.

        This is called for tables created when :meth:`TableDataService.make_table` is called with refreshing=False.

        Note that asynchronous calls to any callback may block until this method has returned.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            size_cb (Callable[[int], None]): the callback function
        """
        pass

    @abstractmethod
    def subscribe_to_table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey,
                                         size_cb: Callable[[int], None],
                                         success_cb: Callable[[], None],
                                         failure_cb: Callable[[Exception], None]) -> Callable[[], None]:
        """ Provides the current and future sizes of the table location with the given table key and table location
        key via the size_cb callback. The size is the number of rows in the table location.

        The success callback should be called when the subscription is established successfully and after the current
        table location size has been delivered to the size callback.

        The failure callback should be invoked at initial failure to establish a subscription, or on a permanent failure
        to keep the subscription active (e.g. failure with no reconnection possible, or failure to reconnect/resubscribe
        before a timeout).

        This is called for tables created when :meth:``TableDataService.make_table` is called with refreshing=True

        Note that asynchronous calls to any callback will block until this method has returned.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            size_cb (Callable[[int], None]): the table location size callback function
            success_cb (Callable[[], None]): the success callback function
            failure_cb (Callable[[Exception], None]): the failure callback function

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
        """
        pass

    @abstractmethod
    def column_values(self, table_key: TableKey, table_location_key: TableLocationKey, col: str, offset: int,
                      min_rows: int, max_rows: int,
                      values_cb: Callable[[pa.Table], None],
                      failure_cb: Callable[[Exception], None]) -> None:
        """ Provides the data values for the column with the given name for the table location with the given table key
        and table location key via the values_cb callback. The column values are provided as a pyarrow.Table that
        contains the data values for the column within the specified range requirement. The values_cb callback should be
        called with a single column pyarrow.Table that contains the data values for the given column within the
        specified range requirement.

        The failure callback should be invoked when a failure to provide the column values occurs.

        The column_values caller will block until one of the values or failure callbacks is called.

        Note that asynchronous calls to any callback may block until this method has returned.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            col (str): the column name
            offset (int): the starting row index
            min_rows (int): the minimum number of rows to return, min_rows is always <= page size
            max_rows (int): the maximum number of rows to return
            values_cb (Callable[[pa.Table], None]): the callback function with one argument: the pyarrow.Table that
                contains the data values for the column within the specified range
            failure_cb (Callable[[Exception], None]): the failure callback function
        """
        pass


class TableDataService(JObjectWrapper):
    """ A TableDataService serves as a wrapper around a tightly-coupled Deephaven TableDataService implementation
    (Java class PythonTableDataService) that delegates to a Python TableDataServiceBackend for TableKey creation,
    TableLocationKey discovery, and data subscription/retrieval operations. It supports the creation of Deephaven tables
    from the Python backend service that provides table data and table data locations to the Deephaven tables.
    """
    j_object_type = _JPythonTableDataService
    _backend: TableDataServiceBackend

    def __init__(self, backend: TableDataServiceBackend, *, chunk_reader_factory: Optional[jpy.JType] = None,
                 stream_reader_options: Optional[jpy.JType] = None, page_size: Optional[int] = None):
        """ Creates a new TableDataService with the given user-implemented backend service.

        Args:
            backend (TableDataServiceBackend): the user-implemented backend service implementation
            chunk_reader_factory (Optional[jpy.JType]): the Barrage chunk reader factory, default is None
            stream_reader_options (Optional[jpy.JType]): the Barrage stream reader options, default is None
            page_size (int): the page size for the table service, default is None, meaning to use the configurable
                jvm property: PythonTableDataService.defaultPageSize which defaults to 64K.
        """
        self._backend = backend

        if page_size is None:
            page_size = 0
        elif page_size < 0:
            raise ValueError("The page size must be non-negative")

        self._j_tbl_service = _JPythonTableDataService.create(
            self, chunk_reader_factory, stream_reader_options, page_size)

    @property
    def j_object(self):
        return self._j_tbl_service

    def make_table(self, table_key: TableKey, *, refreshing: bool) -> Table:
        """ Creates a Table backed by the backend service with the given table key.

        Args:
            table_key (TableKey): the table key
            refreshing (bool): whether the table is live or static

        Returns:
            Table: a new table

        Raises:
            DHError
        """
        j_table_key = _JTableKeyImpl(table_key)
        try:
            return Table(self._j_tbl_service.makeTable(j_table_key, refreshing))
        except Exception as e:
            raise DHError(e, message=f"failed to make a table for the key {table_key}") from e

    def make_partitioned_table(self, table_key: TableKey, *, refreshing: bool) -> PartitionedTable:
        """ Creates a PartitionedTable backed by the backend service with the given table key.

        Args:
            table_key (TableKey): the table key
            refreshing (bool): whether the partitioned table is live or static

        Returns:
            PartitionedTable: a new partitioned table

        Raises:
            DHError
        """
        j_table_key = _JTableKeyImpl(table_key)
        try:
            return PartitionedTable(self._j_tbl_service.makePartitionedTable(j_table_key, refreshing))
        except Exception as e:
            raise DHError(e, message=f"failed to make a partitioned table for the key {table_key}") from e

    def _table_schema(self, table_key: TableKey, schema_cb: jpy.JType, failure_cb: jpy.JType) -> None:
        """ Provides the table data schema and the partitioning values schema for the table with the given table key as
        two serialized byte buffers to the PythonTableDataService (Java) via callbacks. Only called by the
        PythonTableDataService.

        Args:
            table_key (TableKey): the table key
            schema_cb (jpy.JType): the Java callback function with one argument: an array of byte buffers that contain
                the serialized table data arrow and partitioning values schemas
            failure_cb (jpy.JType): the failure Java callback function with one argument: an exception stringyy
        """
        def schema_cb_proxy(dt_schema: pa.Schema, pc_schema: Optional[pa.Schema] = None):
            j_dt_schema_bb = jpy.byte_buffer(dt_schema.serialize())
            pc_schema = pc_schema if pc_schema is not None else pa.schema([])
            j_pc_schema_bb = jpy.byte_buffer(pc_schema.serialize())
            schema_cb.accept(jpy.array("java.nio.ByteBuffer", [j_pc_schema_bb, j_dt_schema_bb]))

        def failure_cb_proxy(error: Exception):
            message = error.getMessage() if hasattr(error, "getMessage") else str(error)
            tb_str = traceback.format_exc()
            failure_cb.accept("\n".join([message, tb_str]))

        self._backend.table_schema(table_key, schema_cb_proxy, failure_cb_proxy)

    def _table_locations(self, table_key: TableKey, location_cb: jpy.JType, success_cb: jpy.JType,
                         failure_cb: jpy.JType) -> None:
        """ Provides the existing table locations for the table with the given table key to the PythonTableDataService
        (Java) via callbacks. Only called by the PythonTableDataService.

        Args:
            table_key (TableKey): the table key
            location_cb (jpy.JType): the Java callback function with two arguments: a table location key and an array of
                byte buffers that contain the serialized arrow schema and a record batch of the partitioning values
            success_cb (jpy.JType): the success Java callback function with no arguments
            failure_cb (jpy.JType): the failure Java callback function with one argument: an exception string
        """
        def location_cb_proxy(pt_location_key: TableLocationKey, pt_table: Optional[pa.Table] = None):
            j_tbl_location_key = _JTableLocationKeyImpl(pt_location_key)
            if pt_table is None or pt_table.to_batches() is None:
                location_cb.apply(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", []))
            else:
                if pt_table.num_rows != 1:
                    raise ValueError("The number of rows in the pyarrow table for partitioning values must be 1")
                bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
                bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
                location_cb.accept(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", bb_list))

        def success_cb_proxy():
            success_cb.run()

        def failure_cb_proxy(error: Exception):
            message = error.getMessage() if hasattr(error, "getMessage") else str(error)
            tb_str = traceback.format_exc()
            failure_cb.accept("\n".join([message, tb_str]))

        self._backend.table_locations(table_key, location_cb_proxy, success_cb_proxy, failure_cb_proxy)

    def _subscribe_to_table_locations(self, table_key: TableKey, location_cb: jpy.JType, success_cb: jpy.JType,
                                      failure_cb: jpy.JType) -> Callable[[], None]:
        """ Provides the table locations, existing and new, for the table with the given table key to the
        PythonTableDataService (Java) via callbacks. Only called by the PythonTableDataService.

        Args:
            table_key (TableKey): the table key
            location_cb (jpy.JType): the Java callback function with two arguments: a table location key of the new
                location and an array of byte buffers that contain the partitioning arrow schema and the serialized
                record batches of the partitioning values
            success_cb (jpy.JType): the success Java callback function with no arguments
            failure_cb (jpy.JType): the failure Java callback function with one argument: an exception string

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
        """
        def location_cb_proxy(pt_location_key: TableLocationKey, pt_table: Optional[pa.Table] = None):
            j_tbl_location_key = _JTableLocationKeyImpl(pt_location_key)
            if pt_table is None:
                location_cb.apply(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", []))
            else:
                if pt_table.num_rows != 1:
                    raise ValueError("The number of rows in the pyarrow table for partitioning column values must be 1")
                bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
                bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
                location_cb.accept(j_tbl_location_key, jpy.array("java.nio.ByteBuffer", bb_list))

        def success_cb_proxy():
            success_cb.run()

        def failure_cb_proxy(error: Exception):
            message = error.getMessage() if hasattr(error, "getMessage") else str(error)
            tb_str = traceback.format_exc()
            failure_cb.accept("\n".join([message, tb_str]))

        return self._backend.subscribe_to_table_locations(table_key, location_cb_proxy, success_cb_proxy,
                                                          failure_cb_proxy)

    def _table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey, size_cb: jpy.JType,
                             failure_cb: jpy.JType) -> None:
        """ Provides the size of the table location with the given table key and table location key to the
        PythonTableDataService (Java) via callbacks. Only called by the PythonTableDataService.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            size_cb (jpy.JType): the Java callback function with one argument: the size of the table location in number
                of rows
            failure_cb (jpy.JType): the failure Java callback function with one argument: an exception string
        """
        def size_cb_proxy(size: int):
            size_cb.accept(size)

        def failure_cb_proxy(error: Exception):
            message = error.getMessage() if hasattr(error, "getMessage") else str(error)
            tb_str = traceback.format_exc()
            failure_cb.accept("\n".join([message, tb_str]))

        self._backend.table_location_size(table_key, table_location_key, size_cb_proxy, failure_cb_proxy)

    def _subscribe_to_table_location_size(self, table_key: TableKey, table_location_key: TableLocationKey,
                                          size_cb: jpy.JType, success_cb: jpy.JType, failure_cb: jpy.JType) -> Callable[[], None]:
        """ Provides the current and future sizes of the table location with the given table key and table location key
        to the PythonTableDataService (Java) via callbacks. Only called by the PythonTableDataService.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            size_cb (jpy.JType): the Java callback function with one argument: the size of the location in number of
                rows
            success_cb (jpy.JType): the success Java callback function with no arguments
            failure_cb (jpy.JType): the failure Java callback function with one argument: an exception string

        Returns:
            Callable[[], None]: a function that can be called to unsubscribe from this subscription
        """
        def size_cb_proxy(size: int):
            size_cb.accept(size)

        def success_cb_proxy():
            success_cb.run()

        def failure_cb_proxy(error: Exception):
            message = error.getMessage() if hasattr(error, "getMessage") else str(error)
            tb_str = traceback.format_exc()
            failure_cb.accept("\n".join([message, tb_str]))

        return self._backend.subscribe_to_table_location_size(table_key, table_location_key, size_cb_proxy,
                                                              success_cb_proxy, failure_cb_proxy)

    def _column_values(self, table_key: TableKey, table_location_key: TableLocationKey, col: str, offset: int,
                       min_rows: int, max_rows: int, values_cb: jpy.JType, failure_cb: jpy.JType) -> None:
        """ Provides the data values for the column with the given name for the table column with the given table key
        and table location key to the PythonTableDataService (Java) via callbacks. Only called by the
        PythonTableDataService.

        Args:
            table_key (TableKey): the table key
            table_location_key (TableLocationKey): the table location key
            col (str): the column name
            offset (int): the starting row index
            min_rows (int): the minimum number of rows to return, min_rows is always <= page size
            max_rows (int): the maximum number of rows to return
            values_cb (jpy.JType): the Java callback function with one argument: an array of byte buffers that contain
                the arrow schema and the serialized record batches for the given column
            failure_cb (jpy.JType): the failure Java callback function with one argument: an exception string
        """
        def values_cb_proxy(pt_table: pa.Table):
            if len(pt_table) < min_rows or len(pt_table) > max_rows:
                raise ValueError("The number of rows in the pyarrow table for column values must be in the range of "
                                 f"{min_rows} to {max_rows}")
            bb_list = [jpy.byte_buffer(rb.serialize()) for rb in pt_table.to_batches()]
            bb_list.insert(0, jpy.byte_buffer(pt_table.schema.serialize()))
            values_cb.accept(jpy.array("java.nio.ByteBuffer", bb_list))

        def failure_cb_proxy(error: Exception):
            message = error.getMessage() if hasattr(error, "getMessage") else str(error)
            tb_str = traceback.format_exc()
            failure_cb.accept("\n".join([message, tb_str]))

        self._backend.column_values(table_key, table_location_key, col, offset, min_rows, max_rows, values_cb_proxy,
                                    failure_cb_proxy)
