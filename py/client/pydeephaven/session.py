#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import os
import threading
from typing import List

import pyarrow
from bitstring import BitArray

import grpc

from pydeephaven._arrow_flight_service import ArrowFlightService
from pydeephaven._console_service import ConsoleService
from pydeephaven._app_service import AppService
from pydeephaven._session_service import SessionService
from pydeephaven._table_ops import TimeTableOp, EmptyTableOp, MergeTablesOp, FetchTableOp
from pydeephaven._table_service import TableService
from pydeephaven.dherror import DHError
from pydeephaven.proto import ticket_pb2
from pydeephaven.query import Query
from pydeephaven.table import Table

class Session:
    """ A Session object represents a connection to the Deephaven data server. It contains a number of convenience
    methods for asking the server to create tables, import Arrow data into tables, merge tables, run Python scripts, and
    execute queries.

    Session objects can be used in Python with statement so that whatever happens in the with statement block, they
    are guaranteed to be closed upon exit.

    Attributes:
        tables (list[str]): names of the global tables available in the server after running scripts
        is_alive (bool): check if the session is still alive (may refresh the session)
    """

    def __init__(self, host: str = None, port: int = None, never_timeout: bool = True, session_type: str = 'python'):
        """ Initialize a Session object that connects to the Deephaven server

        Args:
            host (str): the host name or IP address of the remote machine, default is 'localhost'
            port (int): the port number that Deephaven server is listening on, default is 10000
            never_timeout (bool, optional): never allow the session to timeout, default is True
            session_type (str, optional): the Deephaven session type. Defaults to 'python'

        Raises:
            DHError
        """
        self._r_lock = threading.RLock()
        self._last_ticket = 0
        self._ticket_bitarray = BitArray(1024)

        self.host = host
        if not host:
            self.host = os.environ.get("DH_HOST", "localhost")

        self.port = port
        if not port:
            self.port = int(os.environ.get("DH_PORT", 10000))

        self.is_connected = False
        self.session_token = None
        self.grpc_channel = None
        self._session_service = None
        self._table_service = None
        self._grpc_barrage_stub = None
        self._console_service = None
        self._flight_service = None
        self._app_service = None
        self._never_timeout = never_timeout
        self._keep_alive_timer = None
        self._session_type = session_type

        self._connect()

    def __enter__(self):
        if not self.is_connected:
            self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def tables(self):
        with self._r_lock:
            fields = self._fetch_fields()
            return [field.field_name for field in fields if field.application_id == 'scope' and field.typed_ticket.type == 'Table']

    @property
    def grpc_metadata(self):
        return [(b'authorization', self.session_token)]

    @property
    def table_service(self):
        if not self._table_service:
            self._table_service = TableService(self)
        return self._table_service

    @property
    def session_service(self):
        if not self._session_service:
            self._session_service = SessionService(self)
        return self._session_service

    @property
    def console_service(self):
        if not self._console_service:
            self._console_service = ConsoleService(self)
        return self._console_service

    @property
    def flight_service(self):
        if not self._flight_service:
            self._flight_service = ArrowFlightService(self)

        return self._flight_service

    @property
    def app_service(self):
        if not self._app_service:
            self._app_service = AppService(self)

        return self._app_service

    def make_ticket(self, ticket_no=None):
        if not ticket_no:
            ticket_no = self.get_ticket()
        ticket_bytes = ticket_no.to_bytes(4, byteorder='little', signed=True)
        return ticket_pb2.Ticket(ticket=b'e' + ticket_bytes)

    def get_ticket(self):
        with self._r_lock:
            self._last_ticket += 1
            if self._last_ticket == 2 ** 31 - 1:
                raise DHError("fatal error: out of free internal ticket")

            return self._last_ticket

    def _fetch_fields(self):
        """ Returns a list of available fields on the server.

        Raises:
            DHError
        """
        with self._r_lock:
            list_fields = self.app_service.list_fields()
            resp = next(list_fields)
            if not list_fields.cancel():
                raise DHError("could not cancel ListFields subscription")
            return resp.created if resp.created else []

    def _connect(self):
        with self._r_lock:
            self.grpc_channel, self.session_token, self._timeout = self.session_service.connect()
            self.is_connected = True

            if self._never_timeout:
                self._keep_alive()

    def _keep_alive(self):
        if self._keep_alive_timer:
            self._refresh_token()
        self._keep_alive_timer = threading.Timer(self._timeout / 2 / 1000, self._keep_alive)
        self._keep_alive_timer.daemon = True
        self._keep_alive_timer.start()

    def _refresh_token(self):
        with self._r_lock:
            try:
                self.session_token, self._timeout = self.session_service.refresh_token()
            except DHError:
                self.is_connected = False

    @property
    def is_alive(self):
        with self._r_lock:
            if not self.is_connected:
                return False

            if self._never_timeout:
                return True

            try:
                self.session_token = self.session_service.refresh_token()
                return True
            except DHError as e:
                self.is_connected = False
                return False

    def close(self) -> None:
        """ Close the Session object if it hasn't timed out already.

        Raises:
            DHError
        """
        with self._r_lock:
            if self.is_connected:
                self.session_service.close()
                self.grpc_channel.close()
                self.is_connected = False
                self._last_ticket = 0
                # self._executor.shutdown()

    def release(self, ticket):
        self.session_service.release(ticket)

    # convenience/factory methods
    def run_script(self, script: str) -> None:
        """ Run the supplied Python script on the server.

        Args:
            script (str): the Python script code

        Raises:
            DHError
        """

        with self._r_lock:
            response = self.console_service.run_script(script)
            if response.error_message != '':
                raise DHError("could not run script: " + response.error_message)

    def open_table(self, name: str) -> Table:
        """ Open a table in the global scope with the given name on the server.

        Args:
            name (str): the name of the table

        Returns:
            a Table object

        Raises:
            DHError
        """
        with self._r_lock:
            ticket = ticket_pb2.Ticket(ticket=f's/{name}'.encode(encoding='ascii'))

            faketable = Table(session=self, ticket=ticket)

            try:
                table_op = FetchTableOp()
                return self.table_service.grpc_table_op(faketable, table_op)
            except Exception as e:
                if isinstance(e.__cause__, grpc.RpcError):
                    if e.__cause__.code() == grpc.StatusCode.INVALID_ARGUMENT:
                        raise DHError(f"no table by the name {name}") from None
                raise e
            finally:
                # Explicitly close the table without releasing it (because it isn't ours)
                faketable.ticket = None
                faketable.schema = None


    def bind_table(self, name: str, table: Table) -> None:
        """ Bind a table to the given name on the server so that it can be referenced by that name.

        Args:
            name (str): name for the table
            table (Table): a Table object

        Raises:
            DHError
        """
        with self._r_lock:
            self.console_service.bind_table(table=table, variable_name=name)

    def time_table(self, period: int, start_time: int = None) -> Table:
        """ Create a time table on the server.

        Args:
            period (int): the interval (in nano seconds) at which the time table ticks (adds a row)
            start_time (int, optional): the start time for the time table in nano seconds, default is None (meaning now)

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = TimeTableOp(start_time=start_time, period=period)
        return self.table_service.grpc_table_op(None, table_op)

    def empty_table(self, size: int) -> Table:
        """ create an empty table on the server.

        Args:
            size (int): the size of the empty table in number of rows

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = EmptyTableOp(size=size)
        return self.table_service.grpc_table_op(None, table_op)

    def import_table(self, data: pyarrow.Table) -> Table:
        """ Import the pyarrow table as a new Deephaven table on the server.

        Deephaven supports most of the Arrow data types. However, if the pyarrow table contains any field with a data
        type not supported by Deephaven, the import operation will fail.

        Args:
            data (pyarrow.Table): a pyarrow Table object

        Returns:
            a Table object

        Raises:
            DHError
        """
        return self.flight_service.import_table(data=data)

    def merge_tables(self, tables: List[Table], order_by: str = None) -> Table:
        """ Merge several tables into one table on the server.

        Args:
            tables (list[Table]): the list of Table objects to merge
            order_by (str, optional): if specified the resultant table will be sorted on this column

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = MergeTablesOp(tables=tables, key_column=order_by)
        return self.table_service.grpc_table_op(None, table_op)

    def query(self, table: Table) -> Query:
        """ Create a Query object to define a sequence of operations on a Deephaven table.

        Args:
            table (Table): a Table object

        Returns:
            a Query object

        Raises:
            DHError
        """
        return Query(self, table)
