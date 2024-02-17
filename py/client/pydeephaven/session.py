#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module implements the Session class which provides methods to connect to and interact with the Deephaven
server."""

import base64
import os
import threading
from typing import Dict, List, Union, Tuple, Any

import grpc
import pyarrow as pa
import pyarrow.flight as paflight
from bitstring import BitArray
from pyarrow._flight import ClientMiddlewareFactory, ClientMiddleware, ClientAuthHandler

from pydeephaven._app_service import AppService
from pydeephaven._arrow_flight_service import ArrowFlightService
from pydeephaven._config_service import ConfigService
from pydeephaven._console_service import ConsoleService
from pydeephaven._input_table_service import InputTableService
from pydeephaven._plugin_obj_service import PluginObjService
from pydeephaven._session_service import SessionService
from pydeephaven._table_ops import TimeTableOp, EmptyTableOp, MergeTablesOp, FetchTableOp, CreateInputTableOp
from pydeephaven._table_service import TableService
from pydeephaven._utils import to_list
from pydeephaven.dherror import DHError
from pydeephaven.experimental.plugin_client import PluginClient
from pydeephaven.proto import ticket_pb2
from pydeephaven.query import Query
from pydeephaven.table import Table, InputTable


class _DhClientAuthMiddlewareFactory(ClientMiddlewareFactory):
    def __init__(self, session):
        super().__init__()
        self._session = session

    def start_call(self, info):
        return _DhClientAuthMiddleware(self._session)


class _DhClientAuthMiddleware(ClientMiddleware):
    def __init__(self, session):
        super().__init__()
        self._session = session

    def call_completed(self, exception):
        super().call_completed(exception)

    def received_headers(self, headers):
        super().received_headers(headers)
        if headers:
            auth_token = bytes(headers.get("authorization")[0], encoding='ascii')
            if auth_token and auth_token != self._session._auth_token:
                self._session._auth_token = auth_token

    def sending_headers(self):
        return {
            **{
                "authorization": self._session._auth_token
            }, **self._session._extra_headers
        }


class _DhClientAuthHandler(ClientAuthHandler):
    def __init__(self, session):
        super().__init__()
        self._session = session
        self._token = b''

    def authenticate(self, outgoing, incoming):
        outgoing.write(self._session._auth_token)
        self._token = incoming.read()

    def get_token(self):
        return self._token


class Session:
    """A Session object represents a connection to the Deephaven data server. It contains a number of convenience
    methods for asking the server to create tables, import Arrow data into tables, merge tables, run Python scripts, and
    execute queries.

    Session objects can be used in Python with statement so that whatever happens in the with statement block, they
    are guaranteed to be closed upon exit.

    Attributes:
        tables (list[str]): names of the global tables available in the server after running scripts
        is_alive (bool): check if the session is still alive (may refresh the session)
    """

    def __init__(self, host: str = None,
                 port: int = None,
                 auth_type: str = "Anonymous",
                 auth_token: str = "",
                 never_timeout: bool = True,
                 session_type: str = 'python',
                 use_tls: bool = False,
                 tls_root_certs: bytes = None,
                 client_cert_chain: bytes = None,
                 client_private_key: bytes = None,
                 client_opts: List[Tuple[str, Union[int, str]]] = None,
                 extra_headers: Dict[bytes, bytes] = None):
        """Initializes a Session object that connects to the Deephaven server

        Args:
            host (str): the host name or IP address of the remote machine, default is 'localhost'
            port (int): the port number that Deephaven server is listening on, default is 10000
            auth_type (str): the authentication type string, can be "Anonymous', 'Basic", or any custom-built
                authenticator in the server, such as "io.deephaven.authentication.psk.PskAuthenticationHandler",
                default is 'Anonymous'.
            auth_token (str): the authentication token string. When auth_type is 'Basic', it must be
                "user:password"; when auth_type is "Anonymous', it will be ignored; when auth_type is a custom-built
                authenticator, it must conform to the specific requirement of the authenticator
            never_timeout (bool): never allow the session to timeout, default is True
            session_type (str): the Deephaven session type. Defaults to 'python'
            use_tls (bool): if True, use a TLS connection.  Defaults to False
            tls_root_certs (bytes): PEM encoded root certificates to use for TLS connection, or None to use system defaults.
                 If not None implies use a TLS connection and the use_tls argument should have been passed
                 as True. Defaults to None
            client_cert_chain (bytes): PEM encoded client certificate if using mutual TLS.  Defaults to None,
                 which implies not using mutual TLS.
            client_private_key (bytes): PEM encoded client private key for client_cert_chain if using mutual TLS.
                 Defaults to None, which implies not using mutual TLS.
            client_opts (List[Tuple[str,Union[int,str]]): list of tuples for name and value of options to
                the underlying grpc channel creation.  Defaults to None, which implies not using any channel
                options.
                See https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html for a list of valid options.
                Example options:
                  [ ('grpc.target_name_override', 'idonthaveadnsforthishost'),
                    ('grpc.min_reconnect_backoff_ms', 2000) ]
            extra_headers (Dict[bytes, bytes]): additional headers (and values) to add to server requests.
                Defaults to None, which implies not using any extra headers.

        Raises:
            DHError
        """
        self._r_lock = threading.RLock()  # for thread-safety when accessing/changing session global state
        self._last_ticket = 0
        self._ticket_bitarray = BitArray(1024)

        self.host = host
        if not host:
            self.host = os.environ.get("DH_HOST", "localhost")

        self.port = port
        if not port:
            self.port = int(os.environ.get("DH_PORT", 10000))

        self._use_tls = use_tls
        self._tls_root_certs = tls_root_certs
        self._client_cert_chain = client_cert_chain
        self._client_private_key = client_private_key
        self._client_opts = client_opts
        self._extra_headers = extra_headers if extra_headers else {}

        self.is_connected = False

        if auth_type == "Anonymous":
            self._auth_token = auth_type
        elif auth_type == "Basic":
            auth_token_base64 = base64.b64encode(auth_token.encode("ascii")).decode("ascii")
            self._auth_token = "Basic " + auth_token_base64
        else:
            self._auth_token = str(auth_type) + " " + auth_token

        self.grpc_channel = None
        self._session_service = None
        self._table_service = None
        self._grpc_barrage_stub = None
        self._console_service = None
        self._flight_service = None
        self._app_service = None
        self._input_table_service = None
        self._plugin_obj_service = None
        self._never_timeout = never_timeout
        self._keep_alive_timer = None
        self._session_type = session_type
        self._flight_client = None
        self._auth_handler = None
        self._config_service = None

        self._connect()

    def __enter__(self):
        if not self.is_connected:
            self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    @property
    def tables(self):
        with self._r_lock:
            fields = self._fetch_fields()
            return [field.field_name for field in fields if
                    field.application_id == 'scope' and field.typed_ticket.type == 'Table']

    @property
    def exportable_objects(self) -> Dict[str, ticket_pb2.TypedTicket]:
        with self._r_lock:
            fields = self._fetch_fields()
            return {field.field_name: field.typed_ticket for field in fields if field.application_id == 'scope'}

    @property
    def grpc_metadata(self):
        l = [(b'authorization', self._auth_token)]
        if self._extra_headers:
            l.extend(list(self._extra_headers.items()))
        return l

    @property
    def table_service(self) -> TableService:
        if not self._table_service:
            self._table_service = TableService(self)
        return self._table_service

    @property
    def session_service(self) -> SessionService:
        if not self._session_service:
            self._session_service = SessionService(self)
        return self._session_service

    @property
    def console_service(self) -> ConsoleService:
        if not self._console_service:
            self._console_service = ConsoleService(self)
        return self._console_service

    @property
    def flight_service(self) -> ArrowFlightService:
        if not self._flight_service:
            self._flight_service = ArrowFlightService(self, self._flight_client)

        return self._flight_service

    @property
    def app_service(self) -> AppService:
        if not self._app_service:
            self._app_service = AppService(self)

        return self._app_service

    @property
    def config_service(self):
        if not self._config_service:
            self._config_service = ConfigService(self)

        return self._config_service

    @property
    def input_table_service(self) -> InputTableService:
        if not self._input_table_service:
            self._input_table_service = InputTableService(self)

        return self._input_table_service

    @property
    def plugin_object_service(self) -> PluginObjService:
        if not self._plugin_obj_service:
            self._plugin_obj_service = PluginObjService(self)

        return self._plugin_obj_service

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
        """Returns a list of available fields on the server.

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
            try:
                scheme = "grpc+tls" if self._use_tls else "grpc"
                self._flight_client = paflight.FlightClient(
                    location=f"{scheme}://{self.host}:{self.port}",
                    middleware=[_DhClientAuthMiddlewareFactory(self)],
                    tls_root_certs=self._tls_root_certs,
                    cert_chain=self._client_cert_chain,
                    private_key=self._client_private_key,
                    generic_options=self._client_opts
                )
                self._auth_handler = _DhClientAuthHandler(self)
                self._flight_client.authenticate(self._auth_handler)
            except Exception as e:
                raise DHError("failed to connect to the server.") from e

            self.grpc_channel = self.session_service.connect()

            config_dict = self.config_service.get_configuration_constants()
            session_duration = config_dict.get("http.session.durationMs")
            if not session_duration:
                raise DHError("server configuration is missing http.session.durationMs")

            self.is_connected = True

            self._timeout = int(session_duration.string_value)
            if self._never_timeout:
                self._keep_alive()

    def _keep_alive(self):
        if self.is_connected:
            if self._keep_alive_timer:
                self._refresh_token()
            self._keep_alive_timer = threading.Timer(self._timeout / 2 / 1000, self._keep_alive)
            self._keep_alive_timer.daemon = True
            self._keep_alive_timer.start()

    def _refresh_token(self):
        try:
            self._flight_client.authenticate(self._auth_handler)
        except Exception as e:
            self.is_connected = False
            raise DHError("failed to refresh auth token") from e

    @property
    def is_alive(self) -> bool:
        """Whether the session is alive."""
        with self._r_lock:
            if not self.is_connected:
                return False

            if self._never_timeout:
                return True

            try:
                self._flight_client.authenticate(self._auth_handler)
                return True
            except DHError as e:
                self.is_connected = False
                return False

    def close(self) -> None:
        """Closes the Session object if it hasn't timed out already.

        Raises:
            DHError
        """
        with self._r_lock:
            if self.is_connected:
                self.session_service.close()
                self.grpc_channel.close()
                self.is_connected = False
                self._last_ticket = 0
                self._flight_client.close()

    def release(self, ticket):
        self.session_service.release(ticket)

    # convenience/factory methods
    def run_script(self, script: str) -> None:
        """Runs the supplied Python script on the server.

        Args:
            script (str): the Python script code

        Raises:
            DHError
        """
        response = self.console_service.run_script(script)
        if response.error_message != '':
            raise DHError("could not run script: " + response.error_message)

    def open_table(self, name: str) -> Table:
        """Opens a table in the global scope with the given name on the server.

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
        """Binds a table to the given name on the server so that it can be referenced by that name.

        Args:
            name (str): name for the table
            table (Table): a Table object

        Raises:
            DHError
        """
        with self._r_lock:
            self.console_service.bind_table(table=table, variable_name=name)

    def time_table(self, period: Union[int, str], start_time: Union[int, str] = None,
                   blink_table: bool = False) -> Table:
        """Creates a time table on the server.

        Args:
            period (Union[int, str]): the interval at which the time table ticks (adds a row); units are nanoseconds
                or a time interval string, e.g. "PT00:00:.001" or "PT1S"
            start_time (Union[int, str]): the start time for the time table in nanoseconds or as a date time
                formatted string; default is None (meaning now)
            blink_table (bool, optional): if the time table should be a blink table, defaults to False

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = TimeTableOp(start_time=start_time, period=period, blink_table=blink_table)
        return self.table_service.grpc_table_op(None, table_op)

    def empty_table(self, size: int) -> Table:
        """Creates an empty table on the server.

        Args:
            size (int): the size of the empty table in number of rows

        Returns:
            a Table object

        Raises:
            DHError
        """
        table_op = EmptyTableOp(size=size)
        return self.table_service.grpc_table_op(None, table_op)

    def import_table(self, data: pa.Table) -> Table:
        """Imports the pyarrow table as a new Deephaven table on the server.

        Deephaven supports most of the Arrow data types. However, if the pyarrow table contains any field with a data
        type not supported by Deephaven, the import operation will fail.

        Args:
            data (pa.Table): a pyarrow Table object

        Returns:
            a Table object

        Raises:
            DHError
        """
        return self.flight_service.import_table(data=data)

    def merge_tables(self, tables: List[Table], order_by: str = None) -> Table:
        """Merges several tables into one table on the server.

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
        """Creates a Query object to define a sequence of operations on a Deephaven table.

        Args:
            table (Table): a Table object

        Returns:
            a Query object

        Raises:
            DHError
        """
        return Query(self, table)

    def input_table(self, schema: pa.Schema = None, init_table: Table = None,
                    key_cols: Union[str, List[str]] = None) -> InputTable:
        """Creates an InputTable from either Arrow schema or initial table. When key columns are
        provided, the InputTable will be keyed, otherwise it will be append-only.

        Args:
            schema (pa.Schema): the schema for the InputTable
            init_table (Table): the initial table
            key_cols (Union[str, Sequence[str]): the name(s) of the key column(s)

        Returns:
            an InputTable

        Raises:
            DHError, ValueError
        """
        if schema is None and init_table is None:
            raise ValueError("either arrow schema or init table should be provided.")
        elif schema and init_table:
            raise ValueError("both arrow schema and init table are provided.")

        table_op = CreateInputTableOp(schema=schema, init_table=init_table, key_cols=to_list(key_cols))
        input_table = self.table_service.grpc_table_op(None, table_op, table_class=InputTable)
        input_table.key_cols = key_cols
        return input_table

    def plugin_client(self, exportable_obj: ticket_pb2.TypedTicket) -> PluginClient:
        """Wraps a ticket as a PluginClient. Capabilities here vary based on the server implementation of the ObjectType,
        but most will at least send a response payload to the client, possibly including references to other objects.
        In some cases, depending on the server implementation, the client will also be able to send the same sort of
        messages back to the server.

        Part of the experimental plugin API."""
        return PluginClient(self, exportable_obj)
