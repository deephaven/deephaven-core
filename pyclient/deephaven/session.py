import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

import grpc
import pyarrow as pa
import pyarrow.flight
from bitstring import BitArray

from deephaven.batch_assembler import BatchOpAssembler
from deephaven.dherror import DHError
from deephaven.proto import barrage_pb2, barrage_pb2_grpc, console_pb2, table_pb2, session_pb2_grpc, \
    console_pb2_grpc, session_pb2, ticket_pb2, table_pb2_grpc
from deephaven.query import Query
from deephaven.table import EmptyTable, Table, TimeTable


def _map_arrow_type(arrow_type):
    arrow_to_dh = {
        pa.null(): '',
        pa.bool_(): '',
        pa.int8(): 'byte',
        pa.int16(): 'short',
        pa.int32(): 'int',
        pa.int64(): 'long',
        pa.uint8(): '',
        pa.uint16(): 'char',
        pa.uint32(): '',
        pa.uint64(): '',
        pa.float16(): '',
        pa.float32(): 'float',
        pa.float64(): 'double',
        pa.time32('s'): '',
        pa.time32('ms'): '',
        pa.time64('us'): '',
        pa.time64('ns'): 'io.deephaven.db.tables.utils.DBDateTime',
        pa.timestamp('us', tz=None): '',
        pa.timestamp('ns', tz=None): '',
        pa.date32(): 'java.time.LocalDate',
        pa.date64(): 'java.time.LocalDate',
        pa.binary(): '',
        pa.string(): 'java.lang.String',
        pa.utf8(): 'java.lang.String',
        pa.large_binary(): '',
        pa.large_string(): '',
        pa.large_utf8(): '',
        # decimal128(int precision, int scale=0)
        # list_(value_type, int list_size=-1)
        # large_list(value_type)
        # map_(key_type, item_type[, keys_sorted])
        # struct(fields)
        # dictionary(index_type, value_type, …)
        # field(name, type, bool nullable = True[, metadata])
        # schema(fields[, metadata])
        # from_numpy_dtype(dtype)
    }

    dh_type = arrow_to_dh.get(arrow_type)
    if not dh_type:
        # if this is a case of timestamp with tz specified
        if isinstance(arrow_type, pa.TimestampType):
            dh_type = "io.deephaven.db.tables.utils.DBDateTime"

    if not dh_type:
        raise DHError('unsupported arrow data type : ', arrow_type)
    return {"deephaven:type": dh_type}


class Session:
    def __init__(self, host='localhost', port=10000, user="", password=""):
        self._r_lock = threading.RLock()
        self._last_ticket = 0
        self._ticket_bitarray = BitArray(1024)
        self._metadata = None
        self._barrage_finish_event = threading.Event()
        self._barrage_wait_event = threading.Event()
        self._executor = ThreadPoolExecutor()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.is_connected = False
        self._session_token = None
        self.grpc_channel = None
        self._grpc_session_stub = None
        self._grpc_table_stub = None
        self._grpc_barrage_stub = None
        self._grpc_console_stub = None
        self._flight_client = None
        self.exported_tables = []
        self._executor_future = None
        self.console_id = None
        self.console_tables = {}

        self._connect()

    @property
    def table_service(self):
        if not self._grpc_table_stub:
            self._grpc_table_stub = table_pb2_grpc.TableServiceStub(self.grpc_channel)
        return self._grpc_table_stub

    @property
    def session_service(self):
        if not self._grpc_session_stub:
            self._grpc_session_stub = session_pb2_grpc.SessionServiceStub(self.grpc_channel)
        return self._grpc_session_stub

    @property
    def console_service(self):
        if not self._grpc_console_stub:
            self._grpc_console_stub = console_pb2_grpc.ConsoleServiceStub(self.grpc_channel)
        return self._grpc_console_stub

    @property
    def barrage_service(self):
        if not self._grpc_barrage_stub:
            self._grpc_barrage_stub = barrage_pb2_grpc.BarrageServiceStub(self.grpc_channel)

        return self._grpc_barrage_stub

    @property
    def flight_client(self):
        if not self._flight_client:
            self._flight_client = pyarrow.flight.connect((self.host, self.port))

        return self._flight_client

    def make_flight_ticket(self, ticket_no=None):
        if not ticket_no:
            ticket_no = self.get_ticket()
        ticket_bytes = ticket_no.to_bytes(4, 'little', signed=True)
        return ticket_pb2.Ticket(ticket=b'e' + ticket_bytes)

    def get_ticket(self):
        with self._r_lock:
            if self._ticket_bitarray[self._last_ticket] == 0:
                self._ticket_bitarray.set(1, self._last_ticket)
                return self._last_ticket + 1
            else:
                for i in range(len(self._ticket_bitarray)):
                    if self._ticket_bitarray[i] == 0:
                        self._ticket_bitarray.set(1, i)
                        return i + 1

                if i == 2 ** 31 - 1:
                    raise DHError("fatal error: out of free internal ticket")

                self._ticket_bitarray.append(BitArray(1024))
                return i + 1

    def release_ticket(self, ticket):
        with self._r_lock:
            print("release_ticket", ticket)
            self._ticket_bitarray.set(0, ticket - 1)
            self._last_ticket = ticket - 1

    def _connect(self):
        with self._r_lock:
            self.grpc_channel = grpc.insecure_channel(":".join([self.host, str(self.port)]))

            try:
                response = self.session_service.NewSession(
                    session_pb2.HandshakeRequest(auth_protocol=1, payload=b'hello deephaven'))
                self._session_token = response.session_token
                # self.metadata_header = response.metadata_header
                self.is_connected = True
                self._metadata = [(b'deephaven_session_id', self._session_token)]
            except Exception as e:
                self.grpc_channel.close()
                raise DHError("failed to connect to the server.") from e

    @property
    def is_alive(self):
        with self._r_lock:
            try:
                self.keep_alive()
            except:
                ...

            return self.is_connected

    def keep_alive(self):
        with self._r_lock:
            if not self.is_connected:
                return

            try:
                response = self.session_service.RefreshSessionToken(
                    session_pb2.HandshakeRequest(auth_protocol=0, payload=self._session_token), metadata=self._metadata)
                self._session_token = response.session_token
            except Exception as e:
                self.is_connected = False
                raise DHError("failed to refresh session token.") from e

    def close(self):
        with self._r_lock:
            if self.is_connected:
                response = self.session_service.CloseSession(
                    session_pb2.HandshakeRequest(auth_protocol=0, payload=self._session_token),
                    metadata=self._metadata)
                # print(response)
                self.grpc_channel.close()
                self.is_connected = False
                self._last_ticket = 0
                self._executor.shutdown()

    def start_console(self):
        if self.console_id:
            return

        try:
            result_id = self.make_flight_ticket()
            response = self.console_service.StartConsole(
                console_pb2.StartConsoleRequest(result_id=result_id, session_type='python'),
                metadata=self._metadata)
            self.console_id = response.result_id
        except Exception as e:
            raise DHError("failed to start a console.") from e

    def _update_console_tables(self, response):
        if response.created:
            for t in response.created:
                self.console_tables[t.name] = t.type

        if response.updated:
            for t in response.updated:
                self.console_tables[t.name] = t.type

        if response.removed:
            for t in response.removed:
                self.console_tables.pop(t.name, None)

    def run_script(self, server_script):
        if not self.console_id:
            self.start_console()

        try:
            response = self.console_service.ExecuteCommand(
                console_pb2.ExecuteCommandRequest(
                    console_id=self.console_id,
                    code=server_script),
                metadata=self._metadata)
            self._update_console_tables(response)
            # print(response)
        except Exception as e:
            raise DHError("failed to execute a command in the console.") from e

    def open_table(self, name):
        if not self.console_id:
            self.start_console()

        try:
            result_id = self.make_flight_ticket()
            response = self.console_service.FetchTable(
                console_pb2.FetchTableRequest(console_id=self.console_id,
                                              table_id=result_id,
                                              table_name=name),
                metadata=self._metadata)

            if response.success:
                self.exported_tables.append(Table(self, ticket=response.result_id.ticket,
                                                  schema_header=response.schema_header,
                                                  size=response.size,
                                                  is_static=response.is_static))
                return self.exported_tables[-1]
            else:
                raise DHError("error open a table: " + response.error_info)
        except Exception as e:
            raise DHError("failed to open a table.") from e

    def time_table(self, start_time=0, period=1000000000):
        try:
            result_id = self.make_flight_ticket()
            response = self.table_service.TimeTable(
                table_pb2.TimeTableRequest(result_id=result_id, start_time_nanos=start_time, period_nanos=period),
                metadata=self._metadata)

            if response.success:
                self.exported_tables.append(TimeTable(self, ticket=response.result_id.ticket,
                                                      schema_header=response.schema_header,
                                                      start_time=start_time,
                                                      period=period,
                                                      is_static=response.is_static))
                return self.exported_tables[-1]
            else:
                raise DHError("error encountered in creating a time table: " + response.error_info)
        except Exception as e:
            raise DHError("failed to create a time table.") from e

    def empty_table(self, size=0):
        try:
            result_id = self.make_flight_ticket()
            response = self.table_service.EmptyTable(
                table_pb2.EmptyTableRequest(result_id=result_id, size=size),
                metadata=self._metadata)

            if response.success:
                self.exported_tables.append(EmptyTable(self, ticket=response.result_id.ticket,
                                                       schema_header=response.schema_header,
                                                       size=response.size,
                                                       is_static=response.is_static))
                return self.exported_tables[-1]
            else:
                raise DHError("error encountered in creating an empty table: " + response.error_info)
        except Exception as e:
            raise DHError("failed to create an empty table.") from e

    def update_table(self, table, column_specs=[]):
        try:
            result_id = self.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self.table_service.Update(
                table_pb2.SelectOrUpdateRequest(result_id=result_id,
                                                source_id=table_reference, column_specs=column_specs),
                metadata=self._metadata)

            if response.success:
                self.exported_tables.append(Table(self, ticket=response.result_id.ticket,
                                                  schema_header=response.schema_header,
                                                  size=response.size,
                                                  is_static=response.is_static))
                return self.exported_tables[-1]
            else:
                raise DHError("error encountered in table update: " + response.error_info)
        except Exception as e:
            raise DHError("failed to update the table.") from e

    def batch(self, dag):
        batch_assembler = BatchOpAssembler(self)
        dag.accept(batch_assembler)

        try:
            response = self.table_service.Batch(
                table_pb2.BatchTableRequest(ops=batch_assembler.batch),
                metadata=self._metadata)

            exported_tables = []
            for exported in response:
                exported_tables.append(Table(self, ticket=exported.result_id.ticket,
                                             schema_header=exported.schema_header,
                                             size=exported.size,
                                             is_static=exported.is_static))
            return exported_tables[-1]
        except Exception as e:
            raise DHError("failed to finish the table batch operation.") from e

    def snapshot_table(self, tbl):
        try:
            options = pyarrow.flight.FlightCallOptions(headers=self._metadata)
            flight_ticket = pyarrow.flight.Ticket(tbl.ticket.ticket)
            reader = self.flight_client.do_get(flight_ticket, options=options)
            return reader.read_all()
            # batches = [b.data for b in reader]
            # return pyarrow.Table.from_batches(batches)
            # df = reader.read_pandas()
            # return df
        except Exception as e:
            raise DHError("failed to take a snapshot of the table.") from e

    def import_table(self, data):
        try:
            options = pyarrow.flight.FlightCallOptions(headers=self._metadata)
            if not isinstance(data, (pyarrow.Table, pyarrow.RecordBatch)):
                raise DHError("source data must be either a PyArrow table or RecordBatch.")
            ticket = self.get_ticket()
            dh_fields = []
            for f in data.schema:
                dh_fields.append(pyarrow.field(name=f.name, type=f.type, metadata=_map_arrow_type(f.type)))
            dh_schema = pyarrow.schema(dh_fields)

            writer, reader = self.flight_client.do_put(
                pyarrow.flight.FlightDescriptor.for_path("export", str(ticket)), dh_schema, options=options)
            writer.write_table(data)
            writer.close()
            _ = reader.read()
            flight_ticket = self.make_flight_ticket(ticket)
            return Table(self, ticket=flight_ticket, schema=dh_schema)
        except Exception as e:
            raise DHError("failed to create a Deephaven table from Arrow data.") from e

    def bind_table(self, table, variable_name):
        if not table or not variable_name:
            raise DHError("invalid table and/or variable_name values.")
        try:
            response = self.console_service.BindTableToVariable(
                console_pb2.BindTableToVariableRequest(console_id=self.console_id,
                                                       table_id=table.ticket,
                                                       variable_name=variable_name),
                metadata=self._metadata)
        except Exception as e:
            raise DHError("failed to bind a table to a variable on the server.") from e

    @staticmethod
    def parse_barrage_data(data_header, data_body):
        from barrage.flatbuf import Message
        from barrage.flatbuf.MessageHeader import MessageHeader
        from barrage.flatbuf.BarrageRecordBatch import BarrageRecordBatch
        header_message = Message.GetRootAs(data_header)
        if header_message.HeaderType() != MessageHeader.BarrageRecordBatch:
            return

        header = BarrageRecordBatch()
        header.Init(header_message.Header().Bytes, header_message.Header().Pos)
        print(header.IsSnapshot())
        print(header.NodesLength())

        import pyarrow as pa

        # data = Buffer(data_body)
        data = data_body
        print(type(data))
        for i in range(header.NodesLength()):
            node = header.Nodes(i)
            buffer = header.Buffers(i)
            pa_arr = pa.array(pa.py_buffer(data), pa.int32())
            print(pa_arr)

    def _response_stream_handler(self, response_iterator: Iterator[barrage_pb2.BarrageData]) -> None:
        try:
            for response in response_iterator:
                if response.data_body:
                    self._barrage_finish_event.set()
                    self._barrage_wait_event.set()
                    Session.parse_barrage_data(response.data_header, response.data_body)
                elif response.data_header:
                    self._barrage_wait_event.set()
                else:
                    raise DHError("Invalid Barrage response")

                print("data_header:\n", response.data_header)
                print("data_body:\n", response.data_body)

        except Exception as e:
            self._barrage_finish_event.set()
            raise

    def subscribe_table(self, tbl):
        try:
            bitset = BitArray((len(tbl.cols) // 8 + 1) * 8)
            for i in range(len(tbl.cols)):
                bitset.set(1, -1 - i)

            request = barrage_pb2.SubscriptionRequest(ticket=tbl.ticket,
                                                      columns=bitset.tobytes(),
                                                      # viewport=b'',
                                                      # update_interval_ms=1000,
                                                      # export_id=None,
                                                      # sequence=0,
                                                      # use_deephaven_nulls=True
                                                      )
            barrage_request_iterator = BarrageRequestIterator(request, self._barrage_wait_event,
                                                              self._barrage_finish_event)
            self._barrage_finish_event.clear()
            self._barrage_wait_event.set()
            response_iterator = self.barrage_service.DoSubscribe(barrage_request_iterator, metadata=self._metadata)
            self._executor_future = self._executor.submit(self._response_stream_handler,
                                                          response_iterator)
            self._executor_future.result()
            # for response in responses:
            #     barrage_request_iterator.add_response(response)
            #     print('data_header:\n', response.data_header)
            #     print('')
            #     print('data_body:\n', response.data_body)

        except Exception as e:
            raise DHError('failed to subscribe the table.') from e

    # factory function
    def query(self, tbl):
        return Query(self, tbl)


class BarrageRequestIterator:

    def __init__(self, initial_request, wait_event, finish_event):
        self._lock = threading.Lock()
        self._responses = []
        self._current_req = initial_request
        self._wait_event = wait_event
        self._finish_event = finish_event
        self._req_count = 0

    def __iter__(self):
        return self

    def __next__(self):  # Python 3
        # print("in __next__", self._finish_event.is_set(), self._wait_event.is_set())
        # print(threading.currentThread())
        if self._finish_event.is_set():
            raise StopIteration

        if not self._wait_event.is_set():
            # print("to wait now")
            self._wait_event.wait()
            # print("out of wait")
            if self._finish_event.is_set():
                raise StopIteration
        self._wait_event.clear()
        return self._current_req
