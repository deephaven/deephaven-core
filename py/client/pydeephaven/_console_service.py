#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from pydeephaven.dherror import DHError
from pydeephaven.proto import console_pb2_grpc, console_pb2
from pydeephaven.table import Table


class ConsoleService:
    def __init__(self, session):
        self.session = session
        self._grpc_console_stub = console_pb2_grpc.ConsoleServiceStub(session.grpc_channel)
        self.console_id = None

    def start_console(self):
        if self.console_id:
            return

        try:
            result_id = self.session.make_ticket()
            response = self._grpc_console_stub.StartConsole(
                console_pb2.StartConsoleRequest(result_id=result_id, session_type=self.session._session_type),
                metadata=self.session.grpc_metadata)
            self.console_id = response.result_id
        except Exception as e:
            raise DHError("failed to start a console.") from e

    def run_script(self, server_script):
        self.start_console()

        try:
            response = self._grpc_console_stub.ExecuteCommand(
                console_pb2.ExecuteCommandRequest(
                    console_id=self.console_id,
                    code=server_script),
                metadata=self.session.grpc_metadata)
            return response
        except Exception as e:
            raise DHError("failed to execute a command in the console.") from e

    def open_table(self, name):
        self.start_console()

        try:
            result_id = self.session.make_ticket()
            response = self._grpc_console_stub.FetchTable(
                console_pb2.FetchTableRequest(console_id=self.console_id,
                                              table_id=result_id,
                                              table_name=name),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error open a table: " + response.error_info)
        except Exception as e:
            raise DHError("failed to open a table.") from e

    def bind_table(self, table, variable_name):
        if not table or not variable_name:
            raise DHError("invalid table and/or variable_name values.")
        try:
            response = self._grpc_console_stub.BindTableToVariable(
                console_pb2.BindTableToVariableRequest(console_id=self.console_id,
                                                       table_id=table.ticket,
                                                       variable_name=variable_name),
                metadata=self.session.grpc_metadata)
        except Exception as e:
            raise DHError("failed to bind a table to a variable on the server.") from e
