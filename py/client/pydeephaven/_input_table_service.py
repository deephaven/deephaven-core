#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from pydeephaven.table import Table
from pydeephaven.dherror import DHError
from pydeephaven.proto import inputtable_pb2, inputtable_pb2_grpc
from pydeephaven.table import InputTable


class InputTableService:
    def __init__(self, session):
        self.session = session
        self._grpc_input_table_stub = inputtable_pb2_grpc.InputTableServiceStub(session.grpc_channel)

    def add(self, input_table: InputTable, table: Table):
        """Adds a table to the InputTable."""
        try:
            response = self._grpc_input_table_stub.AddTableToInputTable(
                inputtable_pb2.AddTableRequest(input_table=input_table.ticket,
                                               table_to_add=table.ticket),
                metadata=self.session.grpc_metadata)
        except Exception as e:
            raise DHError("failed to add to InputTable") from e

    def delete(self, input_table: InputTable, table: Table):
        """Deletes a table from an InputTable."""
        try:
            response = self._grpc_input_table_stub.DeleteTableFromInputTable(
                inputtable_pb2.DeleteTableRequest(input_table=input_table.ticket,
                                                  table_to_remove=table.ticket),
                metadata=self.session.grpc_metadata)
        except Exception as e:
            raise DHError("failed to delete from InputTable") from e
