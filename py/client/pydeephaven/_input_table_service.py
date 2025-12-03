#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from typing import TYPE_CHECKING

from deephaven_core.proto import inputtable_pb2, inputtable_pb2_grpc
from pydeephaven.dherror import DHError
from pydeephaven.table import InputTable, Table

if TYPE_CHECKING:
    from pydeephaven.session import Session


class InputTableService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self._grpc_input_table_stub = inputtable_pb2_grpc.InputTableServiceStub(
            session.grpc_channel
        )

    def add(self, input_table: InputTable, table: Table) -> None:
        """Adds a table to the InputTable."""
        try:
            self.session.wrap_rpc(
                self._grpc_input_table_stub.AddTableToInputTable,
                inputtable_pb2.AddTableRequest(
                    input_table=input_table.pb_ticket, table_to_add=table.pb_ticket
                ),
            )
        except Exception as e:
            raise DHError("failed to add to InputTable") from e

    def delete(self, input_table: InputTable, table: Table) -> None:
        """Deletes a table from an InputTable."""
        try:
            self.session.wrap_rpc(
                self._grpc_input_table_stub.DeleteTableFromInputTable,
                inputtable_pb2.DeleteTableRequest(
                    input_table=input_table.pb_ticket, table_to_remove=table.pb_ticket
                ),
            )
        except Exception as e:
            raise DHError("failed to delete from InputTable") from e
