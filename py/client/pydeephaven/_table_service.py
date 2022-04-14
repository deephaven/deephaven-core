#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from pydeephaven._batch_assembler import BatchOpAssembler
from pydeephaven._table_ops import TableOp
from pydeephaven.dherror import DHError
from pydeephaven.proto import table_pb2_grpc, table_pb2
from pydeephaven.table import Table


class TableService:
    def __init__(self, session):
        self.session = session
        self._grpc_table_stub = table_pb2_grpc.TableServiceStub(session.grpc_channel)

    def batch(self, ops):
        batch_op = BatchOpAssembler(self.session, table_ops=ops).build_batch()

        try:
            response = self._grpc_table_stub.Batch(
                table_pb2.BatchTableRequest(ops=batch_op),
                metadata=self.session.grpc_metadata)

            exported_tables = []
            for exported in response:
                if not exported.success:
                    raise DHError(exported.error_info)
                if exported.result_id.WhichOneof("ref") == "ticket":
                    exported_tables.append(Table(self.session, ticket=exported.result_id.ticket,
                                                 schema_header=exported.schema_header,
                                                 size=exported.size,
                                                 is_static=exported.is_static))
            return exported_tables[-1]
        except Exception as e:
            raise DHError("failed to finish the table batch operation.") from e

    def grpc_table_op(self, table: Table, op: TableOp):
        try:
            result_id = self.session.make_ticket()
            if table:
                table_reference = table_pb2.TableReference(ticket=table.ticket)
            else:
                table_reference = None
            stub_func = op.__class__.get_stub_func(self._grpc_table_stub)
            response = stub_func(op.make_grpc_request(result_id=result_id, source_id=table_reference),
                                 metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError(f"Server error received for {op.__class__.__name__}: {response.error_info}")
        except Exception as e:
            raise DHError(f"failed to finish {op.__class__.__name__} operation") from e
