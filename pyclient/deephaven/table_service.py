from deephaven.batch_assembler import BatchOpAssembler
from deephaven.dherror import DHError
from deephaven.proto import table_pb2_grpc, table_pb2
from deephaven.table import TimeTable, EmptyTable, Table


class TableService:
    def __init__(self, session):
        self.session = session
        self._grpc_table_stub = table_pb2_grpc.TableServiceStub(session.grpc_channel)

    def time_table(self, start_time=0, period=1000000000):
        try:
            result_id = self.session.make_flight_ticket()
            response = self._grpc_table_stub.TimeTable(
                table_pb2.TimeTableRequest(result_id=result_id, start_time_nanos=start_time, period_nanos=period),
                metadata=self.session.grpc_metadata)

            if response.success:
                return TimeTable(self.session, ticket=response.result_id.ticket,
                                 schema_header=response.schema_header,
                                 start_time=start_time,
                                 period=period,
                                 is_static=response.is_static)
            else:
                raise DHError("error encountered in creating a time table: " + response.error_info)
        except Exception as e:
            raise DHError("failed to create a time table.") from e

    def empty_table(self, size=0):
        try:
            result_id = self.session.make_flight_ticket()
            response = self._grpc_table_stub.EmptyTable(
                table_pb2.EmptyTableRequest(result_id=result_id, size=size),
                metadata=self.session.grpc_metadata)

            if response.success:
                return EmptyTable(self.session, ticket=response.result_id.ticket,
                                  schema_header=response.schema_header,
                                  size=response.size,
                                  is_static=response.is_static)
            else:
                raise DHError("error encountered in creating an empty table: " + response.error_info)
        except Exception as e:
            raise DHError("failed to create an empty table.") from e

    def update_table(self, table, column_specs=[]):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self._grpc_table_stub.Update(
                table_pb2.SelectOrUpdateRequest(result_id=result_id,
                                                source_id=table_reference, column_specs=column_specs),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in table update: " + response.error_info)
        except Exception as e:
            raise DHError("failed to update the table.") from e

    def batch(self, dag):
        batch_assembler = BatchOpAssembler(self.session)
        dag.accept(batch_assembler)

        try:
            response = self._grpc_table_stub.Batch(
                table_pb2.BatchTableRequest(ops=batch_assembler.batch),
                metadata=self.session.grpc_metadata)

            exported_tables = []
            for exported in response:
                exported_tables.append(Table(self.session, ticket=exported.result_id.ticket,
                                             schema_header=exported.schema_header,
                                             size=exported.size,
                                             is_static=exported.is_static))
            return exported_tables[-1]
        except Exception as e:
            raise DHError("failed to finish the table batch operation.") from e
