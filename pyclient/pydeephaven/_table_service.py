#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from pydeephaven.constants import MatchRule
from pydeephaven.constants import AggType
from pydeephaven._batch_assembler import BatchOpAssembler
from pydeephaven.dherror import DHError
from pydeephaven.proto import table_pb2_grpc, table_pb2
from pydeephaven.table import TimeTable, EmptyTable, Table


class TableService:
    def __init__(self, session):
        self.session = session
        self._grpc_table_stub = table_pb2_grpc.TableServiceStub(session.grpc_channel)
        self.usv_func_dict = {
            "Update": self._grpc_table_stub.Update,
            "LazyUpdate": self._grpc_table_stub.LazyUpdate,
            "View": self._grpc_table_stub.View,
            "UpdateView": self._grpc_table_stub.UpdateView,
            "Select": self._grpc_table_stub.Select
        }

        self.nel_join_dict = {"natural": (self._grpc_table_stub.NaturalJoinTables, table_pb2.NaturalJoinTablesRequest),
                              "exact": (self._grpc_table_stub.ExactJoinTables, table_pb2.ExactJoinTablesRequest),
                              "left": (self._grpc_table_stub.LeftJoinTables, table_pb2.LeftJoinTablesRequest)}

    def batch(self, ops):
        batch_assembler = BatchOpAssembler(self.session)
        ops.accept(batch_assembler)

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

    def drop_columns(self, table, column_names):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self._grpc_table_stub.DropColumns(
                table_pb2.DropColumnsRequest(result_id=result_id,
                                             source_id=table_reference, column_names=column_names),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in dropping columns: " + response.error_info)
        except Exception as e:
            raise DHError("failed to drop the columns.") from e

    def usv_table(self, table, command, column_specs=[]):
        error_message = "Server error received for " + command
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            usv_func = self.usv_func_dict[command]
            response = usv_func(
                table_pb2.SelectOrUpdateRequest(result_id=result_id,
                                                source_id=table_reference, column_specs=column_specs),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError(error_message + response.error_info)
        except Exception as e:
            raise DHError("failed to finish table " + command + "operation") from e

    def select_distinct(self, table, column_names=[]):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self._grpc_table_stub.SelectDistinct(
                table_pb2.SelectDistinctRequest(result_id=result_id,
                                                source_id=table_reference, column_names=column_names),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in select-distinct operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish select-distinct operation") from e

    def unstructured_filter(self, table, filters=[]):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self._grpc_table_stub.UnstructuredFilter(
                table_pb2.UnstructuredFilterTableRequest(result_id=result_id,
                                                         source_id=table_reference, filters=filters),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in unstructured filter operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish unstructured filter operation.") from e

    def sort(self, table, column_names=[], directions=[]):
        from itertools import zip_longest
        sort_specs = zip_longest(column_names, directions)
        sort_descriptors = []
        for sp in sort_specs:
            sort_descriptor = table_pb2.SortDescriptor(column_name=sp[0], direction=-1 if sp[1] == "DESC" else 1)
            sort_descriptors.append(sort_descriptor)
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self._grpc_table_stub.Sort(
                table_pb2.SortTableRequest(result_id=result_id,
                                           source_id=table_reference, sorts=sort_descriptors),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in sorting: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish sort operation.") from e

    def head_or_tail(self, table, command, num_rows=10):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            ht_req = table_pb2.HeadOrTailRequest(result_id=result_id,
                                                 source_id=table_reference, num_rows=num_rows)
            if command == "Head":
                response = self._grpc_table_stub.Head(ht_req,
                                                      metadata=self.session.grpc_metadata)
            else:
                response = self._grpc_table_stub.Tail(ht_req,
                                                      metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in Head/Tail: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish Head/Tail operation.") from e

    def head_or_tail_by(self, table, command, num_rows=10, column_names=[]):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            ht_req = table_pb2.HeadOrTailByRequest(result_id=result_id,
                                                   source_id=table_reference, num_rows=num_rows,
                                                   group_by_column_specs=column_names)
            if command == "HeadBy":
                response = self._grpc_table_stub.HeadBy(ht_req,
                                                        metadata=self.session.grpc_metadata)
            else:
                response = self._grpc_table_stub.TailBy(ht_req,
                                                        metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in HeadBy/TailBy: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish HeadBy/TailBy operation.") from e

    def group_by(self, table, group_by_columns=[]):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            aggregates = []
            agg = table_pb2.ComboAggregateRequest.Aggregate(type=table_pb2.ComboAggregateRequest.AggType.ARRAY)
            aggregates.append(agg)
            response = self._grpc_table_stub.ComboAggregate(
                table_pb2.ComboAggregateRequest(result_id=result_id,
                                                source_id=table_reference,
                                                aggregates=aggregates,
                                                group_by_columns=group_by_columns),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in group-by operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish group-by operation") from e

    def ungroup(self, table, null_fill=True, columns_to_ungroup=[]):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self._grpc_table_stub.Ungroup(
                table_pb2.UngroupRequest(result_id=result_id,
                                         source_id=table_reference,
                                         null_fill=null_fill, columns_to_ungroup=columns_to_ungroup),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in ungroup operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish ungroup operation") from e

    def merge(self, tables=[], key_column=None):
        try:
            result_id = self.session.make_flight_ticket()
            table_references = []
            for tbl in tables:
                table_references.append(table_pb2.TableReference(ticket=tbl.ticket))

            response = self._grpc_table_stub.MergeTables(
                table_pb2.MergeTablesRequest(result_id=result_id,
                                             source_ids=table_references,
                                             key_column=key_column),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in merge table operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish merge table operation") from e

    def nel_join(self, left_table, right_table, join_type, columns_to_match=[], columns_to_add=[]):
        try:
            result_id = self.session.make_flight_ticket()
            left_id = table_pb2.TableReference(ticket=left_table.ticket)
            right_id = table_pb2.TableReference(ticket=right_table.ticket)
            join_method, join_request = self.nel_join_dict[join_type]
            response = join_method(
                join_request(result_id=result_id,
                             left_id=left_id,
                             right_id=right_id,
                             columns_to_match=columns_to_match,
                             columns_to_add=columns_to_add),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError(f"error encountered in {join_type}-join operation: " + response.error_info)
        except Exception as e:
            raise DHError(f"failed to finish {join_type}-join operation") from e

    def cross_join(self, left_table, right_table, columns_to_match=[], columns_to_add=[], reserve_bits=10):
        try:
            result_id = self.session.make_flight_ticket()
            left_id = table_pb2.TableReference(ticket=left_table.ticket)
            right_id = table_pb2.TableReference(ticket=right_table.ticket)
            response = self._grpc_table_stub.CrossJoinTables(
                table_pb2.CrossJoinTablesRequest(result_id=result_id,
                                                 left_id=left_id,
                                                 right_id=right_id,
                                                 columns_to_match=columns_to_match,
                                                 columns_to_add=columns_to_add,
                                                 reserve_bits=reserve_bits),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in join operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish join operation") from e

    def flatten(self, table):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            response = self._grpc_table_stub.Flatten(
                table_pb2.FlattenRequest(result_id=result_id,
                                         source_id=table_reference),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in flatten operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish flatten operation") from e

    def as_of_join(self, left_table, right_table, columns_to_match=[], columns_to_add=[],
                   match_rule=MatchRule.LESS_THAN_EQUAL):
        try:
            result_id = self.session.make_flight_ticket()
            left_id = table_pb2.TableReference(ticket=left_table.ticket)
            right_id = table_pb2.TableReference(ticket=right_table.ticket)
            response = self._grpc_table_stub.AsOfJoinTables(
                table_pb2.AsOfJoinTablesRequest(result_id=result_id,
                                                left_id=left_id,
                                                right_id=right_id,
                                                columns_to_match=columns_to_match,
                                                columns_to_add=columns_to_add,
                                                as_of_match_rule=match_rule.value),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError("error encountered in as-of join operation: " + response.error_info)
        except Exception as e:
            raise DHError("failed to finish as-of join operation") from e

    def dedicated_aggregator(self, table, agg_type, group_by_columns=[], count_column=None):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            aggregates = []
            if agg_type == AggType.COUNT and count_column:
                agg = table_pb2.ComboAggregateRequest.Aggregate(type=agg_type.value, column_name=count_column)
            else:
                agg = table_pb2.ComboAggregateRequest.Aggregate(type=agg_type.value)
            aggregates.append(agg)

            response = self._grpc_table_stub.ComboAggregate(
                table_pb2.ComboAggregateRequest(result_id=result_id,
                                                source_id=table_reference,
                                                aggregates=aggregates,
                                                group_by_columns=group_by_columns),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError(f"error encountered in {agg_type} operation: " + response.error_info)
        except Exception as e:
            raise DHError(f"failed to finish {agg_type} operation") from e

    def combo_aggregate(self, table, group_by_columns=[], agg_list=[]):
        try:
            result_id = self.session.make_flight_ticket()
            table_reference = table_pb2.TableReference(ticket=table.ticket)
            aggregates = []
            for agg in agg_list:
                aggregates.append(agg.build_grpc_request())

            response = self._grpc_table_stub.ComboAggregate(
                table_pb2.ComboAggregateRequest(result_id=result_id,
                                                source_id=table_reference,
                                                aggregates=aggregates,
                                                group_by_columns=group_by_columns),
                metadata=self.session.grpc_metadata)

            if response.success:
                return Table(self.session, ticket=response.result_id.ticket,
                             schema_header=response.schema_header,
                             size=response.size,
                             is_static=response.is_static)
            else:
                raise DHError(f"error encountered in combo-agg operation: " + response.error_info)
        except Exception as e:
            raise DHError(f"failed to finish combo-agg operation") from e
