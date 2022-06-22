#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from abc import ABC
from typing import List, Any

from pydeephaven.combo_agg import ComboAggregation
from pydeephaven.constants import SortDirection, MatchRule
from pydeephaven._constants import AggType
from pydeephaven.proto import table_pb2, table_pb2_grpc


class TableOp(ABC):
    def make_grpc_request(self, result_id, source_id=None):
        ...

    def make_grpc_request_for_batch(self, result_id, source_id):
        ...


class NoneOp(TableOp):
    def __init__(self, table):
        self.table = table


class TimeTableOp(TableOp):
    def __init__(self, start_time: int = 0, period: int = 1000000000):
        self.start_time = start_time
        self.period = period

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.TimeTable

    def make_grpc_request(self, result_id, source_id=None):
        return table_pb2.TimeTableRequest(result_id=result_id, start_time_nanos=self.start_time,
                                          period_nanos=self.period)

    def make_grpc_request_for_batch(self, result_id, source_id=None):
        return table_pb2.BatchTableRequest.Operation(
            time_table=self.make_grpc_request(result_id=result_id, source_id=source_id))


class EmptyTableOp(TableOp):
    def __init__(self, size: int):
        self.size = size

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.EmptyTable

    def make_grpc_request(self, result_id, source_id=None):
        return table_pb2.EmptyTableRequest(result_id=result_id, size=self.size)

    def make_grpc_request_for_batch(self, result_id, source_id=None):
        return table_pb2.BatchTableRequest.Operation(
            empty_table=self.make_grpc_request(result_id=result_id, source_id=source_id))


class DropColumnsOp(TableOp):
    def __init__(self, column_names: List[str]):
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.DropColumns

    def make_grpc_request(self, result_id, source_id):
        return table_pb2.DropColumnsRequest(result_id=result_id, source_id=source_id,
                                            column_names=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            drop_columns=self.make_grpc_request(result_id=result_id, source_id=source_id))


class USVOp(TableOp):
    def make_grpc_request(self, result_id, source_id):
        return table_pb2.SelectOrUpdateRequest(result_id=result_id, source_id=source_id,
                                               column_specs=self.column_specs)


class UpdateOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.Update

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            update=self.make_grpc_request(result_id=result_id, source_id=source_id))


class LazyUpdateOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.LazyUpdate

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            lazy_update=self.make_grpc_request(result_id=result_id, source_id=source_id))


class ViewOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.View

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            view=self.make_grpc_request(result_id=result_id, source_id=source_id))


class UpdateViewOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.UpdateView

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            update_view=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SelectOp(USVOp):
    def __init__(self, column_specs: List[str] = []):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.Select

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            select=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SelectDistinctOp(TableOp):
    def __init__(self, column_names: List[str] = []):
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.SelectDistinct

    def make_grpc_request(self, result_id, source_id):
        return table_pb2.SelectDistinctRequest(result_id=result_id, source_id=source_id,
                                               column_names=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            select_distinct=self.make_grpc_request(result_id=result_id, source_id=source_id))


class UnstructuredFilterOp(TableOp):
    def __init__(self, filters: List[str]):
        self.filters = filters

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.UnstructuredFilter

    def make_grpc_request(self, result_id, source_id):
        return table_pb2.UnstructuredFilterTableRequest(result_id=result_id,
                                                        source_id=source_id, filters=self.filters)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            unstructured_filter=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SortOp(TableOp):
    def __init__(self, column_names: List[str], directions: List[SortDirection]):
        self.column_names = column_names
        self.directions = directions

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.Sort

    def make_grpc_request(self, result_id, source_id):
        from itertools import zip_longest
        sort_specs = zip_longest(self.column_names, self.directions)
        sort_descriptors = []
        for sp in sort_specs:
            if not sp[0]:
                break
            direction = sp[1] if sp[1] else SortDirection.ASCENDING
            sort_descriptor = table_pb2.SortDescriptor(column_name=sp[0],
                                                       direction=direction.value)
            sort_descriptors.append(sort_descriptor)
        return table_pb2.SortTableRequest(result_id=result_id,
                                          source_id=source_id, sorts=sort_descriptors)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            sort=self.make_grpc_request(result_id=result_id, source_id=source_id))


class HeadOrTailOp(TableOp):
    def make_grpc_request(self, result_id, source_id):
        return table_pb2.HeadOrTailRequest(result_id=result_id, source_id=source_id,
                                           num_rows=self.num_rows)


class HeadOp(HeadOrTailOp):
    def __init__(self, num_rows: int):
        self.num_rows = num_rows

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.Head

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            head=self.make_grpc_request(result_id=result_id, source_id=source_id))


class TailOp(HeadOrTailOp):
    def __init__(self, num_rows: int):
        self.num_rows = num_rows

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.Tail

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            tail=self.make_grpc_request(result_id=result_id, source_id=source_id))


class HeadOrTailByOp(TableOp):
    def make_grpc_request(self, result_id, source_id):
        return table_pb2.HeadOrTailByRequest(result_id=result_id,
                                             source_id=source_id, num_rows=self.num_rows,
                                             group_by_column_specs=self.column_names)


class HeadByOp(HeadOrTailByOp):
    def __init__(self, num_rows: int, column_names: List[str]):
        self.num_rows = num_rows
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.HeadBy

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            head_by=self.make_grpc_request(result_id=result_id, source_id=source_id))


class TailByOp(HeadOrTailByOp):
    def __init__(self, num_rows: int, column_names: List[str]):
        self.num_rows = num_rows
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.TailBy

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            tail_by=self.make_grpc_request(result_id=result_id, source_id=source_id))


class UngroupOp(TableOp):
    def __init__(self, column_names: List[str], null_fill: bool = True):
        self.column_names = column_names
        self.null_fill = null_fill

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.Ungroup

    def make_grpc_request(self, result_id, source_id):
        return table_pb2.UngroupRequest(result_id=result_id,
                                        source_id=source_id,
                                        null_fill=self.null_fill, columns_to_ungroup=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            ungroup=self.make_grpc_request(result_id=result_id, source_id=source_id))


class MergeTablesOp(TableOp):
    def __init__(self, tables: List[Any], key_column: str = ""):
        self.tables = tables
        self.key_column = key_column

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.MergeTables

    def make_grpc_request(self, result_id, source_id):
        table_references = []
        for tbl in self.tables:
            table_references.append(table_pb2.TableReference(ticket=tbl.ticket))

        return table_pb2.MergeTablesRequest(result_id=result_id,
                                            source_ids=table_references,
                                            key_column=self.key_column)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            merge=self.make_grpc_request(result_id=result_id, source_id=source_id))


class NaturalJoinOp(TableOp):
    def __init__(self, table: Any, keys: List[str], columns_to_add: List[str] = []):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.NaturalJoinTables

    def make_grpc_request(self, result_id, source_id):
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.NaturalJoinTablesRequest(result_id=result_id,
                                                  left_id=left_id,
                                                  right_id=right_id,
                                                  columns_to_match=self.keys,
                                                  columns_to_add=self.columns_to_add)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            natural_join=self.make_grpc_request(result_id=result_id, source_id=source_id))


class ExactJoinOp(TableOp):
    def __init__(self, table: Any, keys: List[str], columns_to_add: List[str] = []):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.ExactJoinTables

    def make_grpc_request(self, result_id, source_id):
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.ExactJoinTablesRequest(result_id=result_id,
                                                left_id=left_id,
                                                right_id=right_id,
                                                columns_to_match=self.keys,
                                                columns_to_add=self.columns_to_add)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            exact_join=self.make_grpc_request(result_id=result_id, source_id=source_id))


class CrossJoinOp(TableOp):
    def __init__(self, table: Any, keys: List[str] = [], columns_to_add: List[str] = [], reserve_bits: int = 10):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add
        self.reserve_bits = reserve_bits

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.CrossJoinTables

    def make_grpc_request(self, result_id, source_id):
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.CrossJoinTablesRequest(result_id=result_id,
                                                left_id=left_id,
                                                right_id=right_id,
                                                columns_to_match=self.keys,
                                                columns_to_add=self.columns_to_add,
                                                reserve_bits=self.reserve_bits)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            cross_join=self.make_grpc_request(result_id=result_id, source_id=source_id))


class AsOfJoinOp(TableOp):
    def __init__(self, table: Any, keys: List[str] = [], columns_to_add: List[str] = [],
                 match_rule: MatchRule = MatchRule.LESS_THAN_EQUAL):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add
        self.match_rule = match_rule

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.AsOfJoinTables

    def make_grpc_request(self, result_id, source_id):
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.AsOfJoinTablesRequest(result_id=result_id,
                                               left_id=left_id,
                                               right_id=right_id,
                                               columns_to_match=self.keys,
                                               columns_to_add=self.columns_to_add,
                                               as_of_match_rule=self.match_rule.value)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            as_of_join=self.make_grpc_request(result_id=result_id, source_id=source_id))


class FlattenOp(TableOp):
    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.Flatten

    def make_grpc_request(self, result_id, source_id):
        return table_pb2.FlattenRequest(result_id=result_id, source_id=source_id)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            flatten=self.make_grpc_request(result_id=result_id, source_id=source_id))


class DedicatedAggOp(TableOp):
    def __init__(self, agg_type: AggType, column_names: List[str] = [], count_column: str = None):
        self.agg_type = agg_type
        self.column_names = column_names
        self.count_column = count_column

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.ComboAggregate

    def make_grpc_request(self, result_id, source_id):
        aggregates = []
        if self.agg_type == AggType.COUNT and self.count_column:
            agg = table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value, column_name=self.count_column)
        else:
            agg = table_pb2.ComboAggregateRequest.Aggregate(type=self.agg_type.value)
        aggregates.append(agg)

        return table_pb2.ComboAggregateRequest(result_id=result_id,
                                               source_id=source_id,
                                               aggregates=aggregates,
                                               group_by_columns=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            combo_aggregate=self.make_grpc_request(result_id=result_id, source_id=source_id))


class ComboAggOp(TableOp):
    def __init__(self, column_names: List[str], combo_aggregation: ComboAggregation):
        self.column_names = column_names
        self.combo_aggregation = combo_aggregation

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.ComboAggregate

    def make_grpc_request(self, result_id, source_id):
        aggregates = []
        for agg in self.combo_aggregation.aggregates:
            aggregates.append(agg.make_grpc_request())

        return table_pb2.ComboAggregateRequest(result_id=result_id,
                                               source_id=source_id,
                                               aggregates=aggregates,
                                               group_by_columns=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            combo_aggregate=self.make_grpc_request(result_id=result_id, source_id=source_id))


class FetchTableOp(TableOp):
    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub):
        return table_service_stub.FetchTable

    def make_grpc_request(self, result_id, source_id):
        return table_pb2.FetchTableRequest(result_id=result_id, source_id=source_id)

    def make_grpc_request_for_batch(self, result_id, source_id):
        return table_pb2.BatchTableRequest.Operation(
            empty_table=self.make_grpc_request(result_id=result_id, source_id=source_id))
