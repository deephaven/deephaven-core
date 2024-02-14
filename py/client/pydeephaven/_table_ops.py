#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Any, Union

import pyarrow as pa

from pydeephaven._arrow import map_arrow_type
from pydeephaven.agg import Aggregation
from pydeephaven.proto import table_pb2, table_pb2_grpc
from pydeephaven.updateby import UpdateByOperation


class SortDirection(Enum):
    """An enum defining the sort ordering."""

    DESCENDING = table_pb2.SortDescriptor.SortDirection.DESCENDING
    """Descending sort direction"""
    ASCENDING = table_pb2.SortDescriptor.SortDirection.ASCENDING
    """Ascending sort direction"""


class TableOp(ABC):
    @classmethod
    @abstractmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        ...

    @abstractmethod
    def make_grpc_request(self, result_id, source_id) -> Any:
        ...

    @abstractmethod
    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        ...


class NoneOp(TableOp):
    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        raise AssertionError("should never be called.")

    def make_grpc_request(self, result_id, source_id) -> Any:
        raise AssertionError("should never be called.")

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        raise AssertionError("should never be called.")

    def __init__(self, table):
        self.table = table


class TimeTableOp(TableOp):
    def __init__(self, start_time: Union[int, str], period: Union[int, str], blink_table: bool = False):
        if start_time is None:
            # Force this to zero to trigger `now()` behavior.
            self.start_time = 0
        else:
            self.start_time = start_time
        self.period = period
        self.blink_table = blink_table

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.TimeTable

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.TimeTableRequest(result_id=result_id,
                                          start_time_nanos=self.start_time if not isinstance(self.start_time,
                                                                                             str) else None,
                                          start_time_string=self.start_time if isinstance(self.start_time,
                                                                                          str) else None,
                                          period_nanos=self.period if not isinstance(self.period, str) else None,
                                          period_string=self.period if isinstance(self.period, str) else None,
                                          blink_table=self.blink_table)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            time_table=self.make_grpc_request(result_id=result_id, source_id=source_id))


class EmptyTableOp(TableOp):
    def __init__(self, size: int):
        self.size = size

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.EmptyTable

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.EmptyTableRequest(result_id=result_id, size=self.size)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            empty_table=self.make_grpc_request(result_id=result_id, source_id=source_id))


class DropColumnsOp(TableOp):
    def __init__(self, column_names: List[str]):
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.DropColumns

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.DropColumnsRequest(result_id=result_id, source_id=source_id,
                                            column_names=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            drop_columns=self.make_grpc_request(result_id=result_id, source_id=source_id))


class USVOp(TableOp):
    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.SelectOrUpdateRequest(result_id=result_id, source_id=source_id,
                                               column_specs=self.column_specs)


class UpdateOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Update

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            update=self.make_grpc_request(result_id=result_id, source_id=source_id))


class LazyUpdateOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.LazyUpdate

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            lazy_update=self.make_grpc_request(result_id=result_id, source_id=source_id))


class ViewOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.View

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            view=self.make_grpc_request(result_id=result_id, source_id=source_id))


class UpdateViewOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.UpdateView

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            update_view=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SelectOp(USVOp):
    def __init__(self, column_specs: List[str]):
        self.column_specs = column_specs

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Select

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            select=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SelectDistinctOp(TableOp):
    def __init__(self, column_names: List[str]):
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.SelectDistinct

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.SelectDistinctRequest(result_id=result_id, source_id=source_id,
                                               column_names=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            select_distinct=self.make_grpc_request(result_id=result_id, source_id=source_id))


class UnstructuredFilterOp(TableOp):
    def __init__(self, filters: List[str]):
        self.filters = filters

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.UnstructuredFilter

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.UnstructuredFilterTableRequest(result_id=result_id,
                                                        source_id=source_id, filters=self.filters)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            unstructured_filter=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SortOp(TableOp):
    def __init__(self, column_names: List[str], directions: List[SortDirection]):
        self.column_names = column_names
        self.directions = directions

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Sort

    def make_grpc_request(self, result_id, source_id) -> Any:
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

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            sort=self.make_grpc_request(result_id=result_id, source_id=source_id))


class HeadOrTailOp(TableOp):
    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.HeadOrTailRequest(result_id=result_id, source_id=source_id,
                                           num_rows=self.num_rows)


class HeadOp(HeadOrTailOp):
    def __init__(self, num_rows: int):
        self.num_rows = num_rows

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Head

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            head=self.make_grpc_request(result_id=result_id, source_id=source_id))


class TailOp(HeadOrTailOp):
    def __init__(self, num_rows: int):
        self.num_rows = num_rows

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Tail

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            tail=self.make_grpc_request(result_id=result_id, source_id=source_id))


class HeadOrTailByOp(TableOp):
    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.HeadOrTailByRequest(result_id=result_id,
                                             source_id=source_id, num_rows=self.num_rows,
                                             group_by_column_specs=self.column_names)


class HeadByOp(HeadOrTailByOp):
    def __init__(self, num_rows: int, column_names: List[str]):
        self.num_rows = num_rows
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.HeadBy

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            head_by=self.make_grpc_request(result_id=result_id, source_id=source_id))


class TailByOp(HeadOrTailByOp):
    def __init__(self, num_rows: int, column_names: List[str]):
        self.num_rows = num_rows
        self.column_names = column_names

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.TailBy

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            tail_by=self.make_grpc_request(result_id=result_id, source_id=source_id))


class UngroupOp(TableOp):
    def __init__(self, column_names: List[str], null_fill: bool = True):
        self.column_names = column_names
        self.null_fill = null_fill

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Ungroup

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.UngroupRequest(result_id=result_id,
                                        source_id=source_id,
                                        null_fill=self.null_fill, columns_to_ungroup=self.column_names)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            ungroup=self.make_grpc_request(result_id=result_id, source_id=source_id))


class MergeTablesOp(TableOp):
    def __init__(self, tables: List[Any], key_column: str = ""):
        self.tables = tables
        self.key_column = key_column

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.MergeTables

    def make_grpc_request(self, result_id, source_id) -> Any:
        table_references = []
        for tbl in self.tables:
            table_references.append(table_pb2.TableReference(ticket=tbl.ticket))

        return table_pb2.MergeTablesRequest(result_id=result_id,
                                            source_ids=table_references,
                                            key_column=self.key_column)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            merge=self.make_grpc_request(result_id=result_id, source_id=source_id))


class NaturalJoinOp(TableOp):
    def __init__(self, table: Any, keys: List[str], columns_to_add: List[str]):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.NaturalJoinTables

    def make_grpc_request(self, result_id, source_id) -> Any:
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.NaturalJoinTablesRequest(result_id=result_id,
                                                  left_id=left_id,
                                                  right_id=right_id,
                                                  columns_to_match=self.keys,
                                                  columns_to_add=self.columns_to_add)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            natural_join=self.make_grpc_request(result_id=result_id, source_id=source_id))


class ExactJoinOp(TableOp):
    def __init__(self, table: Any, keys: List[str], columns_to_add: List[str]):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.ExactJoinTables

    def make_grpc_request(self, result_id, source_id) -> Any:
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.ExactJoinTablesRequest(result_id=result_id,
                                                left_id=left_id,
                                                right_id=right_id,
                                                columns_to_match=self.keys,
                                                columns_to_add=self.columns_to_add)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            exact_join=self.make_grpc_request(result_id=result_id, source_id=source_id))


class CrossJoinOp(TableOp):
    def __init__(self, table: Any, keys: List[str] = [], columns_to_add: List[str] = [], reserve_bits: int = 10):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add
        self.reserve_bits = reserve_bits

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.CrossJoinTables

    def make_grpc_request(self, result_id, source_id) -> Any:
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.CrossJoinTablesRequest(result_id=result_id,
                                                left_id=left_id,
                                                right_id=right_id,
                                                columns_to_match=self.keys,
                                                columns_to_add=self.columns_to_add,
                                                reserve_bits=self.reserve_bits)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            cross_join=self.make_grpc_request(result_id=result_id, source_id=source_id))


class AjOp(TableOp):
    def __init__(self, table: Any, keys: List[str] = [], columns_to_add: List[str] = []):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.AjTables

    def make_grpc_request(self, result_id, source_id) -> Any:
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.AjRajTablesRequest(result_id=result_id,
                                            left_id=left_id,
                                            right_id=right_id,
                                            exact_match_columns=self.keys[:-1],
                                            as_of_column=self.keys[-1],
                                            columns_to_add=self.columns_to_add)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            aj=self.make_grpc_request(result_id=result_id, source_id=source_id))


class RajOp(TableOp):
    def __init__(self, table: Any, keys: List[str] = [], columns_to_add: List[str] = []):
        self.table = table
        self.keys = keys
        self.columns_to_add = columns_to_add

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.RajTables

    def make_grpc_request(self, result_id, source_id) -> Any:
        left_id = source_id
        right_id = table_pb2.TableReference(ticket=self.table.ticket)
        return table_pb2.AjRajTablesRequest(result_id=result_id,
                                            left_id=left_id,
                                            right_id=right_id,
                                            exact_match_columns=self.keys[:-1],
                                            as_of_column=self.keys[-1],
                                            columns_to_add=self.columns_to_add)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            raj=self.make_grpc_request(result_id=result_id, source_id=source_id))


class FlattenOp(TableOp):
    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Flatten

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.FlattenRequest(result_id=result_id, source_id=source_id)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            flatten=self.make_grpc_request(result_id=result_id, source_id=source_id))


class FetchTableOp(TableOp):
    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.FetchTable

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.FetchTableRequest(result_id=result_id, source_id=source_id)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            fetch_table=self.make_grpc_request(result_id=result_id, source_id=source_id))


class UpdateByOp(TableOp):
    def __init__(self, operations: List[UpdateByOperation], by: List[str]):
        self.operations = operations
        self.by = by

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.UpdateBy

    def make_grpc_request(self, result_id, source_id) -> Any:
        operations = [op.make_grpc_message() for op in self.operations]
        return table_pb2.UpdateByRequest(result_id=result_id, source_id=source_id, operations=operations,
                                         group_by_columns=self.by)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            update_by=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SnapshotTableOp(TableOp):
    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Snapshot

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.SnapshotTableRequest(result_id=result_id, source_id=source_id)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            snapshot=self.make_grpc_request(result_id=result_id, source_id=source_id))


class SnapshotWhenTableOp(TableOp):

    def __init__(self, trigger_table: Any, stamp_cols: List[str] = None, initial: bool = False,
                 incremental: bool = False, history: bool = False):
        self.trigger_table = trigger_table
        self.stamp_cols = stamp_cols
        self.initial = initial
        self.incremental = incremental
        self.history = history

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.SnapshotWhen

    def make_grpc_request(self, result_id, source_id) -> Any:
        base_id = source_id
        trigger_id = table_pb2.TableReference(ticket=self.trigger_table.ticket)
        return table_pb2.SnapshotWhenTableRequest(result_id=result_id,
                                                  base_id=base_id,
                                                  trigger_id=trigger_id,
                                                  initial=self.initial,
                                                  incremental=self.incremental,
                                                  history=self.history,
                                                  stamp_columns=self.stamp_cols)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            snapshot_when=self.make_grpc_request(result_id=result_id, source_id=source_id))


class AggregateOp(TableOp):
    def __init__(self, aggs: List[Aggregation], by: List[str]):
        self.aggs = aggs
        self.by = by

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.Aggregate

    def make_grpc_request(self, result_id, source_id) -> Any:
        aggregations = [agg.make_grpc_message() for agg in self.aggs]
        return table_pb2.AggregateRequest(result_id=result_id, source_id=source_id, aggregations=aggregations,
                                          group_by_columns=self.by)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            aggregate=self.make_grpc_request(result_id=result_id, source_id=source_id))


class AggregateAllOp(TableOp):
    def __init__(self, agg: Aggregation, by: List[str]):
        self.agg_spec = agg.agg_spec
        self.by = by

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.AggregateAll

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.AggregateAllRequest(result_id=result_id, source_id=source_id, spec=self.agg_spec,
                                             group_by_columns=self.by)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            aggregate_all=self.make_grpc_request(result_id=result_id, source_id=source_id))


class CreateInputTableOp(TableOp):
    def __init__(self, schema: pa.schema, init_table: Any, key_cols: List[str] = None):
        self.schema = schema
        self.init_table = init_table
        self.key_cols = key_cols

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.CreateInputTable

    def make_grpc_request(self, result_id, source_id) -> Any:
        if self.key_cols:
            key_backed = table_pb2.CreateInputTableRequest.InputTableKind.InMemoryKeyBacked(
                key_columns=self.key_cols)
            input_table_kind = table_pb2.CreateInputTableRequest.InputTableKind(in_memory_key_backed=key_backed)
        else:
            append_only = table_pb2.CreateInputTableRequest.InputTableKind.InMemoryAppendOnly()
            input_table_kind = table_pb2.CreateInputTableRequest.InputTableKind(in_memory_append_only=append_only)

        if self.schema:
            dh_fields = []
            for f in self.schema:
                dh_fields.append(pa.field(name=f.name, type=f.type, metadata=map_arrow_type(f.type)))
            dh_schema = pa.schema(dh_fields)

            schema = dh_schema.serialize().to_pybytes()
            return table_pb2.CreateInputTableRequest(result_id=result_id,
                                                     schema=schema,
                                                     kind=input_table_kind)
        else:
            source_table_id = table_pb2.TableReference(ticket=self.init_table.ticket)
            return table_pb2.CreateInputTableRequest(result_id=result_id,
                                                     source_table_id=source_table_id,
                                                     kind=input_table_kind)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            create_input_table=self.make_grpc_request(result_id=result_id, source_id=source_id))


class WhereInTableOp(TableOp):

    def __init__(self, filter_table: Any, cols: List[str], inverted: bool):
        self.filter_table = filter_table
        self.cols = cols
        self.inverted = inverted

    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.WhereIn

    def make_grpc_request(self, result_id, source_id) -> Any:
        right_id = table_pb2.TableReference(ticket=self.filter_table.ticket)
        return table_pb2.WhereInRequest(result_id=result_id,
                                        left_id=source_id,
                                        right_id=right_id,
                                        inverted=self.inverted,
                                        columns_to_match=self.cols)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            where_in=self.make_grpc_request(result_id=result_id, source_id=source_id))


class MetaTableOp(TableOp):
    @classmethod
    def get_stub_func(cls, table_service_stub: table_pb2_grpc.TableServiceStub) -> Any:
        return table_service_stub.MetaTable

    def make_grpc_request(self, result_id, source_id) -> Any:
        return table_pb2.MetaTableRequest(result_id=result_id, source_id=source_id)

    def make_grpc_request_for_batch(self, result_id, source_id) -> Any:
        return table_pb2.BatchTableRequest.Operation(
            meta_table=self.make_grpc_request(result_id=result_id, source_id=source_id))
