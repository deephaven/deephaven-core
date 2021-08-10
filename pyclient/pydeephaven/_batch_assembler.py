#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from pydeephaven.proto import table_pb2
from pydeephaven._query_ops import *


class BatchOpAssembler:
    def __init__(self, session=None):
        self.session = session
        self.ops = []
        self._current_table_ref = None

    @property
    def batch(self):
        return self.ops

    def _make_result_id(self, op):
        if op.child:
            return self.session.make_flight_ticket()
        return None

    def _set_last_op(self, op):
        self._last_op = op

    def visit(self, op):
        if isinstance(op, NoneOp):
            self._current_table_ref = table_pb2.TableReference(ticket=op.table.ticket)
        elif isinstance(op, UpdateOp):
            self.build_update(op)
        elif isinstance(op, TailOp):
            self.build_tail()
        elif isinstance(op, HeadOp):
            self.build_head()

    def build_update(self, op):
        table_ref = self._current_table_ref
        result_id = self._make_result_id(op)
        req = table_pb2.SelectOrUpdateRequest(result_id=result_id, source_id=table_ref,
                                              column_specs=op.column_specs)
        self.ops.append(table_pb2.BatchTableRequest.Operation(update=req))
        self._current_table_ref = table_pb2.TableReference(batch_offset=len(self.ops) - 1)

    def build_tail(self, op):
        table_ref = self._current_table_ref
        result_id = self._make_result_id(op)
        req = table_pb2.HeadOrTailRequest(result_id=result_id, source_id=table_ref, num_rows=op.num_rows)
        self.ops.append(table_pb2.BatchTableRequest.Operation(tail=req))
        self._current_table_ref = table_pb2.TableReference(batch_offset=len(self.ops) - 1)

    def build_head(self, op):
        table_ref = self._current_table_ref
        result_id = self._make_result_id(op)
        req = table_pb2.HeadOrTailRequest(result_id=result_id, source_id=table_ref, num_rows=op.num_rows)
        self.ops.append(table_pb2.BatchTableRequest.Operation(head=req))
        self._current_table_ref = table_pb2.TableReference(batch_offset=len(self.ops) - 1)
