#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

from pydeephaven._table_ops import *


class BatchOpAssembler:
    def __init__(self, session, table_ops: List[TableOp]):
        self.session = session
        self.table_ops = table_ops
        self.grpc_table_ops = []
        self._curr_source = None

    @property
    def batch(self):
        return self.grpc_table_ops

    def build_batch(self):
        self._curr_source = table_pb2.TableReference(ticket=self.table_ops[0].table.ticket)

        for table_op in self.table_ops[1:-1]:
            result_id = None
            self.grpc_table_ops.append(
                table_op.make_grpc_request_for_batch(result_id=result_id, source_id=self._curr_source))
            self._curr_source = table_pb2.TableReference(batch_offset=len(self.grpc_table_ops) - 1)

        # the last op in the batch needs a result_id to reference the result
        result_id = self.session.make_ticket()
        self.grpc_table_ops.append(
            self.table_ops[-1].make_grpc_request_for_batch(result_id=result_id, source_id=self._curr_source))

        return self.grpc_table_ops
