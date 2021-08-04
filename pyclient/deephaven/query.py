from deephaven.dherror import DHError
from deephaven.qst import *


class Query:
    def __init__(self, session=None, table=None):
        self.session = session
        if not self.session or not table:
            raise DHError("invalid session or table value.")
        self._dag = self._last_op = NoneOp(table=table)

    def exec(self):
        return self.session.table_service.batch(self._dag)

    def drop_columns(self, column_names):
        self._last_op = DropColumnsOp(parent=self._last_op, column_names=column_names)
        return self

    def update(self, column_specs=[]):
        self._last_op = UpdateOp(parent=self._last_op, column_specs=column_specs)
        return self

    def lazy_update(self, column_specs):
        self._last_op = LazyUpdateOp(parent=self._last_op, column_specs=column_specs)
        return self

    def view(self, column_specs):
        self._last_op = ViewOp(parent=self._last_op, column_specs=column_specs)
        return self

    def update_view(self, column_specs):
        self._last_op = UpdateViewOp(parent=self._last_op, column_specs=column_specs)
        return self

    def select(self, column_spec):
        self._last_op = SelectOp(parent=self._last_op, column_specs=column_spec)
        return self

    def tail(self, num_rows=1):
        self._last_op = TailOp(parent=self._last_op, num_rows=num_rows)
        return self

    def head(self, num_rows=1):
        self._last_op = HeadOp(parent=self._last_op, num_rows=num_rows)
        return self

    def sort(self, column_name, direction=SortDirection.UNKNOWN):
        self._last_op = SortOp(parent=self._last_op, column_name=column_name, direction=direction)
        return self
