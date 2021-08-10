#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from abc import ABC
from enum import Enum

from pydeephaven.constants import SortDirection


class TableOp(ABC):
    def __init__(self, parent=None):
        self.parent = parent
        if parent:
            self.parent.child = self
        self.child = None

    def accept(self, visitor):
        visitor.visit(self)
        if self.child:
            visitor.visit(self.child)


class NoneOp(TableOp):
    def __init__(self, parent=None, table=None):
        super().__init__()
        self.table = table


class SelectOrUpdateType(Enum):
    UPDATE = 1
    LAZY_UPDATE = 2
    VIEW = 3
    UPDATE_VIEW = 4
    SELECT = 5


class DropColumnsOp(TableOp):
    def __init__(self, parent, column_names):
        super().__init__(parent=parent)
        self.column_names = tuple(column_names)


class UpdateOp(TableOp):
    def __init__(self, parent, column_specs):
        super().__init__(parent=parent)
        self.column_specs = tuple(column_specs)


class LazyUpdateOp(TableOp):
    def __init__(self, parent, column_specs):
        super().__init__(parent=parent)
        self.column_specs = tuple(column_specs)


class ViewOp(TableOp):
    def __init__(self, parent, column_specs):
        super().__init__(parent=parent)
        self.column_specs = column_specs


class UpdateViewOp(TableOp):
    def __init__(self, parent, column_specs):
        super().__init__(parent=parent)
        self.column_specs = column_specs


class SelectOp(TableOp):
    def __init__(self, parent, column_specs):
        super().__init__(parent=parent)
        self.column_specs = column_specs


class SelectDistinctOp(TableOp):
    def __init__(self, parent, columns):
        super().__init__(self, parent=parent)
        self.columns = columns


class UnStructuredFilterOp(TableOp):
    ...


class SortOp(TableOp):
    def __init__(self, parent, columns, directions):
        super().__init__(parent=parent)
        self.columns = columns
        self.directions = directions


class TailOp(TableOp):
    def __init__(self, parent, num_rows):
        super().__init__(parent=parent)
        self.num_rows = num_rows


class HeadOp(TableOp):
    def __init__(self, parent, num_rows):
        super().__init__(parent=parent)
        self.num_rows = num_rows


class HeadBy(TableOp):
    ...


class TailBy(TableOp):
    ...


class Ungroup(TableOp):
    ...


class Join(TableOp):
    ...

