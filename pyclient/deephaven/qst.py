from abc import ABC, abstractmethod
from enum import Enum


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
    def __init__(self, parent=None, column_names=[]):
        super().__init__(parent=parent)
        self.column_names = tuple(column_names)


class UpdateOp(TableOp):
    def __init__(self, parent=None, column_specs=[]):
        super().__init__(parent=parent)
        self.column_specs = tuple(column_specs)


class LazyUpdateOp(TableOp):
    def __init__(self, parent=None, column_specs=[]):
        super().__init__(parent=parent)
        self.column_specs = tuple(column_specs)


class ViewOp(TableOp):
    def __init__(self, parent=None, column_specs=[]):
        super().__init__(parent=parent)
        self.column_specs = column_specs


class UpdateViewOp(TableOp):
    def __init__(self, parent=None, column_specs=[]):
        super().__init__(parent=parent)
        self.column_specs = column_specs


class TailOp(TableOp):
    def __init__(self, parent=None, num_rows=1):
        super().__init__(parent=parent)
        self.num_rows = num_rows


class HeadOp(TableOp):
    def __init__(self, parent=None, num_rows=1):
        super().__init__(parent=parent)
        self.num_rows = num_rows


class SelectOp(TableOp):
    def __init__(self, parent=None, column_specs=[]):
        super().__init__(parent == parent)
        self.column_specs = column_specs


class SortDirection(Enum):
    UNKNOWN = 0
    DESCENDING = 1
    ASCENDING = 2
    REVERSE = 3


class SortOp(TableOp):
    def __init__(self, parent=None, column_name="", direction=SortDirection.UNKNOWN):
        super().__init__(parent=parent)
        self.column_name = column_name
        self.direction = direction
