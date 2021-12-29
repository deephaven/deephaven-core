#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    """ A Column object represents a column in a Deephaven Table. """
    name: str
    data_type: str
    component_type: str
    column_type: str
    isPartitioning: bool
    isGrouping: bool
