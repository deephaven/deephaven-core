#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from enum import Enum, auto
import jpy


class SortDirection(Enum):
    """An enum defining the sorting orders."""
    DESCENDING = auto()
    """"""
    ASCENDING = auto()
    """"""


# Deephaven Special Null values for primitive types
_JQueryConstants = jpy.get_type("io.deephaven.util.QueryConstants")
NULL_CHAR = _JQueryConstants.NULL_CHAR
NULL_FLOAT = _JQueryConstants.NULL_FLOAT
NULL_DOUBLE = _JQueryConstants.NULL_DOUBLE
NULL_SHORT = _JQueryConstants.NULL_SHORT
NULL_INT = _JQueryConstants.NULL_INT
NULL_LONG = _JQueryConstants.NULL_LONG
NULL_BYTE = _JQueryConstants.NULL_BYTE

MAX_CHAR = _JQueryConstants.MAX_CHAR
MAX_FLOAT = _JQueryConstants.MAX_FLOAT
MAX_DOUBLE = _JQueryConstants.MAX_DOUBLE
MAX_SHORT = _JQueryConstants.MAX_SHORT
MAX_INT = _JQueryConstants.MAX_INT
MAX_LONG = _JQueryConstants.MAX_LONG
MAX_BYTE = _JQueryConstants.MAX_BYTE

MIN_CHAR = _JQueryConstants.MIN_CHAR
MIN_FLOAT = _JQueryConstants.MIN_FLOAT
MIN_DOUBLE = _JQueryConstants.MIN_DOUBLE
MIN_SHORT = _JQueryConstants.MIN_SHORT
MIN_INT = _JQueryConstants.MIN_INT
MIN_LONG = _JQueryConstants.MIN_LONG
MIN_BYTE = _JQueryConstants.MIN_BYTE