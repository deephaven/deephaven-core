#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
"""This module hosts helper functions for use with the Deephaven Python Client API."""

import pyarrow as pa

from ._arrow import map_arrow_type
from .dherror import DHError


def is_deephaven_compatible(data_type: pa.DataType) -> bool:
    """Checks if the arrow data type is supported by Deephaven."""
    try:
        map_arrow_type(data_type)
        return True
    except DHError:
        return False
