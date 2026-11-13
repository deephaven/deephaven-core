#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""This module hosts helper functions for use with the Deephaven Python Client API."""

import pyarrow as pa

from ._arrow import _is_supported


def is_deephaven_compatible(data_type: pa.DataType) -> bool:
    """Checks whether Session.import_table supports a pyarrow data type.

    Args:
        data_type (pa.DataType): the pyarrow data type

    Returns:
        True if import_table supports the type, False otherwise
    """
    return _is_supported(data_type)
