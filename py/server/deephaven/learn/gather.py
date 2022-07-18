#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Utilities for gathering Deephaven table data into Python objects """

import enum
from typing import Any, Type

import jpy
import numpy as np

from deephaven import DHError

_JGatherer = jpy.get_type("io.deephaven.integrations.learn.gather.NumPy")


class MemoryLayout(enum.Enum):
    """ Memory layouts for an array. """

    ROW_MAJOR = True
    """ Row-major memory layout."""
    COLUMN_MAJOR = False
    """ Column-major memory layout."""
    C = True
    """ Memory layout consistent with C arrays (row-major)."""
    FORTRAN = False
    """ Memory layout consistent with Fortran arrays (column-major)."""

    def __init__(self, is_row_major):
        self.is_row_major = is_row_major


def _convert_to_numpy_dtype(np_type: Type) -> Type:
    """ Converts an input type to the corresponding NumPy data type. """
    if np_type.__module__ == np.__name__:
        return np_type
    elif np_type == bool:
        np_type = np.bool_
    elif np_type == float:
        np_type = np.double
    elif np_type == int:
        np_type = np.intc
    else:
        raise ValueError(f"{np_type} is not a data type that can be converted to a NumPy dtype.")
    return np_type


def table_to_numpy_2d(row_set, col_set, order: MemoryLayout = MemoryLayout.ROW_MAJOR, np_type: Type = np.intc) -> np.ndarray:
    """ Converts Deephaven table data to a 2d NumPy array of the appropriate size

    Args:
        row_set: a RowSequence describing the number of rows in the table
        col_set: ColumnSources describing which columns to copy
        order (MemoryLayout): the desired memory layout of the output array
        np_type: the desired NumPy data type of the output NumPy array

    Returns
        a np.ndarray

    Raises:
        DHError
    """

    try:
        np_type = _convert_to_numpy_dtype(np_type)

        if np_type == np.byte:
            buffer = _JGatherer.tensorBuffer2DByte(row_set, col_set, order.is_row_major)
        elif np_type == np.short:
            buffer = _JGatherer.tensorBuffer2DShort(row_set, col_set, order.is_row_major)
        elif np_type == np.intc:
            buffer = _JGatherer.tensorBuffer2DInt(row_set, col_set, order.is_row_major)
        elif np_type == np.int_:
            buffer = _JGatherer.tensorBuffer2DLong(row_set, col_set, order.is_row_major)
        elif np_type == np.single:
            buffer = _JGatherer.tensorBuffer2DFloat(row_set, col_set, order.is_row_major)
        elif np_type == np.double:
            buffer = _JGatherer.tensorBuffer2DDouble(row_set, col_set, order.is_row_major)
        else:
            raise ValueError(f"Data type {np_type} is not supported.")

        tensor = np.frombuffer(buffer, dtype=np_type)

        if order.is_row_major:
            tensor.shape = (len(col_set), row_set.intSize())
            return tensor.T
        else:
            tensor.shape = (row_set.intSize(), len(col_set))
            return tensor
    except Exception as e:
        raise DHError(e, f"failed to convert rows: {row_set} and cols: {col_set} to a 2D NumPy array") from e
