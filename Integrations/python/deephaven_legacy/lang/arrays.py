"""
Utilities for working with arrays.

**The contents of this module are intended only for internal Deephaven use and may change at any time.**
"""

from numpy import asarray, int32, int64, float32, float64

# We may want to use columnar Fortran 'F' order
default_order = 'A' #: Default memory representation (e.g. row-major (C-style) or column-major (Fortran-style)).


def int32_(a, order=default_order):
    """
    Converts the input to a numpy int32 array.

    :param a: input
    :return: numpy array
    """
    return asarray(a, dtype=int32, order=order)


def int64_(a, order=default_order):
    """
    Converts the input to a numpy int64 array.

    :param a: input
    :return: numpy array
    """
    return asarray(a, dtype=int64, order=order)


def float32_(a, order=default_order):
    """
    Converts the input to a numpy float32 array.

    :param a: input
    :return: numpy array
    """
    return asarray(a, dtype=float32, order=order)


def float64_(a, order=default_order):
    """
    Converts the input to a numpy float64 array.

    :param a: input
    :return: numpy array
    """
    return asarray(a, dtype=float64, order=order)
