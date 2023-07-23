#
#  Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from typing import List, Sequence, TypeVar, Union
import pydeephaven_ticking._core as dhc

T = TypeVar("T")


def _to_sequence(v: Union[T, Sequence[T]] = None) -> Sequence[T]:
    """This  enables a function to provide parameters that can accept both singular and plural values of the same type
    for the convenience of the users, e.g. both x= "abc" and x = ["abc"] are valid arguments.
    (adapted from table_listener.py)
    """

    if v is None:
        return ()
    if isinstance(v, Sequence) or isinstance(v, str):
        return (v,)
    return tuple(o for o in v)


def canonicalize_cols_param(table: dhc.ClientTable, col_names: Union[str, List[str]] = None) -> Sequence[str]:
    """Canonicalizes the col_names parameter by turning it into a sequence of strings.
    If already a list of strings, just return it.
    If a single string, transform it into a sequence containing one string.
    If None, then consult the ClientTable schema to find out all the column names."""

    if col_names is None:
        return table.schema.names

    return _to_sequence(col_names)
