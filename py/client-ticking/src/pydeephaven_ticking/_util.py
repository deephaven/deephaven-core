#
#  Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

from typing import Any, Optional, Sequence, Union

import pydeephaven_ticking._core as dhc


def _to_sequence(v: Optional[Any] = None) -> Sequence[Any]:
    """This  enables a function to provide parameters that can accept both singular and plural values of the same type
    for the convenience of the users, e.g. both x= "abc" and x = ["abc"] are valid arguments.
    (adapted from table_listener.py)

    Args:
        v (Optional[Any]) : The input argument

    Returns:
        A sequence containing 0, 1, or multiple values
    """

    if v is None:
        return ()
    if not isinstance(v, Sequence) or isinstance(v, str):
        return (v,)
    return tuple(o for o in v)


def canonicalize_cols_param(
    table: dhc.ClientTable, col_names: Optional[Union[str, Sequence[str]]] = None
) -> Sequence[str]:
    """Canonicalizes the col_names parameter by turning it into a sequence of strings.
    If already a list of strings, just return it.
    If a single string, transform it into a sequence containing one string.
    If None, then consult the ClientTable schema to find out all the column names.

    Args:
        table (dhc.ClientTable) : The table to consult to get the column names from if necessary
        col_names (Optional[Union[str, Sequence[str]]]) : The column names to canonicalize

    Returns:
        The canonicalized column names
    """

    if col_names is None:
        return table.schema.names

    return _to_sequence(col_names)
