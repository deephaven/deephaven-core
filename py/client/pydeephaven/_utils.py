#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from collections.abc import Sequence
from typing import Any


def to_list(v: Any = None) -> list[Any]:
    """Converts the input value to a list of the same type, if it is not already a list."""
    if v is None or isinstance(v, list) and not v:
        return []

    if not isinstance(v, Sequence) or isinstance(v, str):
        return [v]
    else:
        return list(v)
