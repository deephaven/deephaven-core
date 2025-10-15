#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
from typing import List, Sequence, Any


def to_list(v: Any = None) -> List[Any]:
    """Converts the input value to a list of the same type, if it is not already a list."""
    if v is None or isinstance(v, List) and not v:
        return []

    if not isinstance(v, Sequence) or isinstance(v, str):
        return [v]
    else:
        return list(v)
