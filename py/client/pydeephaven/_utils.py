#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from typing import Union, TypeVar, List

T = TypeVar("T")


def to_list(v: Union[T, List[T]]) -> List[T]:
    """Converts the input value to a list of the same type, if it is not already a list."""
    if v is None or isinstance(v, List) and not v:
        return []

    if not isinstance(v, List) or isinstance(v, str):
        return [v]
    else:
        return list(v)
