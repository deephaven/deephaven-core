#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from typing import Union, TypeVar, List

T = TypeVar("T")


def to_list(v: Union[T, List[T]]) -> List[T]:
    if not v:
        return []

    if not isinstance(v, List) or isinstance(v, str):
        return [v]
    else:
        return list(v)
