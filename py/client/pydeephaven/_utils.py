#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
from collections.abc import Sequence
from typing import Any

from .dherror import DHError


def to_list(v: Any = None) -> list[Any]:
    """Converts the input value to a list of the same type, if it is not already a list."""
    if v is None or isinstance(v, list) and not v:
        return []

    if not isinstance(v, Sequence) or isinstance(v, str):
        return [v]
    else:
        return list(v)


def to_filter_strings(filters: str | list[str]) -> list[str]:
    """Returns count_where filters as a list of strings. The request carries filters only as strings, so a Filter
    object would fail inside protobuf with an unclear TypeError."""
    filters = to_list(filters)
    if not all(isinstance(f, str) for f in filters):
        raise DHError(
            message="count_where filters must be strings. Filter objects are not supported."
        )
    return filters
