#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

from __future__ import annotations

from typing import Iterable

from ._dtype import *
from ._datetime import DateTime, Period


def j_array_list(values: Iterable):
    j_list = jpy.get_type("java.util.ArrayList")(len(values))
    try:
        for v in values:
            j_list.add(v)
        return j_list
    except Exception as e:
        raise DHError(e, "failed to create a Java ArrayList from the Python collection.") from e
