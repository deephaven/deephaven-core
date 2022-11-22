#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module provides the helper and convenience functions that are not part of the Deephaven server but
nonetheless uniquely useful to user applications built on top of Deephaven."""
import gc

import jpy

from deephaven import DHError


def garbage_collect() -> None:
    """Runs full garbage collection in Python first and then requests the JVM to run its garbage collector. Since there
    is no way to force the Java garbage collector to run, the effect of calling this function is non-deterministic.

    Raises:
        DHError
    """
    try:
        gc.collect()
        jpy.get_type("java.lang.System").gc()
    except Exception as e:
        raise DHError(e, "failed to initiate system-wide garbage collection.")
