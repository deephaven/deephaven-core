#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module defines a convenience function for running both Python and Java garbage collection utilities."""

import gc

import jpy

from deephaven import DHError


def garbage_collect() -> None:
    """Runs full garbage collection in Python first and then requests the JVM to run its garbage collector twice due
    to the cross-referencing nature of the Python/Java integration in Deephaven. Since there is no way to force the
    Java garbage collector to run, the effect of calling this function is non-deterministic. Users also need to be
    mindful of the overhead that running garbage collection generally incurs.

    Raises:
        DHError
    """
    try:
        gc.collect()
        _j_system = jpy.get_type("java.lang.System")
        _j_system.gc()
        _j_system.gc()
    except Exception as e:
        raise DHError(e, "failed to initiate system-wide garbage collection.")
