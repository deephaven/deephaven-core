#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
import contextlib

import jpy

_JUpdateGraphProcessor = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor")


@contextlib.contextmanager
def ugp_exclusive_lock():
    """Context manager for running a block of code under a UGP exclusive lock."""
    j_exclusive_lock = _JUpdateGraphProcessor.DEFAULT.exclusiveLock()
    j_exclusive_lock.lock()
    try:
        yield
    finally:
        j_exclusive_lock.unlock()


@contextlib.contextmanager
def ugp_shared_lock():
    """Context manager for running a block of code under a UGP shared lock."""
    j_shared_lock = _JUpdateGraphProcessor.DEFAULT.sharedLock()
    j_shared_lock.lock()
    try:
        yield
    finally:
        j_shared_lock.unlock()
