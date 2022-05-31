#
#     Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module provides access to the Update Graph Processor's locks that must be acquired to perform certain table
operations."""
import contextlib

import jpy

_JUpdateGraphProcessor = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor")
_j_exclusive_lock = _JUpdateGraphProcessor.DEFAULT.exclusiveLock()
_j_shared_lock = _JUpdateGraphProcessor.DEFAULT.sharedLock()

@contextlib.contextmanager
def ugp_exclusive_lock():
    """Context manager for running a block of code under a UGP exclusive lock."""
    _j_exclusive_lock.lock()
    try:
        yield
    finally:
        _j_exclusive_lock.unlock()


@contextlib.contextmanager
def ugp_shared_lock():
    """Context manager for running a block of code under a UGP shared lock."""
    _j_shared_lock.lock()
    try:
        yield
    finally:
        _j_shared_lock.unlock()
