#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module provides access to the Update Graph Processor(UGP)'s locks that must be acquired to perform certain
table operations. When working with refreshing tables, UGP locks must be held in order to have a consistent view of
the data between table operations.

"""

import contextlib
from functools import wraps
from typing import Callable

import jpy
from deephaven import DHError

_JUpdateGraphProcessor = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor")
_j_exclusive_lock = _JUpdateGraphProcessor.DEFAULT.exclusiveLock()
_j_shared_lock = _JUpdateGraphProcessor.DEFAULT.sharedLock()

auto_locking = False
"""Whether to automatically acquire the Update Graph Processor(UGP) shared lock for a unsafe ticket table operation if
the current thread doesn't own either the UGP shared or the UGP exclusive lock. The newly obtained lock will be released
after the table operation finishes."""


def has_exclusive_lock() -> bool:
    """Checks if the current thread is holding the Update Graph Processor(UGP) exclusive lock."""
    return _j_exclusive_lock.isHeldByCurrentThread()


def has_shared_lock() -> bool:
    """Checks if the current thread is holding the Update Graph Processor(UGP) shared lock."""
    return _j_shared_lock.isHeldByCurrentThread()


@contextlib.contextmanager
def exclusive_lock():
    """Context manager for running a block of code under a Update Graph Processor(UGP) exclusive lock."""
    _j_exclusive_lock.lock()
    try:
        yield
    except Exception as e:
        raise DHError(e, "exception raised in the enclosed code block.") from e
    finally:
        _j_exclusive_lock.unlock()


@contextlib.contextmanager
def shared_lock():
    """Context manager for running a block of code under a Update Graph Processor(UGP) shared lock."""
    _j_shared_lock.lock()
    try:
        yield
    except Exception as e:
        raise DHError(e, "exception raised in the enclosed code block.") from e
    finally:
        _j_shared_lock.unlock()


def exclusive_locked(f: Callable) -> Callable:
    """A decorator that ensures that the decorated function be called under Update Graph Processor(UGP) exclusive
    lock. The lock is release regardless what happens inside the function. """

    @wraps(f)
    def do_locked(*arg, **kwargs):
        with exclusive_lock():
            return f(*arg, **kwargs)

    return do_locked


def shared_locked(f) -> Callable:
    """A decorator that ensures that the decorated function be called under Update Graph Processor(UGP) shared lock.
    The lock is release regardless what happens inside the function. """

    @wraps(f)
    def do_locked(*arg, **kwargs):
        with shared_lock():
            return f(*arg, **kwargs)

    return do_locked


def auto_locking_op(f: Callable) -> Callable:
    @wraps(f)
    def do_locked(*args, **kwargs):
        t = args[0] if args else kwargs['self']
        if (not t.is_refreshing
                or not auto_locking
                or has_shared_lock()
                or has_exclusive_lock()):
            return f(*args, **kwargs)

        with shared_lock():
            return f(*args, **kwargs)

    return do_locked
