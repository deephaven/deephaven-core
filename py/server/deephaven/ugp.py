#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module provides access to the Update Graph Processor(UGP)'s locks that must be acquired to perform certain
table operations. When working with refreshing tables, UGP locks must be held in order to have a consistent view of
the data between table operations.
"""

import contextlib
from collections import abc
from functools import wraps
from typing import Callable

import jpy

from deephaven import DHError

_JUpdateGraphProcessor = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor")
_j_exclusive_lock = _JUpdateGraphProcessor.DEFAULT.exclusiveLock()
_j_shared_lock = _JUpdateGraphProcessor.DEFAULT.sharedLock()

auto_locking = False
"""Whether to automatically acquire the Update Graph Processor(UGP) shared lock for an unsafe operation on a ticking 
table when the current thread doesn't own either the UGP shared or the UGP exclusive lock. The newly obtained lock will 
be released after the table operation finishes."""


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
    """A decorator that ensures the decorated function be called under the Update Graph Processor(UGP) exclusive
    lock. The lock is released after the function returns regardless of what happens inside the function."""

    @wraps(f)
    def do_locked(*arg, **kwargs):
        with exclusive_lock():
            return f(*arg, **kwargs)

    return do_locked


def shared_locked(f: Callable) -> Callable:
    """A decorator that ensures the decorated function be called under the Update Graph Processor(UGP) shared lock.
    The lock is released after the function returns regardless of what happens inside the function."""

    @wraps(f)
    def do_locked(*arg, **kwargs):
        with shared_lock():
            return f(*arg, **kwargs)

    return do_locked


def _is_arg_refreshing(arg):
    if isinstance(arg, list) or isinstance(arg, tuple):
        for e in arg:
            if _is_arg_refreshing(e):
                return True
    elif getattr(arg, "is_refreshing", False):
        return True

    return False


def _has_refreshing_tables(*args, **kwargs):
    for arg in args:
        if _is_arg_refreshing(arg):
            return True
    for k, v in kwargs.items():
        if _is_arg_refreshing(v):
            return True

    return False


def auto_locking_op(f: Callable) -> Callable:
    """A decorator for annotating unsafe Table operations. It ensures that the decorated function runs under the UGP
    shared lock if ugp.auto_locking is True, the target table-like object or any table-like arguments are refreshing,
    and the current thread doesn't own any UGP locks."""

    @wraps(f)
    def do_locked(*args, **kwargs):
        if (not _has_refreshing_tables(*args, **kwargs)
                or not auto_locking
                or has_shared_lock()
                or has_exclusive_lock()):
            return f(*args, **kwargs)

        with shared_lock():
            return f(*args, **kwargs)

    return do_locked


@contextlib.contextmanager
def auto_locking_ctx(*args, **kwargs):
    """An auto-locking aware context manager. It ensures that the enclosed code block runs under the UGP shared lock if
    ugp.auto_locking is True, the target table-like object or any table-like arguments are refreshing, and the current
    thread doesn't own any UGP locks."""
    if (not _has_refreshing_tables(*args, **kwargs)
            or not auto_locking
            or has_shared_lock()
            or has_exclusive_lock()):
        yield
    else:
        with shared_lock():
            yield
