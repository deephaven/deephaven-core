#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""This module provides access to the Update Graph (UG)'s locks that must be acquired to perform certain
table operations. When working with refreshing tables, UG locks must be held in order to have a consistent view of
the data between table operations.
"""

import contextlib
from collections import abc
from functools import wraps
from typing import Callable, Union

import jpy

from deephaven import DHError

_JUpdateGraph = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraph")

auto_locking = True
"""Whether to automatically acquire the Update Graph (UG) shared lock for an unsafe operation on a refreshing 
table when the current thread doesn't own either the UG shared or the UG exclusive lock. The newly obtained lock will 
be released after the table operation finishes. Auto locking is turned on by default."""


def has_exclusive_lock(ug: Union[_JUpdateGraph, "Table", "PartitionedTable", "PartitionTableProxy"]) -> bool:
    """Checks if the current thread is holding the provided Update Graph's (UG) exclusive lock.

    Args:
        ug (Union[_JUpdateGraph, Table, PartitionedTable, PartitionTableProxy]): The Update Graph (UG) or a table-like object.

    Returns:
        True if the current thread is holding the Update Graph (UG) exclusive lock, False otherwise.
    """
    from deephaven.table import Table, PartitionedTable, PartitionedTableProxy
    if isinstance(ug, Table):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTable):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTableProxy):
        ug = ug.update_graph

    return ug.exclusiveLock().isHeldByCurrentThread()


def has_shared_lock(ug: Union[_JUpdateGraph, "Table", "PartitionedTable", "PartitionTableProxy"]) -> bool:
    """Checks if the current thread is holding the provided Update Graph's (UG) shared lock.

    Args:
        ug (Union[_JUpdateGraph, Table, PartitionedTable, PartitionTableProxy]): The Update Graph (UG) or a table-like object.

    Returns:
        True if the current thread is holding the Update Graph (UG) shared lock, False otherwise.
    """
    from deephaven.table import Table, PartitionedTable, PartitionedTableProxy
    if isinstance(ug, Table):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTable):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTableProxy):
        ug = ug.update_graph

    return ug.sharedLock().isHeldByCurrentThread()


@contextlib.contextmanager
def exclusive_lock(ug: Union[_JUpdateGraph, "Table", "PartitionedTable", "PartitionTableProxy"]):
    """Context manager for running a block of code under a Update Graph (UG) exclusive lock.

    Args:
        ug (Union[_JUpdateGraph, Table, PartitionedTable, PartitionTableProxy]): The Update Graph (UG) or a table-like object.
    """
    from deephaven.table import Table, PartitionedTable, PartitionedTableProxy
    if isinstance(ug, Table):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTable):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTableProxy):
        ug = ug.update_graph

    lock = ug.exclusiveLock()
    lock.lock()
    try:
        yield
    except Exception as e:
        raise DHError(e, "exception raised in the enclosed code block.") from e
    finally:
        lock.unlock()


@contextlib.contextmanager
def shared_lock(ug: Union[_JUpdateGraph, "Table", "PartitionedTable", "PartitionTableProxy"]):
    """Context manager for running a block of code under a Update Graph (UG) shared lock.

    Args:
        ug (Union[_JUpdateGraph, Table, PartitionedTable, PartitionTableProxy]): The Update Graph (UG) or a table-like object.
    """
    from deephaven.table import Table, PartitionedTable, PartitionedTableProxy
    if isinstance(ug, Table):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTable):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTableProxy):
        ug = ug.update_graph

    lock = ug.sharedLock()
    lock.lock()
    try:
        yield
    except Exception as e:
        raise DHError(e, "exception raised in the enclosed code block.") from e
    finally:
        lock.unlock()


def exclusive_locked(ug: Union[_JUpdateGraph, "Table", "PartitionedTable", "PartitionTableProxy"]) -> Callable:
    """A decorator that ensures the decorated function be called under the Update Graph (UG) exclusive
    lock. The lock is released after the function returns regardless of what happens inside the function.

    Args:
        ug (Union[_JUpdateGraph, Table, PartitionedTable, PartitionTableProxy]): The Update Graph (UG) or a table-like object.
    """
    from deephaven.table import Table, PartitionedTable, PartitionedTableProxy
    if isinstance(ug, Table):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTable):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTableProxy):
        ug = ug.update_graph

    def inner_wrapper(f: Callable) -> Callable:
        @wraps(f)
        def do_locked(*arg, **kwargs):
            with exclusive_lock(ug):
                return f(*arg, **kwargs)

        return do_locked

    return inner_wrapper

def shared_locked(ug: Union[_JUpdateGraph, "Table", "PartitionedTable", "PartitionTableProxy"]) -> Callable:
    """A decorator that ensures the decorated function be called under the Update Graph (UG) shared lock.
    The lock is released after the function returns regardless of what happens inside the function.

    Args:
        ug (Union[_JUpdateGraph, Table, PartitionedTable, PartitionTableProxy]): The Update Graph (UG) or a table-like object.
    """
    from deephaven.table import Table, PartitionedTable, PartitionedTableProxy
    if isinstance(ug, Table):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTable):
        ug = ug.update_graph
    if isinstance(ug, PartitionedTableProxy):
        ug = ug.update_graph

    def inner_wrapper(f: Callable) -> Callable:
        @wraps(f)
        def do_locked(*arg, **kwargs):
            with shared_lock(ug):
                return f(*arg, **kwargs)

        return do_locked

    return inner_wrapper


def _is_arg_refreshing(arg):
    if isinstance(arg, list) or isinstance(arg, tuple):
        for e in arg:
            if _is_arg_refreshing(e):
                return True
    elif getattr(arg, "is_refreshing", False):
        return True

    return False


def _first_refreshing_table(*args, **kwargs):
    for arg in args:
        if _is_arg_refreshing(arg):
            return arg
    for k, v in kwargs.items():
        if _is_arg_refreshing(v):
            return v

    return None


def auto_locking_op(f: Callable) -> Callable:
    """A decorator for annotating unsafe Table operations. It ensures that the decorated function runs under the UG
    shared lock if ugp.auto_locking is True, the target table-like object or any table-like arguments are refreshing,
    and the current thread doesn't own any UG locks."""

    @wraps(f)
    def do_locked(*args, **kwargs):
        arg = _first_refreshing_table(*args, **kwargs)
        if (not arg
                or not auto_locking
                or has_shared_lock(arg)
                or has_exclusive_lock(arg)):
            return f(*args, **kwargs)

        with shared_lock(arg.update_graph):
            return f(*args, **kwargs)

    return do_locked


@contextlib.contextmanager
def auto_locking_ctx(*args, **kwargs):
    """An auto-locking aware context manager. It ensures that the enclosed code block runs under the UG shared lock if
    ugp.auto_locking is True, the target table-like object or any table-like arguments are refreshing, and the current
    thread doesn't own any UG locks."""
    arg = _first_refreshing_table(*args, **kwargs)
    if (not arg
            or not auto_locking
            or has_shared_lock(arg)
            or has_exclusive_lock(arg)):
        yield
    else:
        with shared_lock(arg.update_graph):
            yield
