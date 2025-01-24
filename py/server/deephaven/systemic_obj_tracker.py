#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
"""This module allows user to enable/disable Deephaven systemic object marking. When enabled, Deephaven will mark
all objects created in the current thread as systemic. These systemic objects will be tracked and if errors occur to
them, the errors are deemed to be systemic and fatal.
"""
import contextlib

import jpy
from deephaven import DHError

_JSystemicObjectTracker = jpy.get_type("io.deephaven.engine.util.systemicmarking.SystemicObjectTracker")


def is_systemic_object_marking_enabled() -> bool:
    """Returns True if the systemic object marking is enabled. When enabled, the current thread can be marked as
    systemic or not systemic.
    """
    return _JSystemicObjectTracker.isSystemicObjectMarkingEnabled()


def is_systemic() -> bool:
    """Returns whether the current thread is systemic. If true, objects created on this thread are treated as
    systemic."""
    return _JSystemicObjectTracker.isSystemicThread()


def set_systemic(systemic: bool) -> None:
    """Sets whether the current thread is systemic. If true, objects created on this thread are treated as systemic.

    Args:
        systemic (bool): True to mark the current thread as systemic, False to mark it as not systemic.

    Raises:
        DHError: If the systemic object marking is not enabled.
    """
    if is_systemic_object_marking_enabled():
        if systemic:
            _JSystemicObjectTracker.markThreadSystemic()
        else:
            _JSystemicObjectTracker.markThreadNotSystemic()
    else:
        raise DHError(message="Systemic object marking is not enabled.")


@contextlib.contextmanager
def systemic_object_marking() -> None:
    """A Context manager to ensure the current thread is marked as systemic for the execution of the enclosed code
    block. On exit, the thread is restored to its previous systemic state.

    Raises:
        DHError: If the systemic object marking is not enabled.
    """
    if is_systemic_object_marking_enabled():
        if not is_systemic():
            try:
                _JSystemicObjectTracker.markThreadSystemic()
                yield
            finally:
                _JSystemicObjectTracker.markThreadNotSystemic()
        else:
            yield
    else:
        raise DHError(message="Systemic object marking is not enabled.")


@contextlib.contextmanager
def no_systemic_object_marking() -> None:
    """A Context manager to ensure the current thread is marked as not systemic for the execution of the enclosed code
    block. On exit, the thread is restored to its previous systemic state.

    Raises:
        DHError: If the systemic object marking is not enabled.
    """
    if is_systemic_object_marking_enabled():
        if is_systemic():
            try:
                _JSystemicObjectTracker.markThreadNotSystemic()
                yield
            finally:
                _JSystemicObjectTracker.markThreadSystemic()
        else:
            yield
    else:
        raise DHError(message="Systemic object marking is not enabled.")
