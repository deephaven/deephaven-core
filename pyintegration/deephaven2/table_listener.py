#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module provides utilities for listening to table changes. """
from __future__ import annotations

from abc import ABC, abstractmethod
from inspect import signature
from typing import Callable, Union, Type

import jpy

from deephaven2 import DHError
from deephaven2.table import Table


class TableListenerHandle:
    """A handle for a table listener."""

    def __init__(self, t: Table, listener):
        """Creates a new table listener handle.

        Args:
            t (Table): table being listened to
            listener: listener object
        """
        self.t = t
        self.listener = listener
        self.isRegistered = False

    def register(self) -> None:
        """Register the listener with the table and listen for updates.

        Raises:
            RuntimeError
        """

        if self.isRegistered:
            raise RuntimeError(
                "Attempting to register an already registered listener.."
            )

        self.t.j_table.listenForUpdates(self.listener)

        self.isRegistered = True

    def deregister(self) -> None:
        """Deregister the listener from the table and stop listening for updates.

        Raises:
            RuntimeError
        """

        if not self.isRegistered:
            raise RuntimeError("Attempting to deregister an unregistered listener..")

        self.t.j_table.removeUpdateListener(self.listener)
        self.isRegistered = False


def _do_locked(f: Callable, lock_type="shared") -> None:
    """Executes a function while holding the UpdateGraphProcessor (UGP) lock.  Holding the UGP lock
    ensures that the contents of a table will not change during a computation, but holding
    the lock also prevents table updates from happening.  The lock should be held for as little
    time as possible.

    Args:
        f (Callable): callable to execute while holding the UGP lock, could be function or an object with an 'apply'
            attribute which is callable
        lock_type (str): UGP lock type, valid values are "exclusive" and "shared".  "exclusive" allows only a single
            reader or writer to hold the lock.  "shared" allows multiple readers or a single writer to hold the lock.
    Raises:
        ValueError
    """
    throwing_runnable = jpy.get_type(
        "io.deephaven.integrations.python.PythonThrowingRunnable"
    )
    update_graph_processor = jpy.get_type(
        "io.deephaven.engine.updategraph.UpdateGraphProcessor"
    )

    if lock_type == "exclusive":
        update_graph_processor.DEFAULT.exclusiveLock().doLocked(throwing_runnable(f))
    elif lock_type == "shared":
        update_graph_processor.DEFAULT.sharedLock().doLocked(throwing_runnable(f))
    else:
        raise ValueError(f"Unsupported lock type: lock_type={lock_type}")


def _nargs_listener(listener) -> int:
    """Returns the number of arguments the listener takes.

    Args:
        listener: listener

    Returns:
        the number of arguments the listener takes

    Raises:
        ValueError, NotImplementedError
    """

    if callable(listener):
        f = listener
    elif hasattr(listener, "onUpdate"):
        f = listener.onUpdate
    else:
        raise ValueError(
            "ShiftObliviousListener is neither callable nor has an 'onUpdate' method"
        )

    return len(signature(f).parameters)


class TableListener(ABC):
    """An abstract table listener class that should be subclassed by any user Table listener class."""

    @abstractmethod
    def onUpdate(self, *args, **kwargs) -> None:
        ...


def listen(
    t: Table,
    listener: Union[Callable, Type[TableListener]],
    description: str = None,
    retain: bool = True,
    listener_type: str = "auto",
    start_listening: bool = True,
    replay_initial: bool = False,
    lock_type: str = "shared",
) -> TableListenerHandle:
    """Listen to table changes.

    Table change events are processed by 'listener', which can be either
        (1) a callable (e.g. function) or
        (2) an object that provides an "onUpdate" method.
    In either case, the method must have one of the following signatures.

    * (added, removed, modified): shift_oblivious
    * (isReplay, added, removed, modified): shift_oblivious + replay
    * (update): shift-aware
    * (isReplay, update): shift-aware + replay

    For shift-oblivious listeners, 'added', 'removed', and 'modified' are the indices of the rows which changed.
    For shift-aware listeners, 'update' is an object that describes the table update.

    Listeners that support replaying the initial table snapshot have an additional parameter, 'isReplay', which is
    true when replaying the initial snapshot and false during normal updates.

    See the Deephaven listener documentation for details on processing update events.  This documentation covers the
    details of the added, removed, and modified row sets; row shift information; as well as the details of how to apply
    the update object.  It also has examples on how to access current and previous-tick table values.

    Args:
        t (Table): table to listen to
        listener (Callable): listener for table changes
        description (str): description for the UpdatePerformanceTracker to append to the listener's entry description
        retain (bool): whether a hard reference to this listener should be maintained to prevent it from being
            collected, default is True
        listener_type (str): listener type, valid values are "auto", "shift_oblivious", and "shift_aware"
            "auto" (default) uses inspection to automatically determine the type of input listener
            "shift_oblivious" is for a shift_oblivious listener, which takes three (added, removed, modified) or four
                (isReplay, added, removed, modified) arguments
            "shift_aware" is for a shift-aware listener, which takes one (update) or two (isReplay, update) arguments
        start_listening (bool): True to create the listener and register the listener with the table. The listener will
            see updates. False to create the listener, but do not register the listener with the table. The listener
            will not see updates. Default is True
        replay_initial (bool): True to replay the initial table contents to the listener. False to only listen to new
            table changes. To replay the initial image, the listener must support replay. Default is False
        lock_type (str): UGP lock type, used when replay_initial=True, valid values are "exclusive" and "shared".
            Default is "shared".

    Returns:
        a TableListenerHandle instance

    Raises:
        DHError
    """

    try:
        if replay_initial and not start_listening:
            raise ValueError(
                "Unable to create listener.  Inconsistent arguments.  If the initial snapshot is replayed ("
                "replay_initial=True), then the listener must be registered to start listening ("
                "start_listening=True)."
            )

        nargs = _nargs_listener(listener)

        if listener_type is None or listener_type == "auto":
            if nargs == 1 or nargs == 2:
                listener_type = "shift_aware"
            elif nargs == 3 or nargs == 4:
                listener_type = "shift_oblivious"
            else:
                raise ValueError(
                    f"Unable to autodetect listener type. ShiftObliviousListener does not take an expected number of "
                    f"arguments. args={nargs}."
                )

        if listener_type == "shift_oblivious":
            if nargs == 3:
                if replay_initial:
                    raise ValueError(
                        "ShiftObliviousListener does not support replay: ltype={} nargs={}".format(
                            listener_type, nargs
                        )
                    )

                listener_adapter = jpy.get_type(
                    "io.deephaven.integrations.python.PythonShiftObliviousListenerAdapter"
                )
                listener_adapter = listener_adapter(
                    description, t.j_table, retain, listener
                )
            elif nargs == 4:
                listener_adapter = jpy.get_type(
                    "io.deephaven.integrations.python"
                    ".PythonReplayShiftObliviousListenerAdapter"
                )
                listener_adapter = listener_adapter(
                    description, t.j_table, retain, listener
                )
            else:
                raise ValueError(
                    "Legacy listener must take 3 (added, removed, modified) or 4 (isReplay, added, removed, modified) "
                    "arguments."
                )

        elif listener_type == "shift_aware":
            if nargs == 1:
                if replay_initial:
                    raise ValueError(
                        "ShiftObliviousListener does not support replay: ltype={} nargs={}".format(
                            listener_type, nargs
                        )
                    )

                listener_adapter = jpy.get_type(
                    "io.deephaven.integrations.python.PythonListenerAdapter"
                )
                listener_adapter = listener_adapter(
                    description, t.j_table, retain, listener
                )
            elif nargs == 2:
                listener_adapter = jpy.get_type(
                    "io.deephaven.integrations.python.PythonReplayListenerAdapter"
                )
                listener_adapter = listener_adapter(
                    description, t.j_table, retain, listener
                )
            else:
                raise ValueError(
                    "Shift-aware listener must take 1 (update) or 2 (isReplay, update) arguments."
                )

        else:
            raise ValueError(
                "Unsupported listener type: ltype={}".format(listener_type)
            )

        handle = TableListenerHandle(t, listener_adapter)

        def start():
            if replay_initial:
                listener_adapter.replay()

            if start_listening:
                handle.register()

        if replay_initial:
            _do_locked(start, lock_type=lock_type)
        else:
            start()

        return handle
    except Exception as e:
        raise DHError(e, "failed to listen to the table.") from e
