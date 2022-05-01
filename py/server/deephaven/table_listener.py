#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module provides utilities for listening to table changes. """
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, Union, Type, Sequence, List, Generator, Dict

import jpy
import numpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.column import Column
from deephaven.jcompat import to_sequence
from deephaven.numpy import column_to_numpy_array
from deephaven.table import Table

_JPythonListenerAdapter = jpy.get_type("io.deephaven.integrations.python.PythonListenerAdapter")
_JPythonReplayListenerAdapter = jpy.get_type("io.deephaven.integrations.python.PythonReplayListenerAdapter")
_JTableUpdate = jpy.get_type("io.deephaven.engine.table.TableUpdate")
_JTableUpdateDataReader = jpy.get_type("io.deephaven.integrations.python.PythonListenerTableUpdateDataReader")


def _changes_to_numpy(table: Table, col_defs: List[Column], row_set, chunk_size: int, prev: bool = False) -> Generator[
    Dict[str, numpy.ndarray], None, None]:
    row_sequence_iterator = row_set.getRowSequenceIterator()
    col_sources = [table.j_table.getColumnSource(col_def.name) for col_def in col_defs]
    chunk_size = row_set.size() if not chunk_size else chunk_size
    j_reader_context = _JTableUpdateDataReader.makeContext(chunk_size, *col_sources)
    try:
        while row_sequence_iterator.hasMore():
            chunk_row_set = row_sequence_iterator.getNextRowSequenceWithLength(chunk_size)

            j_array = _JTableUpdateDataReader.readChunkColumnMajor(j_reader_context, chunk_row_set, col_sources, prev)

            col_dict = {}
            for i, col_def in enumerate(col_defs):
                np_array = column_to_numpy_array(col_def, j_array[i])
                col_dict[col_def.name] = np_array

            yield col_dict
    finally:
        j_reader_context.close()
        row_sequence_iterator.close()


def _col_defs(table: Table, cols: Union[str, List[str]]) -> List[Column]:
    if not cols:
        col_defs = table.columns
    else:
        cols = to_sequence(cols)
        col_defs = [col for col in table.columns if col.name in cols]

    return col_defs


class TableUpdate(JObjectWrapper):
    j_object_type = _JTableUpdate

    def __init__(self, table: Table, j_table_update):
        self.table = table
        self.j_table_update = j_table_update
        self.chunk_size = None

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table_update

    def added(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """TODO"""
        if not self.j_table_update.added:
            return {}

        col_defs = _col_defs(table=self.table, cols=cols)
        try:
            return next(
                _changes_to_numpy(table=self.table, col_defs=col_defs, row_set=self.j_table_update.added.asRowSet(),
                                  chunk_size=None))
        except StopIteration:
            return {}

    def added_chunks(self, cols: Union[str, List[str]] = None) -> Generator[Dict[str, numpy.ndarray], None, None]:
        """TODO"""
        if not self.j_table_update.added:
            return (_ for _ in ())

        col_defs = _col_defs(table=self.table, cols=cols)
        return _changes_to_numpy(table=self.table, col_defs=col_defs, row_set=self.j_table_update.added.asRowSet(),
                                 chunk_size=self.chunk_size)

    def removed(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """TODO"""
        if not self.j_table_update.removed:
            return {}

        col_defs = _col_defs(table=self.table, cols=cols)
        try:
            return next(
                _changes_to_numpy(table=self.table, col_defs=col_defs, row_set=self.j_table_update.removed.asRowSet(),
                                  chunk_size=None, prev=True))
        except StopIteration:
            return {}

    def removed_chunks(self, cols: Union[str, List[str]] = None) -> Generator[Dict[str, numpy.ndarray], None, None]:
        """TODO"""
        if not self.j_table_update.removed:
            return (_ for _ in ())

        col_defs = _col_defs(table=self.table, cols=cols)
        return _changes_to_numpy(table=self.table, col_defs=col_defs, row_set=self.j_table_update.removed.asRowSet(),
                                 chunk_size=self.chunk_size, prev=True)

    def modified(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """TODO"""
        if not self.j_table_update.modified:
            return {}

        col_defs = _col_defs(table=self.table, cols=cols)
        try:
            return next(
                _changes_to_numpy(self.table, col_defs=col_defs, row_set=self.j_table_update.modified.asRowSet(),
                                  chunk_size=None))
        except StopIteration:
            return {}

    def modified_chunks(self, cols: Union[str, List[str]] = None) -> Generator[Dict[str, numpy.ndarray], None, None]:
        """TODO"""
        if not self.j_table_update.modified:
            return (_ for _ in ())

        col_defs = _col_defs(table=self.table, cols=cols)
        return _changes_to_numpy(self.table, col_defs=col_defs, row_set=self.j_table_update.modified.asRowSet(),
                                 chunk_size=self.chunk_size)

    def modified_prev(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """TODO"""
        if not self.j_table_update.modified:
            return {}

        col_defs = _col_defs(table=self.table, cols=cols)
        try:
            return next(
                _changes_to_numpy(self.table, col_defs=col_defs, row_set=self.j_table_update.modified.asRowSet(),
                                  chunk_size=None, prev=True))
        except StopIteration:
            return {}

    def modified_prev_chunks(self, cols: Union[str, List[str]] = None) -> Generator[
            Dict[str, numpy.ndarray], None, None]:
        """TODO"""
        if not self.j_table_update.modified:
            return (_ for _ in ())

        col_defs = _col_defs(table=self.table, cols=cols)
        return _changes_to_numpy(self.table, col_defs=col_defs, row_set=self.j_table_update.modified.asRowSet(),
                                 chunk_size=self.chunk_size, prev=True)

    @property
    def shifted(self):
        # return self.j_table_update.shifted
        raise NotImplemented("shifts are not supported yet.")

    @property
    def modified_columns(self) -> List[str]:
        """The list of dirty columns in this update."""
        cols = self.j_table_update.modifiedColumnSet.dirtyColumnNames()

        return list(cols) if cols else []


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


class TableListener(ABC):
    """An abstract table listener class that should be subclassed by any user table listener class."""

    @abstractmethod
    def on_update(self, update: TableUpdate) -> None:
        ...


class TableReplayListener(ABC):
    """An abstract table replay listener class that should be subclassed by any user table replay listener class."""

    @abstractmethod
    def on_update(self, update: TableUpdate, is_replay: bool) -> None:
        ...


def listener_wrapper(table: Table):
    def decorator(listener: Callable):
        @wraps(listener)
        def wrapper(update, *args):
            sig = inspect.signature(listener)
            n_params = len(sig.parameters)
            t_update = TableUpdate(table=table, j_table_update=update)

            if n_params == 2:
                listener(t_update, args[0])
            else:
                listener(t_update)

        return wrapper

    return decorator


def _wrap_listener_func(t: Table, description: str, listener: Callable, replay_initial: bool, retain: bool):
    n_params = len(signature(listener).parameters)

    if n_params not in {1, 2}:
        raise ValueError("listener must take 1 (update) or 2 (isReplay, update) arguments.")
    if n_params == 1 and replay_initial:
        raise ValueError(f"Listener does not support replay: nargs={n_params}")

    listener = listener_wrapper(table=t)(listener)

    if n_params == 1:
        return _JPythonListenerAdapter(description, t.j_table, retain, listener)
    else:
        return _JPythonReplayListenerAdapter(description, t.j_table, retain, listener)


def _wrap_listener_obj(t: Table, description: str, listener: TableListener, replay_initial: bool, retain: bool):
    n_params = len(signature(listener.on_update).parameters)

    if n_params not in {1, 2}:
        raise ValueError(f"listener must take 1 (update) or 2 (isReplay, update) arguments.")
    if n_params == 1 and replay_initial:
        raise ValueError(f"Listener does not support replay: nargs={n_params}")

    listener.on_update = listener_wrapper(table=t)(listener.on_update)
    if n_params == 1:
        return _JPythonListenerAdapter(description, t.j_table, retain, listener)
    else:
        return _JPythonReplayListenerAdapter(description, t.j_table, retain, listener)


def listen(
        t: Table,
        listener: Union[Callable, Type[TableListener], Type[TableReplayListener]],
        description: str = None,
        *,
        retain: bool = True,
        start_listening: bool = True,
        replay_initial: bool = False,
        lock_type: str = "shared",
) -> TableListenerHandle:
    """Listen to table changes.

    Table change events are processed by 'listener', which can be either
        (1) a callable (e.g. function) or
        (2) an instance of either TableListener or TableReplayListener type which provides an "on_update" method.

    The callable or the on_update method must have one of the following two signatures.
        * (update: TableUpdate): receive only normal table updates
        * (update: TableUpdate, is_replay: bool): support replaying the initial table snapshot and normal table updates

        The 'update' parameter is an object that describes the table update;
        The 'is_replay' parameter is used only by replay listeners, it is set to 'true' when replaying the initial
            snapshot and 'false' during normal updates.

    Args:
        t (Table): table to listen to
        listener (Union[Callable, Type[TableListener], Type[TableReplayListener]]): listener for table changes
        description (str): description for the UpdatePerformanceTracker to append to the listener's entry description
        retain (bool): whether a hard reference to this listener should be maintained to prevent it from being
            collected, default is True
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

        if callable(listener):
            listener_adapter = _wrap_listener_func(t, description, listener, replay_initial, retain)
        elif isinstance(listener, (TableListener, TableReplayListener)):
            listener_adapter = _wrap_listener_obj(t, description, listener, replay_initial, retain)
        else:
            raise ValueError("listener is neither callable nor TableListener nor TableReplayListener object")

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
        raise DHError(e, "failed to listen to the table changes.") from e
