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
    throwing_runnable = jpy.get_type("io.deephaven.integrations.python.PythonThrowingRunnable")
    update_graph_processor = jpy.get_type("io.deephaven.engine.updategraph.UpdateGraphProcessor")

    if lock_type == "exclusive":
        update_graph_processor.DEFAULT.exclusiveLock().doLocked(throwing_runnable(f))
    elif lock_type == "shared":
        update_graph_processor.DEFAULT.sharedLock().doLocked(throwing_runnable(f))
    else:
        raise ValueError(f"Unsupported lock type: lock_type={lock_type}")


class TableListener(ABC):
    """An abstract table listener class that should be subclassed by any user table listener class."""

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


def _wrap_listener_func(t: Table, description: str, listener: Callable):
    n_params = len(signature(listener).parameters)
    if n_params != 2:
        raise ValueError("listener must have 2 (update, is_replay) parameters.")
    return listener_wrapper(table=t)(listener)


def _wrap_listener_obj(t: Table, description: str, listener: TableListener):
    n_params = len(signature(listener.on_update).parameters)
    if n_params != 2:
        raise ValueError(f"The on_update method must have 2 (update, is_replay) parameters.")
    listener.on_update = listener_wrapper(table=t)(listener.on_update)
    return listener


def listen(t: Table, listener: Union[Callable, TableListener], description: str = None, do_replay: bool = False,
           replay_lock: str = "shared"):
    """This is a convenience function that creates a TableListenerHandle object and immediately starts it to listen
    for table updates.

    The function returns the created TableListenerHandle object whose 'stop' method can be called to stop listening.
    If it goes out of scope and is garbage collected,

    Args:
        t (Table): table to listen to
        listener (Union[Callable, TableListener]): listener for table changes
        description (str, optional): description for the UpdatePerformanceTracker to append to the listener's entry
            description, default is None
        do_replay (bool): whether to replay the initial snapshot of the table, default is False
        replay_lock (str): the lock type used during replay, default is 'shared', can also be 'exclusive'

    Returns:
        a TableListenerHandle

    Raises:
        DHError
    """
    table_listener_handle = TableListenerHandle(t=t, listener=listener, description=description)
    table_listener_handle.start(do_replay=do_replay, replay_lock=replay_lock)
    return table_listener_handle


class TableListenerHandle:
    """A handle for a table listener."""

    def __init__(self, t: Table, listener: Union[Callable, TableListener], description: str = None):
        """Creates a new table listener handle.

        Table change events are processed by 'listener', which can be either
        (1) a callable (e.g. function) or
        (2) an instance of TableListener type which provides an "on_update" method.

        The callable or the on_update method must have one of the following two signatures.
        * (update: TableUpdate, is_replay: bool): support replaying the initial table snapshot and normal table updates
        The 'update' parameter is an object that describes the table update;
        The 'is_replay' parameter is used only by replay listeners, it is set to 'true' when replaying the initial
            snapshot and 'false' during normal updates.

        Args:
            t (Table): table to listen to
            listener (Union[Callable, TableListener]): listener for table changes
            description (str, optional): description for the UpdatePerformanceTracker to append to the listener's entry
                description, default is None

        Raises:
            ValueError
        """
        self.t = t

        if callable(listener):
            listener_wrapped = _wrap_listener_func(t, description, listener)
        elif isinstance(listener, TableListener):
            listener_wrapped = _wrap_listener_obj(t, description, listener)
        else:
            raise ValueError("listener is neither callable nor TableListener nor TableReplayListener object")
        self.listener = _JPythonReplayListenerAdapter(description, t.j_table, False, listener_wrapped)

        self.started = False

    def start(self, do_replay: bool = False, replay_lock: str = "shared") -> None:
        """Start the listener by registering it with the table and listening for updates.

        lock_type (str): UGP lock type, used when replay_initial=True, valid values are "exclusive" and "shared".
            Default is "shared".

        Args:
            do_replay (bool): whether to replay the initial snapshot of the table, default is False
            replay_lock (str): the lock type used during replay, default is 'shared', can also be 'exclusive'.

        Raises:
            DHError
        """
        if self.started:
            raise RuntimeError("Attempting to start an already started listener..")

        try:
            def _start():
                if do_replay:
                    self.listener.replay()

                self.t.j_table.listenForUpdates(self.listener)

            if do_replay:
                _do_locked(_start, lock_type=replay_lock)
            else:
                _start()
        except Exception as e:
            raise DHError(e, "failed to listen to the table changes.") from e

        self.started = True

    def stop(self) -> None:
        """Stop the listener by de=registering it from the table and stop listening for updates."""

        if not self.started:
            return
        self.t.j_table.removeUpdateListener(self.listener)
        self.started = False
