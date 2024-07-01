#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
""" This module provides utilities for listening to table changes. """

from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, Union, List, Generator, Dict, Literal, Sequence

import jpy
import numpy

from deephaven import DHError
from deephaven import update_graph
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import to_sequence
from deephaven.table import Table
from deephaven._table_reader import _table_reader_chunks
from deephaven.update_graph import UpdateGraph

_JPythonReplayListenerAdapter = jpy.get_type("io.deephaven.integrations.python.PythonReplayListenerAdapter")
_JTableUpdate = jpy.get_type("io.deephaven.engine.table.TableUpdate")

class TableUpdate(JObjectWrapper):
    """A TableUpdate object represents a table update event.  It contains the added, removed, and modified rows in the
    table. """
    j_object_type = _JTableUpdate

    def __init__(self, table: Table, j_table_update: jpy.JType):
        self.table = table
        self.j_table_update = j_table_update

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table_update

    def added(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """Returns a dict with each key being a column name and each value being a NumPy array of
        all the added rows in the columns.

        Args:
             cols (Union[str, List[str]): the column(s) for which to return the added rows

        Returns:
            a dict
        """
        if not self.j_table_update.added:
            return {}

        try:
            j_row_set = self.j_table_update.added.asRowSet()
            return next(
                _table_reader_chunks(table=self.table, cols=cols, row_set= j_row_set,
                                     prev=False, chunk_size=j_row_set.size(), to_numpy=True))
        except StopIteration:
            return {}

    def added_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> Generator[
        Dict[str, numpy.ndarray], None, None]:
        """Returns a generator that on each iteration, only returns a chunk of added rows in the form of a dict with
        each key being a column name and each value being a NumPy array of the rows in the chunk.

        Args:
            chunk_size (int): the size of the chunk
            cols (Union[str, List[str]]): the columns(s) for which to return the added rows

        Returns:
            a generator
        """
        if not self.j_table_update.added:
            return (_ for _ in ())

        return _table_reader_chunks(table=self.table, cols=cols, row_set=self.j_table_update.added.asRowSet(),
                                    prev=False, chunk_size=chunk_size, to_numpy=True)

    def removed(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """Returns a dict with each key being a column name and each value being a NumPy array of
        all the removed rows in the columns.

        Args:
             cols (Union[str, List[str]): the column(s) for which to return the added rows

        Returns:
            a dict
        """
        if not self.j_table_update.removed:
            return {}

        try:
            j_row_set = self.j_table_update.removed.asRowSet()
            return next(
                _table_reader_chunks(table=self.table, cols=cols, row_set=j_row_set,
                                     chunk_size=j_row_set.size(), prev=True, to_numpy=True))
        except StopIteration:
            return {}

    def removed_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> Generator[
        Dict[str, numpy.ndarray], None, None]:
        """Returns a generator that on each iteration, only returns a chunk of removed rows in the form of a dict with
        each key being a column name and each value being a NumPy array of the rows in the chunk.

        Args:
            chunk_size (int): the size of the chunk
            cols (Union[str, List[str]]): the columns(s) for which to return the added rows

        Returns:
            a generator
        """
        if not self.j_table_update.removed:
            return (_ for _ in ())

        return _table_reader_chunks(table=self.table, cols=cols, row_set=self.j_table_update.removed.asRowSet(),
                                    chunk_size=chunk_size, prev=True, to_numpy=True)

    def modified(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """Returns a dict with each key being a column name and each value being a NumPy array of the current values of
        all the modified rows in the columns.

        Args:
             cols (Union[str, List[str]): the column(s) for which to return the added rows

        Returns:
            a dict
        """
        if not self.j_table_update.modified:
            return {}

        try:
            j_row_set = self.j_table_update.modified.asRowSet()
            return next(
                _table_reader_chunks(self.table, cols=cols, row_set=j_row_set,
                                     chunk_size=j_row_set.size(), prev=False, to_numpy=True))
        except StopIteration:
            return {}

    def modified_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> Generator[
        Dict[str, numpy.ndarray], None, None]:
        """Returns a generator that on each iteration, only returns a chunk of modified rows in the form of a dict with
        each key being a column name and each value being a NumPy array of the current values of the rows in the chunk.

        Args:
            chunk_size (int): the size of the chunk
            cols (Union[str, List[str]]): the columns(s) for which to return the added rows

        Returns:
            a generator
        """
        if not self.j_table_update.modified:
            return (_ for _ in ())

        return _table_reader_chunks(self.table, cols=cols, row_set=self.j_table_update.modified.asRowSet(),
                                    chunk_size=chunk_size, prev=False, to_numpy=True)

    def modified_prev(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """Returns a dict with each key being a column name and each value being a NumPy array of the previous values of
        all the modified rows in the columns.

        Args:
             cols (Union[str, List[str]): the column(s) for which to return the added rows

        Returns:
            a dict
        """
        if not self.j_table_update.modified:
            return {}

        try:
            j_row_set = self.j_table_update.modified.asRowSet()
            return next(
                _table_reader_chunks(self.table, cols=cols, row_set=j_row_set,
                                     chunk_size=j_row_set.size(), prev=True, to_numpy=True))
        except StopIteration:
            return {}

    def modified_prev_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> Generator[
        Dict[str, numpy.ndarray], None, None]:
        """Returns a generator that on each iteration, only returns a chunk of modified rows in the form of a dict with
        each key being a column name and each value being a NumPy array of the previous values of the rows in the chunk.

        Args:
            chunk_size (int): the size of the chunk
            cols (Union[str, List[str]]): the columns(s) for which to return the added rows

        Returns:
            a generator
        """
        if not self.j_table_update.modified:
            return (_ for _ in ())

        return _table_reader_chunks(self.table, cols=cols, row_set=self.j_table_update.modified.asRowSet(),
                                    chunk_size=chunk_size, prev=True, to_numpy=True)

    @property
    def shifted(self):
        raise NotImplemented("shifts are not supported yet.")

    @property
    def modified_columns(self) -> List[str]:
        """The list of modified columns in this update."""
        cols = self.j_table_update.modifiedColumnSet.dirtyColumnNames()

        return list(cols) if cols else []


def _do_locked(ug: Union[UpdateGraph, Table], f: Callable, lock_type: Literal["shared","exclusive"] = "shared") -> \
        None:
    """Executes a function while holding the UpdateGraph (UG) lock.  Holding the UG lock
    ensures that the contents of a table will not change during a computation, but holding
    the lock also prevents table updates from happening.  The lock should be held for as little
    time as possible.

    Args:
        ug (Union[UpdateGraph, Table]): The Update Graph (UG) or a table-like object.
        f (Callable): callable to execute while holding the UG lock, could be function or an object with an 'apply'
            attribute which is callable
        lock_type (str): UG lock type, valid values are "exclusive" and "shared".  "exclusive" allows only a single
            reader or writer to hold the lock.  "shared" allows multiple readers or a single writer to hold the lock.
    Raises:
        ValueError
    """
    if isinstance(ug, Table):
        ug = ug.update_graph

    if lock_type == "exclusive":
        with update_graph.exclusive_lock(ug):
            f()
    elif lock_type == "shared":
        with update_graph.shared_lock(ug):
            f()
    else:
        raise ValueError(f"Unsupported lock type: lock_type={lock_type}")


class TableListener(ABC):
    """An abstract table listener class that should be subclassed by any user table listener class."""

    @abstractmethod
    def on_update(self, update: TableUpdate, is_replay: bool) -> None:
        """The required method on a listener object that receives table updates."""
        ...


def _listener_wrapper(table: Table):
    """A decorator to wrap a user listener function or on_update method to receive the numpy-converted Table updates.

    Args:
        table (Table): the table to listen for updates.
    """

    def decorator(listener: Callable):
        @wraps(listener)
        def wrapper(update, *args):
            t_update = TableUpdate(table=table, j_table_update=update)
            listener(t_update, args[0])

        return wrapper

    return decorator


def _wrap_listener_func(t: Table, listener: Callable):
    n_params = len(signature(listener).parameters)
    if n_params != 2:
        raise ValueError("listener function must have 2 (update, is_replay) parameters.")
    return _listener_wrapper(table=t)(listener)


def _wrap_listener_obj(t: Table, listener: TableListener):
    n_params = len(signature(listener.on_update).parameters)
    if n_params != 2:
        raise ValueError(f"The on_update method must have 2 (update, is_replay) parameters.")
    listener.on_update = _listener_wrapper(table=t)(listener.on_update)
    return listener


class TableListenerHandle(JObjectWrapper):
    """A handle to manage a table listener's lifecycle."""
    j_object_type = _JPythonReplayListenerAdapter

    def __init__(self, t: Table, listener: Union[Callable, TableListener], description: str = None,
                 dependencies: Union[Table, Sequence[Table]] = None):
        """Creates a new table listener handle with dependencies.

        Table change events are processed by 'listener', which can be either
        (1) a callable (e.g. function) or
        (2) an instance of TableListener type which provides an "on_update" method.

        The callable or the on_update method must have the following signatures.
        * (update: TableUpdate, is_replay: bool): support replaying the initial table snapshot and normal table updates
        The 'update' parameter is an object that describes the table update;
        The 'is_replay' parameter is used only by replay listeners, it is set to 'true' when replaying the initial
        snapshot and 'false' during normal updates.

        Note: Don't do table operations in the listener. Do them beforehand, and add the results as dependencies.

        Args:
            t (Table): table to listen to
            listener (Union[Callable, TableListener]): listener for table changes
            description (str, optional): description for the UpdatePerformanceTracker to append to the listener's entry
                description, default is None
            dependencies (Union[Table, Sequence[Table]]): tables that must be satisfied before the listener's execution.
                A refreshing table is considered to be satisfied if all possible updates to the table have been processed
                in the current update graph cycle. A static table is always considered to be satisfied. If a specified
                table is refreshing, it must belong to the same update graph as the table being listened to. Default is
                None.

                Dependencies ensure that the listener can safely access the dependent tables during its execution. This
                mainly includes reading the data from the tables. While performing operations on the dependent tables in
                the listener is safe, it is not recommended because reading or operating on the result tables of those
                operations may not be safe. It is best to perform the operations on the dependent tables beforehand,
                and then add the result tables as dependencies to the listener so that they can be safely read in it.

        Raises:
            DHError
        """
        self.t = t
        self.description = description
        self.dependencies = to_sequence(dependencies)

        if callable(listener):
            self.listener_wrapped = _wrap_listener_func(t, listener)
        elif isinstance(listener, TableListener):
            self.listener_wrapped = _wrap_listener_obj(t, listener)
        else:
            raise DHError(message="listener is neither callable nor TableListener object")

        try:
            self.listener = _JPythonReplayListenerAdapter.create(description, t.j_table, False, self.listener_wrapped, self.dependencies)
        except Exception as e:
            raise DHError(e, "failed to create a table listener.") from e
        self.started = False

    @property
    def j_object(self) -> jpy.JType:
        return self.listener

    def start(self, do_replay: bool = False, replay_lock: Literal["shared", "exclusive"] = "shared") -> None:
        """Start the listener by registering it with the table and listening for updates.

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

                self.t.j_table.addUpdateListener(self.listener)

            if do_replay:
                _do_locked(self.t, _start, lock_type=replay_lock)
            else:
                _start()
        except Exception as e:
            raise DHError(e, "failed to listen to the table changes.") from e

        self.started = True

    def stop(self) -> None:
        """Stop the listener by de-registering it from the table and stop listening for updates."""

        if not self.started:
            return
        self.t.j_table.removeUpdateListener(self.listener)
        self.started = False


def listen(t: Table, listener: Union[Callable, TableListener], description: str = None, do_replay: bool = False,
           replay_lock: Literal["shared", "exclusive"] = "shared", dependencies: Union[Table, Sequence[Table]] = None)\
        -> TableListenerHandle:
    """This is a convenience function that creates a TableListenerHandle object and immediately starts it to listen
    for table updates.

    The function returns the created TableListenerHandle object whose 'stop' method can be called to stop listening.
    If it goes out of scope and is garbage collected, the listener will stop receiving any table updates.

    Note: Don't do table operations in the listener. Do them beforehand, and add the results as dependencies.

    Args:
        t (Table): table to listen to
        listener (Union[Callable, TableListener]): listener for table changes
        description (str, optional): description for the UpdatePerformanceTracker to append to the listener's entry
            description, default is None
        do_replay (bool): whether to replay the initial snapshot of the table, default is False
        replay_lock (str): the lock type used during replay, default is 'shared', can also be 'exclusive'
        dependencies (Union[Table, Sequence[Table]]): tables that must be satisfied before the listener's execution.
            A refreshing table is considered to be satisfied if all possible updates to the table have been processed
            in the current update graph cycle. A static table is always considered to be satisfied. If a specified
            table is refreshing, it must belong to the same update graph as the table being listened to. Default is
            None.

            Dependencies ensure that the listener can safely access the dependent tables during its execution. This
            mainly includes reading the data from the tables. While performing operations on the dependent tables in
            the listener is safe, it is not recommended because reading or operating on the result tables of those
            operations may not be safe. It is best to perform the operations on the dependent tables beforehand,
            and then add the result tables as dependencies to the listener so that they can be safely read in it.

    Returns:
        a TableListenerHandle

    Raises:
        DHError
    """
    table_listener_handle = TableListenerHandle(t=t, dependencies=dependencies, listener=listener,
                                                                    description=description)
    table_listener_handle.start(do_replay=do_replay, replay_lock=replay_lock)
    return table_listener_handle
