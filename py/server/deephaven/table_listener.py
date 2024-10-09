#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
""" This module provides utilities for listening to table changes. """

from abc import ABC, abstractmethod
from functools import wraps
from inspect import signature
from typing import Callable, Union, List, Generator, Dict, Sequence, Optional

import jpy
import numpy

from deephaven import DHError
from deephaven import update_graph
from deephaven._wrapper import JObjectWrapper
from deephaven.jcompat import to_sequence, j_list_to_list
from deephaven.table import Table
from deephaven._table_reader import _table_reader_all_dict, _table_reader_chunk_dict
from deephaven.table_factory import _error_callback_wrapper

_JPythonReplayListenerAdapter = jpy.get_type("io.deephaven.integrations.python.PythonReplayListenerAdapter")
_JTableUpdate = jpy.get_type("io.deephaven.engine.table.TableUpdate")
_JListenerRecorder = jpy.get_type("io.deephaven.engine.table.impl.ListenerRecorder")
_JPythonMergedListenerAdapter = jpy.get_type("io.deephaven.integrations.python.PythonMergedListenerAdapter")

_DEFAULT_ON_ERROR_CALLBACK = lambda e : print(f"An error occurred during table update processing: {e}")

class TableUpdate(JObjectWrapper):
    """A TableUpdate object represents a table update event.  It contains the added, removed, and modified rows in the
    table. """
    j_object_type = _JTableUpdate

    def __init__(self, table: Table, j_table_update: jpy.JType):
        self.table = table
        self.j_table_update = j_table_update
        # make sure we always use the _JTableUpdate interface and not the implementations
        self.j_table_update = jpy.cast(j_table_update, _JTableUpdate)

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
        if not (added := self.j_table_update.added()):
            return {}

        return _table_reader_all_dict(table=self.table, cols=cols, row_set= added.asRowSet(),
                                          prev=False, to_numpy=True)

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
        if not (added := self.j_table_update.added()):
            return (_ for _ in ())

        return _table_reader_chunk_dict(table=self.table, cols=cols, row_set=added.asRowSet(),
                                        chunk_size=chunk_size, prev=False)

    def removed(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """Returns a dict with each key being a column name and each value being a NumPy array of
        all the removed rows in the columns.

        Args:
             cols (Union[str, List[str]): the column(s) for which to return the added rows

        Returns:
            a dict
        """
        if not (removed := self.j_table_update.removed()):
            return {}

        return _table_reader_all_dict(table=self.table, cols=cols, row_set=removed.asRowSet(),
                                      prev=True)

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
        if not (removed := self.j_table_update.removed()):
            return (_ for _ in ())

        return _table_reader_chunk_dict(table=self.table, cols=cols, row_set=removed.asRowSet(),
                                        chunk_size=chunk_size, prev=True)

    def modified(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """Returns a dict with each key being a column name and each value being a NumPy array of the current values of
        all the modified rows in the columns.

        Args:
             cols (Union[str, List[str]): the column(s) for which to return the added rows

        Returns:
            a dict
        """
        if not (modified := self.j_table_update.modified()):
            return {}

        return _table_reader_all_dict(table=self.table, cols=cols, row_set=modified.asRowSet(),
                                          prev=False, to_numpy=True)

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
        if not (modified := self.j_table_update.modified()):
            return (_ for _ in ())

        return _table_reader_chunk_dict(self.table, cols=cols, row_set=modified.asRowSet(),
                                        chunk_size=chunk_size, prev=False)

    def modified_prev(self, cols: Union[str, List[str]] = None) -> Dict[str, numpy.ndarray]:
        """Returns a dict with each key being a column name and each value being a NumPy array of the previous values of
        all the modified rows in the columns.

        Args:
             cols (Union[str, List[str]): the column(s) for which to return the added rows

        Returns:
            a dict
        """
        if not (modified := self.j_table_update.modified()):
            return {}

        return _table_reader_all_dict(table=self.table, cols=cols, row_set=modified.asRowSet(),
                                          prev=True, to_numpy=True)

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
        if not (modified := self.j_table_update.modified()):
            return (_ for _ in ())

        return _table_reader_chunk_dict(self.table, cols=cols, row_set=modified.asRowSet(),
                                        chunk_size=chunk_size, prev=True)

    @property
    def shifted(self):
        raise NotImplemented("shifts are not supported yet.")

    @property
    def modified_columns(self) -> List[str]:
        """The list of modified columns in this update."""
        cols = self.j_table_update.modifiedColumnSet().dirtyColumnNames()

        return list(cols) if cols else []


class TableListener(ABC):
    """An abstract table listener class that should be subclassed by any user table listener class. It provides a
    default implementation for the on_error method that simply prints out the error."""

    @abstractmethod
    def on_update(self, update: TableUpdate, is_replay: bool) -> None:
        """The required method on a listener object that receives table updates."""
        ...

    def on_error(self, e: Exception) -> None:
        """The callback method on a listener object that handles the received error. The default implementation simply prints the error.

        Args:
            e (Exception): the exception that occurred during the listener's execution.
        """
        print(f"An error occurred during table update processing: {self}, {e}")


def _listener_wrapper(table: Table):
    """A decorator to wrap a user listener function or on_update method to receive the numpy-converted Table updates.

    Args:
        table (Table): the table to listen for updates.
    """

    def decorator(listener: Callable[[TableUpdate, bool], None]):
        @wraps(listener)
        def wrapper(update, *args):
            t_update = TableUpdate(table=table, j_table_update=update)
            listener(t_update, args[0])

        return wrapper

    return decorator

def _wrap_listener_func(t: Table, listener: Callable[[TableUpdate, bool], None]):
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

    def __init__(self, t: Table, listener: Union[Callable[[TableUpdate, bool], None], TableListener], description: str = None,
                 dependencies: Union[Table, Sequence[Table]] = None, on_error: Callable[[Exception], None] = None):
        """Creates a new table listener handle with dependencies.

        Table change events are processed by 'listener', which can be either
        (1) a callable (e.g. function) or
        (2) an instance of a TableListener subclass that must override the abstract "on_update" method, and optionally
        override the default "on_error" method.

        The callable or the on_update method must have the following signatures.
        * (update: TableUpdate, is_replay: bool): support replaying the initial table snapshot and normal table updates
        The 'update' parameter is an object that describes the table update;
        The 'is_replay' parameter is used only by replay listeners, it is set to 'True' when replaying the initial
        snapshot and 'False' during normal updates.

        Note: Don't do table operations in the listener. Do them beforehand, and add the results as dependencies.

        Args:
            t (Table): table to listen to
            listener (Union[Callable[[TableUpdate, bool], None], TableListener]): listener for table changes
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
            on_error (Callable[[Exception], None]): a callback function to be invoked when an error occurs during the
                listener's execution. It should only be set when the listener is a function, not when it is an instance
                of TableListener. When the listener is a TableListener, TableListener.on_error will be used.
                Defaults to None. When None, a default callback function will be provided that simply
                prints out the received exception. If the callback function itself raises an exception, the new exception
                will be logged in the Deephaven server log and will not be further processed by the server.

        Raises:
            DHError
        """
        if not t.is_refreshing:
            raise DHError(message="table must be a refreshing table.")

        self.t = t
        self.description = description
        self.dependencies = to_sequence(dependencies)

        if isinstance(listener, TableListener):
            if on_error:
                raise DHError(message="Invalid on_error argument for listeners of TableListener type which already have an on_error method.")
            self.listener_wrapped = _wrap_listener_obj(t, listener)
            on_error_callback = _error_callback_wrapper(listener.on_error)
        elif callable(listener):
            self.listener_wrapped = _wrap_listener_func(t, listener)
            if on_error:
                on_error_callback = _error_callback_wrapper(on_error)
            else:
                on_error_callback = _error_callback_wrapper(_DEFAULT_ON_ERROR_CALLBACK)
        else:
            raise DHError(message="listener is neither callable nor TableListener object")

        try:
            self.listener_adapter = _JPythonReplayListenerAdapter.create(description, t.j_table, False,
                                                                         self.listener_wrapped, on_error_callback,
                                                                         self.dependencies)
        except Exception as e:
            raise DHError(e, "failed to create a table listener.") from e
        self.started = False

    @property
    def j_object(self) -> jpy.JType:
        return self.listener_adapter

    def start(self, do_replay: bool = False) -> None:
        """Start the listener by registering it with the table and listening for updates.

        Args:
            do_replay (bool): whether to replay the initial snapshot of the table, default is False

        Raises:
            DHError
        """
        if self.started:
            raise RuntimeError("Attempting to start an already started listener..")

        try:
            with update_graph.auto_locking_ctx(self.t):
                if do_replay:
                    self.listener_adapter.replay()

                self.t.j_table.addUpdateListener(self.listener_adapter)
        except Exception as e:
           raise DHError(e, "failed to listen to the table changes.") from e

        self.started = True

    def stop(self) -> None:
        """Stop the listener by de-registering it from the table and stop listening for updates."""

        if not self.started:
            return
        self.t.j_table.removeUpdateListener(self.listener_adapter)
        self.started = False


def listen(t: Table, listener: Union[Callable[[TableUpdate, bool], None], TableListener], description: str = None, do_replay: bool = False,
           dependencies: Union[Table, Sequence[Table]] = None, on_error: Callable[[Exception], None] = None) -> TableListenerHandle:
    """This is a convenience function that creates a TableListenerHandle object and immediately starts it to listen
    for table updates.

    The function returns the created TableListenerHandle object whose 'stop' method can be called to stop listening.
    If it goes out of scope and is garbage collected, the listener will stop receiving any table updates.

    Note: Don't do table operations in the listener. Do them beforehand, and add the results as dependencies.

    Args:
        t (Table): table to listen to
        listener (Union[Callable[[TableUpdate, bool], None], TableListener]): listener for table changes
        description (str, optional): description for the UpdatePerformanceTracker to append to the listener's entry
            description, default is None
        do_replay (bool): whether to replay the initial snapshot of the table, default is False
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
        on_error (Callable[[Exception], None]): a callback function to be invoked when an error occurs during the
            listener's execution. It should only be set when the listener is a function, not when it is an instance
            of TableListener. When the listener is a TableListener, TableListener.on_error will be used.
            Defaults to None. When None, a default callback function will be provided that simply
            prints out the received exception. If the callback function itself raises an exception, the new exception
            will be logged in the Deephaven server log and will not be further processed by the server.

    Returns:
        a TableListenerHandle

    Raises:
        DHError
    """
    table_listener_handle = TableListenerHandle(t=t, listener=listener, description=description,
                                                dependencies=dependencies,  on_error=on_error)
    table_listener_handle.start(do_replay=do_replay)
    return table_listener_handle


class _ListenerRecorder (JObjectWrapper):
    """A ListenerRecorder object records the table updates and notifies the associated MergedListener that a change
    has occurred."""

    j_object_type = _JListenerRecorder

    @property
    def j_object(self) -> jpy.JType:
        return self.j_listener_recorder

    def __init__(self, table: Table):
        if not table.is_refreshing:
            raise DHError(message="table must be a refreshing table.")

        self.j_listener_recorder = _JListenerRecorder("Python Wrapped Listener recorder", table.j_table, None)
        self.table = table

    def table_update(self) -> Optional[TableUpdate]:
        """Gets the table update from the listener recorder. If there is no update in the current update graph cycle,
        returns None.

        Returns:
            a TableUpdate or None
        """
        j_table_update = self.j_listener_recorder.getUpdate()
        return TableUpdate(self.table, j_table_update) if j_table_update else None


class MergedListener(ABC):
    """An abstract multi-table listener class that should be subclassed by any user multi-table listener class. It
    provides a default implementation for the on_error method that simply prints out the error."""

    @abstractmethod
    def on_update(self, updates: Dict[Table, TableUpdate], is_replay: bool) -> None:
        """The required method on a listener object that receives table updates from the
        tables that are listened to.
        """
        ...

    def on_error(self, e: Exception) -> None:
        """ The callback method on a listener object that handles the received error. The default implementation simply prints the error.

        Args:
            e (Exception): the exception that occurred during the listener's execution.
        """
        print(f"An error occurred during table update processing: {self}, {e}")


class MergedListenerHandle(JObjectWrapper):
    """A handle to manage a merged listener's lifecycle."""
    j_object_type = _JPythonMergedListenerAdapter

    @property
    def j_object(self) -> jpy.JType:
        return self.merged_listener_adapter

    def __init__(self, tables: Sequence[Table], listener: Union[Callable[[Dict[Table, TableUpdate], bool], None], MergedListener],
                 description: str = None, dependencies: Union[Table, Sequence[Table]] = None, on_error: Callable[[Exception], None] = None):
        """Creates a new MergedListenerHandle with the provided listener recorders and dependencies.

        Table change events are processed by 'listener', which can be either
        (1) a callable (e.g. function) or
        (2) an instance of a MergedListener subclass that must override the abstract "on_update" method, and optionally
        override the default "on_error" method.

        The callable or the on_update method must have the following signature.
        *(updates: Dict[Table, TableUpdate], is_replay: bool): support replaying the initial table snapshots and normal table updates
        The 'updates' parameter is a dictionary of Table to TableUpdate;
        The 'is_replay' parameter is used only by replay listeners, it is set to 'True' when replaying the initial
        snapshots and 'False' during normal updates.


        Note: Don't do table operations in the listener. Do them beforehand, and add the results as dependencies.

        Args:
            tables (Sequence[Table]): tables to listen to
            listener (Union[Callable[[Dict[Table, TableUpdate], bool], None], MergedListener]): listener to process table updates
                from the tables.
            description (str, optional): description for the UpdatePerformanceTracker to append to the listener's entry
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
            on_error (Callable[[Exception], None]): a callback function to be invoked when an error occurs during the
                listener's execution. It should only be set when the listener is a function, not when it is an instance
                of MergedListener. When the listener is a MergedListener, MergedListener.on_error will be used.
                Defaults to None. When None, a default callback function will be provided that simply
                prints out the received exception. If the callback function itself raises an exception, the new exception
                will be logged in the Deephaven server log and will not be further processed by the server.

        Raises:
            DHError
        """
        if len(tables) < 2:
            raise DHError(message="A merged listener must listen to at least 2 refreshing tables.")

        self.tables = tables
        self.listener_recorders = [_ListenerRecorder(t) for t in tables]

        self.dependencies = dependencies

        if isinstance(listener, MergedListener):
            if on_error:
                raise DHError(message="Invalid on_error argument for listeners of MergedListener type which already have an on_error method.")
            self.listener = listener.on_update
            on_error_callback = _error_callback_wrapper(listener.on_error)
        elif callable(listener):
            self.listener = listener
            if on_error:
                on_error_callback = _error_callback_wrapper(on_error)
            else:
                on_error_callback = _error_callback_wrapper(_DEFAULT_ON_ERROR_CALLBACK)
        else:
            raise DHError(message="listener is neither callable nor MergedListener object")

        n_params = len(signature(self.listener).parameters)
        if n_params != 2:
            raise ValueError("merged listener function must have 2 parameters (updates, is_replay).")

        try:
            self.merged_listener_adapter = _JPythonMergedListenerAdapter.create(
                        to_sequence(self.listener_recorders),
                        to_sequence(self.dependencies),
                        description,
                        self,
                        on_error_callback)
            self.started = False
        except Exception as e:
            raise DHError(e, "failed to create a merged listener adapter.") from e

    def _process(self) -> None:
        """Process the table updates from the listener recorders. """
        self.listener({lr.table: lr.table_update() for lr in self.listener_recorders}, False)

    def start(self, do_replay: bool = False) -> None:
        """Start the listener by registering it with the tables and listening for updates.

        Args:
            do_replay (bool): whether to replay the initial snapshots of the tables, default is False

        Raises:
            DHError
        """
        if self.started:
            raise RuntimeError("Attempting to start an already started merged listener..")

        try:
            # self.tables[0] is guaranteed to be a refreshing table, the lock is needed to add all the listener recorders
            # on the same update graph cycle and if replay is requested, the initial snapshots of the tables are all
            # taken on the same update graph cycle as well.
            with update_graph.auto_locking_ctx(self.tables[0]):
                if do_replay:
                    j_replay_updates = self.merged_listener_adapter.currentRowsAsUpdates()
                    replay_updates = {t: TableUpdate(t, tu) for t, tu in zip(self.tables, j_list_to_list(j_replay_updates))}
                    try:
                        self.listener(replay_updates, True)
                    finally:
                        for replay_update in replay_updates.values():
                            replay_update.j_object.release()

                for lr in self.listener_recorders:
                    lr.table.j_table.addUpdateListener(lr.j_listener_recorder)
        except Exception as e:
            raise DHError(e, "failed to listen to the table changes.") from e

        self.started = True

    def stop(self) -> None:
        """Stop the listener."""
        if not self.started:
            return

        # self.tables[0] is guaranteed to be a refreshing table, the lock is needed to remove all the listener recorders
        # on the same update graph cycle
        with update_graph.auto_locking_ctx(self.tables[0]):
            for lr in self.listener_recorders:
                lr.table.j_table.removeUpdateListener(lr.j_listener_recorder)
            self.started = False


def merged_listen(tables: Sequence[Table], listener: Union[Callable[[Dict[Table, TableUpdate]], None], MergedListener],
                do_replay: bool = False, description: str = None, dependencies: Union[Table, Sequence[Table]] = None,
                  on_error: Callable[[Exception], None] = None) -> MergedListenerHandle:
    """This is a convenience function that creates a MergedListenerHandle object and immediately starts it to
    listen for table updates.

    The function returns the created MergedListenerHandle object whose 'stop' method can be called to stop
    listening. If it goes out of scope and is garbage collected, the listener will stop receiving any table updates.

    Note: Don't do table operations in the listener. Do them beforehand, and add the results as dependencies.

    Args:
        tables (Sequence[Table]): tables to listen to.
        listener (Union[Callable[[Dict[Table, TableUpdate]], None], MergedListener]): listener to process table updates
            from the tables.
        description (str, optional): description for the UpdatePerformanceTracker to append to the listener's entry
            description, default is None
        do_replay (bool): whether to replay the initial snapshots of the tables, default is False
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
        on_error (Callable[[Exception], None]): a callback function to be invoked when an error occurs during the
            listener's execution. It should only be set when the listener is a function, not when it is an instance
            of MergedListener. When the listener is a MergedListener, MergedListener.on_error will be used.
            Defaults to None. When None, a default callback function will be provided that simply
            prints out the received exception. If the callback function itself raises an exception, the new exception
            will be logged in the Deephaven server log and will not be further processed by the server.
    """
    merged_listener_handle = MergedListenerHandle(tables=tables, listener=listener,
                                                  description=description, dependencies=dependencies, on_error=on_error)
    merged_listener_handle.start(do_replay=do_replay)
    return merged_listener_handle
