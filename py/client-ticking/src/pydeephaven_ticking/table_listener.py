#
#  Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

"""This module provides utilities for listening to table changes."""

from abc import ABC, abstractmethod
from inspect import signature
from typing import Callable, Dict, Generator, List, Tuple, TypeVar, Union
import pyarrow as pa
import pyarrow.flight as flight
import pydeephaven_ticking._core as dhc
import pydeephaven_ticking.util as tick_util
import pydeephaven
from pydeephaven.table import Table
import threading


ColDictType = Dict[str, pa.Array]
"""A dictionary mapping column name to an Arrow Array of the data update."""

DictGeneratorType = Generator[ColDictType, None, None]
"""A Generator yielding dictionaries mapping column name to an Arrow Array
of the data update."""

R = TypeVar("R")
S = TypeVar("S")
T = TypeVar("T")

def _make_generator(table: dhc.ClientTable,
                    rows: dhc.RowSequence,
                    col_names: Union[str, List[str], None],
                    chunk_size: int) -> DictGeneratorType:
    """Repeatedly pulls up to chunk_size elements from the indicated columns of the ClientTable, collects them
    in a dictionary (whose keys are column name and whose values are PyArrow arrays), and yields that dictionary)"""

    col_names = tick_util.canonicalize_cols_param(table, col_names)
    while not rows.empty:
        these_rows = rows.Take(chunk_size)
        rows = rows.Drop(chunk_size)

        result: ColDictType = {}
        for i in range(len(col_names)):
            col_name = col_names[i]
            col = table.get_column_by_name(col_name, True)
            data = col.get_chunk(these_rows)
            result[col_name] = data

        yield result


def _first_or_default(generator: Generator[T, S, R]) -> T:
    """Returns the first (and assumed only) result of the generator, or the empty dictionary if the generator
    yields no values."""

    try:
        return next(generator)

    except StopIteration:
        return {}


class TableUpdate:
    """Represents a set of updates (adds, removes, modifies) that have happened on a table.
    """

    update: dhc.TickingUpdate

    def __init__(self, update: dhc.TickingUpdate):
        """Constructor.

        :param update: The Cython wrapper of the C++ TickingUpdate class.
        """

        self.update = update

    def removed(self, cols: Union[str, List[str]] = None) -> ColDictType:
        """Gets all the data that was removed in this TableUpdate.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A dictionary mapping column name to an Arrow Array of the removed data.
        """

        return _first_or_default(self.removed_chunks(self.update.removed_rows.size, cols))

    def removed_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> DictGeneratorType:
        """Creates a generator yielding chunks of the data that was removed in this TableUpdate.
        :param chunk_size: The maximum number of rows yielded by each iteration of the generator.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A Generator yielding dictionaries mapping column name to an Arrow Array of the removed data. The
          Arrow Arrays will have length less than or equal to chunk_size.
        """

        return _make_generator(self.update.before_removes, self.update.removed_rows, cols, chunk_size)

    def added(self, cols: Union[str, List[str]] = None) -> ColDictType:
        """Gets all the data that was added in this TableUpdate.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A dictionary mapping column name to an Arrow Array of the added data.
        """

        return _first_or_default(self.added_chunks(self.update.added_rows.size, cols))

    def added_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> DictGeneratorType:
        """Creates a generator yielding chunks of the data that was added in this TableUpdate.
        :param chunk_size: The maximum number of rows yielded by each iteration of the generator.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A Generator yielding dictionaries mapping column name to an Arrow Array of the added data. The
          Arrow Arrays will have length less than or equal to chunk_size.
        """

        return _make_generator(self.update.after_adds, self.update.added_rows, cols, chunk_size)

    def modified_prev(self, cols: Union[str, List[str]] = None) -> ColDictType:
        """Gets all the data as it existed *before* the modify operation in this TableUpdate.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A dictionary mapping column name to an Arrow Array of the data as it existed before the modify
            operation.
        """

        return _first_or_default(self.modified_prev_chunks(self.update.all_modified_rows.size, cols))

    def modified_prev_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> DictGeneratorType:
        """Creates a generator yielding chunks of the data as it existed *before* the modify operation in this TableUpdate.
        :param chunk_size: The maximum number of rows yielded by each iteration of the generator.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A Generator yielding dictionaries mapping column name to an Arrow Array of the data as it existed
          before the modify operation. The Arrow Arrays will have length less than or equal to chunk_size.
        """

        return _make_generator(self.update.before_modifies, self.update.all_modified_rows, cols, chunk_size)

    def modified(self, cols: Union[str, List[str]] = None) -> ColDictType:
        """Gets all the modified data *after* the modify operation in this TableUpdate.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A dictionary mapping column name to an Arrow Array of the data after the modify operation.
        """

        return _first_or_default(self.modified_chunks(self.update.all_modified_rows.size, cols))

    def modified_chunks(self, chunk_size: int, cols: Union[str, List[str]] = None) -> DictGeneratorType:
        """Creates a generator yielding chunks of the modified data *after* the modify operation in this TableUpdate.
        :param chunk_size: The maximum number of rows yielded by each iteration of the generator.
        :param cols: A column name or list of column names. None means "all columns in the table".
        :return: A Generator yielding dictionaries mapping column name to an Arrow Array of the data after
          the modify operation. The Arrow Arrays will have length less than or equal to chunk_size.
        """

        return _make_generator(self.update.after_modifies, self.update.all_modified_rows, cols, chunk_size)


class TableListener(ABC):
    """The abstract base class definition for table listeners. Provides a default on_error implementation that
    just prints the exception."""

    @abstractmethod
    def on_update(self, update: TableUpdate) -> None:
        """This method is invoked when there is an incoming TableUpdate message.
        :param update: The table update.
        """

        pass

    def on_error(self, error: Exception) -> None:
        """This method is invoked when there is an error during tabe listener processing.
        :param error: The Exception that was encountered during processing.
        """

        print(f"Error happened during ticking processing: {error}")


class TableListenerHandle:
    """An object for managing the ticking callback state.

    Usage:
    handle = TableListenerHandle(table, MyListener())
    handle.start()  # subscribe to updates on the table
    # When updates arrive, they will invoke callbacks on your TableListener object in a separate thread
    handle.stop()  # unsubscribe and shut down the thread.
    """

    _table: Table
    _listener: TableListener
    # Set when the user calls stop()
    _cancelled: bool
    # Tracks Whether we've called cancel on the FlightStreamReader
    _reader_cancelled: bool
    _bp: dhc.BarrageProcessor
    _writer: flight.FlightStreamWriter
    _reader: flight.FlightStreamReader
    _thread: threading.Thread

    def __init__(self, table: Table, listener: TableListener):
        """Constructor.
        :param table: The Table that is being listened to.
        :listener: The TableListener callback that will receive TableUpdate messages as the table changes.
        """

        self._table = table
        self._listener = listener
        self._cancelled = False
        self._reader_cancelled = False

    def start(self) -> None:
        """Subscribes to changes on the table referenced in the constructor. When changes happen, the
        TableListener passed into the constructor will be invoked."""

        fls = self._table.session.flight_service
        self._writer, self._reader = fls.do_exchange()
        self._bp = dhc.BarrageProcessor.create(self._table.schema)
        subreq = dhc.BarrageProcessor.create_subscription_request(self._table.ticket.ticket)
        self._writer.write_metadata(subreq)

        self._thread = threading.Thread(target=self._process_data)
        self._thread.start()

    def stop(self) -> None:
        """Cancels the subscription to the table and stops the service thread. By the time this method returns, the
        thread servicing the subscription will be destroyed, and the callback will no longer be invoked.
        This method joins the subscription servicing thread, unless stop() was called from that very thread.
        This can happen if the user's callback calls stop()."""

        self._cancelled = True
        if threading.get_ident() == self._thread.ident:
            # We are inside the callback, so just setting the 'cancelled' flag suffices.
            return

        self._reader_cancelled = True
        self._reader.cancel()
        self._thread.join()

    def _process_data(self):
        """This method continuously runs on a separate thread. It processes incoming Barrage messages, feeds them to
        the BarrageProcessor library, and, when the BarrageProcessor library produces a TableUpdate, calls the
        user-supplied callback with that TableUpdate."""

        try:
            while not self._cancelled:
                data, metadata = self._reader.read_chunk()
                ticking_update = self._bp.process_next_chunk(data.columns, metadata)
                if ticking_update is not None:
                    table_update = TableUpdate(ticking_update)
                    self._listener.on_update(table_update)
        except StopIteration:
            pass
        except Exception as e:
            if not self._cancelled:
                self._listener.on_error(e)

        try:
            if not self._reader_cancelled:
                self._reader_cancelled = True
                self._reader.cancel()
            self._writer.close()
        except Exception as e:
            pass

def listen(table: Table, listener: Union[Callable, TableListener],
           on_error: Union[Callable, None] = None) -> TableListenerHandle:
    """A convenience method to create a TableListenerHandle. This method can be called in one of three ways:

    listen(MyTableListener())  # invoke with your own subclass of TableListener
    listen(on_update_callback)  # invoke with your own on_update Callback
    listen(on_update_callback, on_error_callback)  # invoke with your own on_update and on_error callbacks
    """
    
    listener_to_use: TableListener

    if callable(listener):
        n_params = len(signature(listener).parameters)
        if n_params != 1:
            raise ValueError("Callabale listener function must have 1 (update) parameter.")

        if on_error is None:
            listener_to_use = _CallableAsListener(listener)
        else:
            n_params = len(signature(on_error).parameters)
            if n_params != 1:
                raise ValueError("on_error function must have 1 (exception) parameter.")
            listener_to_use = _CallableAsListenerWithErrorCallback(listener, on_error)
    elif isinstance(listener, TableListener):
        if on_error is not None:
            raise ValueError("When passing a TableListener object, second argument must be None")
        listener_to_use = listener
    else:
        raise ValueError("listener is neither callable nor TableListener object")
    return TableListenerHandle(table, listener_to_use)


class _CallableAsListener(TableListener):
    """A TableListener implementation that delegates on_update to a supplied Callback. This class does not
    override on_error."""
    
    _on_update_callback: Callable

    def __init__(self, on_update_callback: Callable):
        self._on_update_callback = on_update_callback

    def on_update(self, update: TableUpdate) -> None:
        self._on_update_callback(update)

class _CallableAsListenerWithErrorCallback(_CallableAsListener):
    """A TableListener implementation that delegates both on_update and on_error to supplied Callbacks."""

    _on_error_callback: Callable

    def __init__(self, on_update_callback: Callable, on_error_callback: Callable):
        super().__init__(on_update_callback)
        self._on_error_callback = on_error_callback

    def on_error(self, error: Exception) -> None:
        self._on_error_callback(error)
