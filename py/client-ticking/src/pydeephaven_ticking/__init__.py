#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

"""Deephaven Python Ticking Client (`pydeephaven-ticking`) is a Python API that supports ticking tables (i.e.
tables whose contents change over time). It is designed to work in conjunction with the Deephaven Python Client
(`pydeephaven`). The main entry point is the method `listen`: the caller provides a TableHandle and a listener callback,
and the API invokes the callback (asynchronously, on a separate Python thread) periodically when there are changes to
the table. The table changes are represented in a `TableUpdate` class, which contains the data that was added, removed,
or modified.

Example:
    >>> import pydeephaven as dh
    >>> session = dh.Session() # assuming Deephaven Community Edition is running locally with the default configuration
    >>> table = session.time_table(period=1000000000).update(formulas=["Col1 = i"])
    >>> listener_handle = dh.table_listener.listen(table, lambda update : print(update.added()))
    >>> listener_handle.start()
    >>> # data starts printing asynchronously here
    >>> listener_handle.stop()
    >>> session.close()
"""
