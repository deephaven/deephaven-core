#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""Deephaven Python Integration Package provides the ability to access the Deephaven's query engine natively and thus
unlocks the unique power of Deephaven to the Python community.

"""

__version__ = "0.21.0"

from deephaven_internal import jvm
try:
    jvm.check_ready()
finally:
    del jvm

from .dherror import DHError
from .table import SortDirection, AsOfMatchRule
from .csv import read as read_csv
from .csv import write as write_csv
from .stream.kafka import consumer as kafka_consumer
from .stream.kafka import producer as kafka_producer
from .table_factory import empty_table, time_table, merge, merge_sorted, new_table, DynamicTableWriter, input_table
from .replay import TableReplayer
from ._gc import garbage_collect

import threading

def create_thread_entry(thread_name):
    """
    Helper to call from the JVM into python to set up py thread state exactly once per jvm thread, and support debugging
    """
    # First, ensure that this Java thread has a python _DummyThread instance registered, which will have the same
    # lifetime as the pythreadstate (and so, the tracing). This ensures that if debugging is enabled after this thread
    # was created, it will correctly be able to trace this thread.
    thread = threading.current_thread()

    # Assign the java thread name to the python thread
    thread.name = 'java-' + thread_name
    # Then, if pydevd has already been initialized, we should attempt to make ourselves known to it.
    try:
        # TODO can we conditionally import the other ones, try each module until we find a working one?
        import pydevd

        # We don't want to be the first one to call settrace(), so check to see if setup completed on another thread before attemption it here
        if pydevd.SetupHolder.setup is not None:
            pydevd.settrace(suspend=False)
    except ImportError:
        # Debugger hasn't started yet (or we don't know which one is in use), so registering our thread
        # above should be sufficient
        pass

    # Return a def to Java with a particular name that will call back into the Java stack
    def JavaThread(runnable):
        runnable.run()
    return JavaThread
