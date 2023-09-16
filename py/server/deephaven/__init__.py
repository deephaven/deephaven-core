#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""Deephaven Python Integration Package provides the ability to access the Deephaven's query engine natively and thus
unlocks the unique power of Deephaven to the Python community.

"""

__version__ = "0.28.1"

from deephaven_internal import jvm

try:
    jvm.check_ready()
finally:
    del jvm

from .dherror import DHError
from .table import SortDirection
from .csv import read as read_csv
from .csv import write as write_csv
from .stream.kafka import consumer as kafka_consumer
from .stream.kafka import producer as kafka_producer
from .table_factory import empty_table, time_table, merge, merge_sorted, new_table, DynamicTableWriter, input_table, \
    ring_table, function_generated_table
from .replay import TableReplayer
from ._gc import garbage_collect
from .dbc import read_sql

__all__ = ["read_csv", "write_csv", "kafka_consumer", "kafka_producer", "empty_table", "time_table", "merge",
           "merge_sorted", "new_table", "input_table", "ring_table", "function_generated_table", "DynamicTableWriter",
           "TableReplayer", "garbage_collect", "read_sql", "DHError", "SortDirection"]
