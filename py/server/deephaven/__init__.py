#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

"""Deephaven Python Integration Package provides the ability to access the Deephaven's query engine natively and thus
unlocks the unique power of Deephaven to the Python community.

"""

__version__ = "0.16.0"

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
from .table_factory import empty_table, time_table, merge, merge_sorted, new_table, DynamicTableWriter
from .replay import TableReplayer
