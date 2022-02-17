#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
"""Deephaven Python Integration Package provides the ability to access the Deephaven's query engine natively and thus
unlocks the unique power of Deephaven to the Python community.

"""

__version__ = "0.9.0"

from ._init.bootstrap import build_py_session
from .dherror import DHError

try:
    build_py_session()
except Exception as e:
    raise DHError(e, "deephaven initialization failed.") from e
else:
    from .constants import SortDirection
    from .csv import read as read_csv
    from .csv import write as write_csv
    from .stream.kafka import consumer as kafka_consumer
    from .stream.kafka import producer as kafka_producer
    from .table_factory import empty_table, time_table, merge, merge_sorted, new_table, DynamicTableWriter
    from .replay import TableReplayer
