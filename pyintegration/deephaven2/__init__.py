#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
"""Deephaven Python Integration Package provides the ability to access the Deephaven's query engine natively and thus
unlocks the unique and tremendous power of Deephaven to the Python community.

"""
import jpy

__version__ = "0.9.0"

from .dherror import DHError
from ._init.bootstrap import build_py_session

try:
    build_py_session()
except Exception as e:
    raise DHError(e, "deephaven initialization failed.") from e
else:
    from deephaven2.constants import SortDirection
    from deephaven2.csv import read as read_csv
    from deephaven2.csv import write as write_csv
    from deephaven2.table_factory import empty_table, time_table, merge, merge_sorted, new_table
    from deephaven2.utils import get_workspace_root
    import deephaven2.stream.kafka.consumer as kafka_consumer
    import deephaven2.stream.kafka.producer as kafka_producer

