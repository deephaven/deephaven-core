#
#  Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#
"""Deephaven Python Integration Package provides the ability to access the Deephaven's query engine natively and thus
unlocks the unique and tremendous power of Deephaven to the Python community.

"""
import jpy

__version__ = "0.9.0"

if not jpy.has_jvm():
    from ._utils.bootstrap import build_py_session

    build_py_session()

from .dherror import DHError
from .constants import SortDirection
from .csv import read as read_csv
from .csv import write as write_csv
from .table_factory import empty_table, time_table, merge, merge_sorted, new_table
